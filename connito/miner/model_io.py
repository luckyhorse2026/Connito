import os
import random
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from queue import Queue
from threading import Lock, Thread

from dotenv import load_dotenv

load_dotenv()


import bittensor

from connito.shared.app_logging import configure_logging, structlog
from connito.shared.chain import (
    CHAIN_COMMIT_MAX_HF_REPO_ID_CHARS,
    MinerChainCommit,
    SignedModelHashChainCommit,
    commit_status,
)
from connito.shared.checkpoints import (
    ModelCheckpoint,
    select_best_checkpoint,
)
from connito.shared.expert_manager import ExpertManager
from connito.shared.config import MinerConfig, parse_args
from connito.shared.chain import setup_chain_worker
from connito.shared.cycle import (
    PhaseResponse,
    check_phase_expired,
    get_allowed_version_range,
    wait_till,
)
from connito.shared.hf_distribute import (
    get_hf_upload_readiness,
    resolve_hf_repo_ids,
    upload_checkpoint_to_hf,
)
from connito.shared.model import fetch_model_from_chain_validator
from connito.shared.telemetry import inc_error
from connito.sn_owner.cycle import PhaseNames

# Short SHA prefix written to the chain. Matches the validator convention so
# HF short-SHA resolution behaves the same on both sides.
HF_CHAIN_REVISION_LENGTH = 7

configure_logging()
logger = structlog.get_logger(__name__)


def _classify_upload_error(exc: BaseException) -> str:
    """Map an upload exception to a small set of `inc_error` kind labels.

    Keeps cardinality bounded; refine the buckets here as we learn what
    actually surfaces in production logs. Returns one of:
      - "timeout" — TimeoutError or any exception whose name/message mentions a timeout
      - "rpc" — HF / requests / urllib HTTP transport failures
      - "unknown" — anything else (config, filesystem, programmer error)
    """
    if isinstance(exc, TimeoutError):
        return "timeout"
    name = type(exc).__name__.lower()
    msg = str(exc).lower()
    if "timeout" in name or "timed out" in msg or "timeout" in msg:
        return "timeout"
    if any(k in name for k in ("http", "connection", "request", "hub", "hf")):
        return "rpc"
    if any(k in msg for k in ("connection", "network", "503", "502", "504", "reset", "unreachable", "dns")):
        return "rpc"
    return "unknown"


# --- Job definitions ---


class JobType(Enum):
    DOWNLOAD = auto()
    COMMIT = auto()


@dataclass
class Job:
    job_type: JobType
    payload: dict | None = None
    phase_response: PhaseResponse | None = None


@dataclass
class SharedState:
    current_model_version: int | None = None
    current_model_hash: str | None = None
    latest_checkpoint_path: str | None = None
    lock: Lock = field(default_factory=Lock, repr=False)


class FileNotReadyError(RuntimeError):
    pass


def _skip_download() -> bool:
    """Rotation-commit miners commit pre-downloaded models from ../models and
    never use the validator model fetched during Distribute. Set
    CONNITO_SKIP_DOWNLOAD=1 to skip the download phase entirely — this drops the
    heavy per-cycle get_chain_commits archive query (the main chain-RPC load and
    crash source) and only leaves the lightweight commit writes."""
    return os.environ.get("CONNITO_SKIP_DOWNLOAD", "") == "1"


# --- Scheduler service ---
def scheduler_service(
    config,
    download_queue: Queue,
    commit_queue: Queue,
    poll_fallback_block: int = 3,
    skip_download: bool = False,
):
    """
    Periodically checks whether to start download/commit phases and enqueues jobs.
    """
    while True:
        # --------- DOWNLOAD SCHEDULING ---------
        # Always wait for Distribute first: it is the once-per-cycle gate that
        # keeps the loop from re-triggering for the whole (multi-block)
        # MinerCommit1 phase. In skip_download mode we still wait here (a cheap
        # phase-timing poll) but don't enqueue a download job — only the heavy
        # download WORK is skipped, not the cycle gate.
        phase_response = wait_till(config, phase_name=PhaseNames.distribute, poll_fallback_block=poll_fallback_block)
        if not skip_download:
            download_queue.put(Job(job_type=JobType.DOWNLOAD, phase_response=phase_response))

        # --------- COMISSION SCHEDULING ---------
        phase_response = wait_till(
            config, phase_name=PhaseNames.miner_commit_1, poll_fallback_block=poll_fallback_block
        )
        commit_queue.put(
            Job(
                job_type=JobType.COMMIT,
                phase_response=phase_response,
            )
        )


# --- Workers ---
def download_worker(
    config,
    wallet,
    expert_manager,
    download_queue: Queue,
    current_model_meta,
    current_model_hash,
    shared_state: SharedState,
    subtensor=None,
):
    """
    Consumes DOWNLOAD jobs and runs the download phase logic.
    """
    if subtensor is None:
        subtensor = bittensor.Subtensor(config.chain.network)
    while True:
        job = download_queue.get()
        if job is None:  # poison pill — clean shutdown
            download_queue.task_done()
            logger.info(f"<{PhaseNames.distribute}> shutdown signal received.")
            return
        try:
            # Read current version/hash snapshot
            current_model_meta = select_best_checkpoint(
                primary_dir=config.ckpt.validator_checkpoint_path,
                secondary_dir=config.ckpt.checkpoint_path,
                resume=config.ckpt.resume_from_ckpt,
            )

            if current_model_meta is not None:
                current_model_meta.model_hash = current_model_hash

            chain_checkpoint = fetch_model_from_chain_validator(
                current_model_meta,
                config,
                subtensor,
                wallet,
                expert_group_ids=[config.task.exp.group_id],
                expert_group_assignment = expert_manager.expert_group_assignment
            )

            if (
                chain_checkpoint is None
                or chain_checkpoint.global_ver is None
                or chain_checkpoint.model_hash is None
            ):
                raise FileNotReadyError(f"No required download job: {chain_checkpoint}")

            logger.info(f"<{PhaseNames.distribute}> downloaded model metadata from chain: {chain_checkpoint}.")

            # Update shared state with new version/hash
            current_model_meta = select_best_checkpoint(
                primary_dir=config.ckpt.validator_checkpoint_path,
                secondary_dir=config.ckpt.checkpoint_path,
                resume=config.ckpt.resume_from_ckpt,
            )

            with shared_state.lock:
                shared_state.current_model_version = current_model_meta.global_ver
                shared_state.current_model_hash = current_model_meta.model_hash

        except FileNotReadyError as e:
            logger.info(f"<{PhaseNames.distribute}>: {e}")

        except Exception as e:
            logger.error(f"<{PhaseNames.distribute}> Error while handling job", error=str(e), exc_info=True)

        finally:
            if job.phase_response is not None:
                check_phase_expired(subtensor, job.phase_response)
            download_queue.task_done()
            logger.info(f"<{PhaseNames.distribute}> task completed.")


# Directory holding the pre-downloaded HuggingFace model repos to rotate
# through, one committed per cycle. Resolved as `../models` relative to the
# project root (root_path is the repo dir; its parent holds `models/`).
ROTATION_MODELS_DIRNAME = "models"


def _rotation_models_dir(config) -> Path:
    return Path(config.run.root_path).parent / ROTATION_MODELS_DIRNAME


def _list_rotation_models(models_dir: Path) -> list[Path]:
    """Return the model repo subdirectories under `models_dir`, sorted by name
    so the first cycle deterministically picks `model0` (the first entry).
    """
    if not models_dir.exists():
        return []
    return sorted(p for p in models_dir.iterdir() if p.is_dir())


def _select_rotation_model(models_dir: Path, previous: Path | None) -> Path:
    """Pick the model to commit this cycle: a random model, excluding the one
    committed in the immediately previous cycle.

    First cycle (`previous is None`) is also random — nothing is excluded, so
    every model is a candidate. If only one model exists it is reused regardless.
    """
    models = _list_rotation_models(models_dir)
    if not models:
        raise FileNotReadyError(f"No model directories under {models_dir}, skip commit.")
    # previous is None on the first cycle -> `m != previous` keeps every model,
    # so the first pick is random too. The `or models` fallback covers the
    # single-model case where the only candidate is the previous one.
    candidates = [m for m in models if m != previous] or models
    return random.choice(candidates)


def _prepare_checkpoint_for_commit(
    config,
    wallet,
    shared_state: SharedState,
    previous_model: Path | None,
) -> tuple[ModelCheckpoint, Path]:
    """Select the next model from the rotation directory, sign its hash, and
    publish the path to shared state.

    Returns the signed checkpoint and the selected model directory (so the
    caller can exclude it from next cycle's random pick).
    """
    models_dir = _rotation_models_dir(config)
    selected = _select_rotation_model(models_dir, previous_model)

    # Validators reject commits whose global_ver falls outside
    # [phase_start - version_range_cycles * cycle_length, phase_start]. The
    # rotated models carry no training version, so stamp them with the upper
    # bound of that window (the current MinerCommit1 phase start block).
    _min_ver, max_ver = get_allowed_version_range(config)
    if max_ver is None:
        raise FileNotReadyError("Could not resolve allowed version range, skip commit.")

    latest_checkpoint = ModelCheckpoint(
        path=selected,
        expert_group=config.task.exp.group_id,
        global_ver=max_ver,
        role="miner",
        place="local",
    )
    latest_checkpoint.sign_hash(wallet=wallet)

    with shared_state.lock:
        shared_state.latest_checkpoint_path = latest_checkpoint.path

    return latest_checkpoint, selected


def _commit_signed_model_hash(
    config,
    wallet,
    subtensor,
    latest_checkpoint: ModelCheckpoint,
) -> None:
    logger.info(
        f"<{PhaseNames.miner_commit_1}> committing",
        model_version=latest_checkpoint.global_ver,
        hash=latest_checkpoint.model_hash,
        path=latest_checkpoint.path,
    )
    commit_status(
        config,
        wallet,
        subtensor,
        SignedModelHashChainCommit(
            signed_model_hash=latest_checkpoint.signed_model_hash,
        ),
    )


def _upload_checkpoint_to_hf_safe(
    config,
    latest_checkpoint: ModelCheckpoint,
) -> tuple[str | None, str | None]:
    """Resolve the miner's HF repo and upload the checkpoint directory.

    Returns ``(chain_repo_id, revision)`` — both ``None`` if the HF transport
    isn't configured or the upload fails. HF is the only submission path:
    a failure here means the miner will be missing for this round.
    """
    try:
        hf_upload_repo_id, hf_chain_repo_id = resolve_hf_repo_ids(
            config.hf,
            max_chain_repo_chars=CHAIN_COMMIT_MAX_HF_REPO_ID_CHARS,
        )
    except Exception as e:
        inc_error("checkpoint_upload", _classify_upload_error(e))
        logger.error(
            f"<{PhaseNames.miner_commit_1}> HF repo id resolution failed; miner will be missing for this round",
            error=str(e),
            exc_info=True,
        )
        return None, None

    hf_ready, hf_reason = get_hf_upload_readiness(
        repo_id=hf_upload_repo_id,
        token_env_var=config.hf.token_env_var,
    )
    if not (hf_ready and latest_checkpoint.path is not None):
        logger.error(
            f"<{PhaseNames.miner_commit_1}> HF upload unavailable; miner will be missing for this round",
            reason=hf_reason,
            upload_checkpoint_repo=hf_upload_repo_id,
            has_ckpt_path=latest_checkpoint.path is not None,
        )
        return None, None

    try:
        hf_revision = upload_checkpoint_to_hf(
            ckpt_dir=latest_checkpoint.path,
            repo_id=hf_upload_repo_id,
            token_env_var=config.hf.token_env_var,
            commit_message=(
                f"miner submission global_ver={latest_checkpoint.global_ver} "
                f"expert_group={config.task.exp.group_id}"
            ),
            # Validators fetch this validator's expert-group shard. New
            # miners ship `.safetensors` (no pickle, no code-execution
            # surface); the validator's download worker (PR #98) tries
            # `.safetensors` first and falls back to `.pt`, so we include
            # both extensions during the migration window so a miner
            # upgrading mid-cycle whose latest checkpoint is still `.pt`
            # doesn't end up with an empty upload. `model_shared.*` is
            # intentionally excluded — validators only fetch the expert-
            # group shard, and skipping it keeps uploads small.
            allow_patterns=[
                f"model_expgroup_{config.task.exp.group_id}.safetensors",
                f"model_expgroup_{config.task.exp.group_id}.pt",
            ],
        )
    except Exception as e:
        inc_error("checkpoint_upload", _classify_upload_error(e))
        logger.error(
            f"<{PhaseNames.miner_commit_1}> HF upload failed; miner will be missing for this round",
            upload_checkpoint_repo=hf_upload_repo_id,
            error=str(e),
            exc_info=True,
        )
        return None, None

    return hf_chain_repo_id, hf_revision


def _commit_model_hash(
    config,
    wallet,
    subtensor,
    latest_checkpoint: ModelCheckpoint,
    hf_chain_repo_id: str | None,
    hf_revision: str | None,
) -> None:
    """Emit the miner_commit_2 payload. Omits block and inner_opt so the
    serialized JSON stays within the 128-byte chain budget shared with the
    validator commit.
    """
    short_revision = hf_revision[:HF_CHAIN_REVISION_LENGTH] if hf_revision else None
    logger.info(
        f"<{PhaseNames.miner_commit_2}> committing",
        model_version=latest_checkpoint.global_ver,
        hash=latest_checkpoint.model_hash,
        path=latest_checkpoint.path,
        hf_repo_id=hf_chain_repo_id if hf_revision else None,
        hf_revision=short_revision,
    )
    commit_status(
        config,
        wallet,
        subtensor,
        MinerChainCommit(
            expert_group=config.task.exp.group_id,
            model_hash=latest_checkpoint.model_hash,
            global_ver=latest_checkpoint.global_ver,
            hf_repo_id=hf_chain_repo_id if hf_revision else None,
            hf_revision=short_revision,
        ),
    )


def commit_worker(
    config,
    commit_queue: Queue,
    wallet,
    shared_state: SharedState,
    subtensor=None,
):
    """Consume COMMIT jobs. For each cycle: select the next model from the
    rotation directory and sign+publish its hash (miner_commit_1), upload to
    HF, then commit the hash+HF coords (miner_commit_2). Each step lives in its
    own helper for readability.
    """
    if subtensor is None:
        subtensor = bittensor.Subtensor(config.chain.network)
    # Model committed in the previous cycle, excluded from this cycle's random
    # pick. Advances on every successful selection so the rotation keeps moving.
    previous_model: Path | None = None
    while True:
        job = commit_queue.get()
        if job is None:  # poison pill — clean shutdown
            commit_queue.task_done()
            logger.info(f"<{PhaseNames.miner_commit_1}> shutdown signal received.")
            return
        try:
            latest_checkpoint, previous_model = _prepare_checkpoint_for_commit(
                config, wallet, shared_state, previous_model
            )
            _commit_signed_model_hash(config, wallet, subtensor, latest_checkpoint)
            check_phase_expired(subtensor, job.phase_response)

            # HF upload runs between the two commits so the revision is known
            # by the time we write miner_commit_2. Failure returns (None, None)
            # and the chain commit goes out without r/rv — the miner is then
            # missing for this round and gets the zero-score penalty.
            hf_chain_repo_id, hf_revision = _upload_checkpoint_to_hf_safe(config, latest_checkpoint)

            phase_response = wait_till(config, PhaseNames.miner_commit_2)
            _commit_model_hash(
                config, wallet, subtensor, latest_checkpoint,
                hf_chain_repo_id, hf_revision,
            )
            check_phase_expired(subtensor, phase_response)

        except FileNotReadyError as e:
            logger.warning(f"<{PhaseNames.miner_commit_1}> File not ready error: {e}")

        except Exception as e:
            logger.error(f"<{PhaseNames.miner_commit_1}> Error while handling job", error=str(e), exc_info=True)

        finally:
            commit_queue.task_done()


# --- Wiring it all together ---
def run_system(config, wallet, expert_manager, current_model_version: int = 0, current_model_hash: str = "xxx", subtensor=None):
    if subtensor is None:
        subtensor = bittensor.Subtensor(config.chain.network)

    download_queue = Queue()
    commit_queue = Queue()
    shared_state = SharedState(current_model_version, current_model_hash)

    skip_download = _skip_download()
    if skip_download:
        logger.info("CONNITO_SKIP_DOWNLOAD=1: download worker disabled (rotation-commit mode)")

    # Non-daemon threads so they can be joined cleanly on shutdown.
    download_thread = None
    if not skip_download:
        download_thread = Thread(
            target=download_worker,
            args=(config, wallet, expert_manager, download_queue, current_model_version, current_model_hash, shared_state, subtensor),
            daemon=False,
        )
    commit_thread = Thread(
        target=commit_worker,
        args=(config, commit_queue, wallet, shared_state, subtensor),
        daemon=False,
    )

    if download_thread is not None:
        download_thread.start()
    commit_thread.start()

    try:
        # Scheduler runs in the foreground; blocks until interrupted or it errors.
        scheduler_service(
            config=config,
            download_queue=download_queue,
            commit_queue=commit_queue,
            skip_download=skip_download,
        )
    finally:
        # Send poison pills so each worker loop exits cleanly.
        if download_thread is not None:
            download_queue.put(None)
        commit_queue.put(None)

        _JOIN_TIMEOUT_S = 30
        if download_thread is not None:
            download_thread.join(timeout=_JOIN_TIMEOUT_S)
        commit_thread.join(timeout=_JOIN_TIMEOUT_S)

        logger.info("run_system: all worker threads have exited.")


if __name__ == "__main__":
    args = parse_args()

    if args.debug:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose debug logging enabled!")

    if args.path:
        config = MinerConfig.from_path(args.path, auto_update_config=args.auto_update_config)
    else:
        config = MinerConfig()

    config.write()

    # Redirect cycle-API calls to a reachable host (e.g. the local phase
    # server) when CONNITO_OWNER_URL is set. Applied after write() so the YAML
    # keeps the real (locked) owner_url. No-op when the env var is unset.
    from connito.shared.cycle import apply_owner_url_override
    apply_owner_url_override(config)

    wallet, subtensor, _lite_subtensor = setup_chain_worker(config)

    expert_manager = ExpertManager(config)

    run_system(config, wallet, expert_manager, subtensor=subtensor)
