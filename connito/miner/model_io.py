import fcntl
import json
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
    get_blocks_until_next_phase_from_api,
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

        # --------- COMMIT SCHEDULING ---------
        # Enqueue the COMMIT job now, at the start of the cycle (Distribute).
        # The commit worker prepares the checkpoint (rotation-model select +
        # multi-GB hash) during the long Train phase that follows, then waits
        # for the MinerCommit1 window itself and submits immediately — so the
        # slow hash no longer eats the ~2-block commit window. The worker
        # blocks on Train/MinerCommit1, which paces this loop to one job/cycle.
        commit_queue.put(Job(job_type=JobType.COMMIT))

        # Pace the loop: block until this cycle's MinerCommit1 window so we
        # enqueue exactly one COMMIT per cycle. Without this, the next
        # wait_till(Distribute) would return immediately (still in Distribute)
        # and spin out a flood of jobs. The result is intentionally unused —
        # the worker does its own wait_till(MinerCommit1).
        wait_till(
            config, phase_name=PhaseNames.miner_commit_1, poll_fallback_block=poll_fallback_block
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
# Shared, host-local file where concurrent miners reserve a distinct model for
# the current cycle (see _claim_rotation_model). Lives in the same cache dir as
# the cycle cache; gitignored.
ROTATION_CLAIMS_FILENAME = "model_claims.json"


def _rotation_models_dir(config) -> Path:
    # .resolve() BEFORE .parent: the config writer relativizes paths on save, so
    # run.root_path is typically stored as '.' — and Path('.').parent is '.',
    # which collapses the intended '../models' to './models' (a nonexistent dir
    # inside the repo) and makes every commit skip with "No group-N shard".
    # Resolving against the CWD (the repo dir, per ecosystem `cwd: REPO`) yields
    # the absolute repo path so .parent correctly climbs to its parent, where
    # the rotation pool lives. Works whether root_path is '.' or already absolute.
    return Path(config.run.root_path).resolve().parent / ROTATION_MODELS_DIRNAME


def _rotation_claims_path(config) -> Path:
    return Path(config.run.root_path) / "cache" / ROTATION_CLAIMS_FILENAME


def _list_rotation_models(models_dir: Path, group_id: int) -> list[Path]:
    """Return the rotation model directories under `models_dir`, sorted by name.

    Only directories that actually contain this expert group's shard
    (`model_expgroup_{group_id}.safetensors` or `.pt`) qualify. This skips
    non-model entries — hidden dirs like `.claude`/`.cache`, partial downloads,
    or scratch folders — which would otherwise be selectable and produce a
    broken commit (the upload matches no files, so the on-chain model_hash
    wouldn't match what's on HF and the validator rejects it).
    """
    if not models_dir.exists():
        return []
    shards = (f"model_expgroup_{group_id}.safetensors", f"model_expgroup_{group_id}.pt")
    return sorted(
        p for p in models_dir.iterdir()
        if p.is_dir() and any((p / s).exists() for s in shards)
    )


def _claim_rotation_model(
    models: list[Path],
    previous: Path | None,
    *,
    cycle_key: int,
    hotkey: str,
    claims_path: Path,
) -> Path:
    """Atomically reserve a distinct model for this cycle so concurrent miners on
    the same host don't all commit the same model (validators may penalise
    duplicate model hashes across hotkeys).

    Coordination is a single JSON file guarded by an exclusive `flock`, keyed by
    `cycle_key` (resets when the cycle rolls over). Each miner excludes models
    already claimed by *other* hotkeys this cycle, then picks randomly from
    what's left and records its own claim — all under the lock so the read,
    decision and write are one atomic step across processes.

    Guarantees a distinct pick per hotkey while the pool has >= as many models as
    active miners. When the pool is smaller, a collision is unavoidable: it logs
    a warning and falls back to a random pick.
    """
    claims_path.parent.mkdir(parents=True, exist_ok=True)
    # "a+" creates the file if missing and never truncates on open; we seek(0)
    # to read and truncate explicitly before writing, all while holding LOCK_EX.
    with open(claims_path, "a+", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.seek(0)
            raw = f.read()
            try:
                data = json.loads(raw) if raw.strip() else {}
            except ValueError:
                data = {}
            if data.get("cycle_key") != cycle_key:
                data = {"cycle_key": cycle_key, "claims": {}}
            claims = data.setdefault("claims", {})

            taken = {m for hk, m in claims.items() if hk != hotkey}
            # Prefer a model that is neither last cycle's pick nor already claimed
            # by another miner this cycle.
            pool = [p for p in models if p != previous and p.name not in taken]
            if not pool:  # everything non-previous is taken -> drop the previous-exclusion
                pool = [p for p in models if p.name not in taken]
            if not pool:  # more active miners than models -> collision unavoidable
                logger.warning(
                    "rotation pool smaller than active miners; duplicate commit unavoidable",
                    models=len(models),
                    claimed=len(taken),
                    hotkey=hotkey,
                )
                pool = models
            selected = random.choice(pool)

            claims[hotkey] = selected.name
            f.seek(0)
            f.truncate()
            json.dump(data, f)
            f.flush()
            return selected
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def _select_rotation_model(
    models_dir: Path,
    previous: Path | None,
    group_id: int,
    *,
    cycle_key: int,
    hotkey: str,
    claims_path: Path,
) -> Path:
    """Pick the model to commit this cycle: a random model, excluding the one
    committed in the immediately previous cycle and any claimed by a sibling
    miner this cycle (see _claim_rotation_model).

    First cycle (`previous is None`) is also random — nothing is excluded, so
    every model is a candidate. If only one model exists it is reused regardless.
    """
    models = _list_rotation_models(models_dir, group_id)
    if not models:
        raise FileNotReadyError(
            f"No model directories with a group-{group_id} shard under {models_dir}, skip commit."
        )
    return _claim_rotation_model(
        models, previous, cycle_key=cycle_key, hotkey=hotkey, claims_path=claims_path
    )


def _upcoming_commit1_version(config) -> int | None:
    """Start block of the NEXT MinerCommit1 window, from the phase API.

    Used when preparing a checkpoint ahead of the window (during Train): the
    schedule endpoint reports the upcoming MinerCommit1 start, which is exactly
    the `phase_start_block` that `get_allowed_version_range` returns once the
    window is actually open. Returns None if the API is unavailable, so the
    caller can fall back to the in-window resolution.
    """
    schedule = get_blocks_until_next_phase_from_api(config)
    if not schedule:
        return None
    entry = schedule.get(PhaseNames.miner_commit_1)
    if not entry:
        return None
    return entry[0]


def _prepare_checkpoint_for_commit(
    config,
    wallet,
    shared_state: SharedState,
    previous_model: Path | None,
    commit1_version: int | None = None,
) -> tuple[ModelCheckpoint, Path]:
    """Select the next model from the rotation directory, sign its hash, and
    publish the path to shared state.

    Returns the signed checkpoint and the selected model directory (so the
    caller can exclude it from next cycle's random pick).

    `commit1_version`, when given, is used as the version/dedup key instead of
    resolving it from `get_allowed_version_range`. The commit worker prepares
    the checkpoint DURING Train — before the MinerCommit1 window opens — so the
    multi-GB hash is done ahead of time; at that point `get_allowed_version_range`
    would return the *previous* cycle's MinerCommit1 start, so the caller passes
    the *upcoming* MinerCommit1 start explicitly (same value validators use once
    the window opens).
    """
    models_dir = _rotation_models_dir(config)

    # Validators reject commits whose global_ver falls outside
    # [phase_start - version_range_cycles * cycle_length, phase_start]. The
    # rotated models carry no training version, so stamp them with the upper
    # bound of that window (the current MinerCommit1 phase start block). That same
    # phase-start block is constant for the whole cycle and identical across all
    # miners, so it doubles as the per-cycle key for cross-miner claim dedup.
    if commit1_version is not None:
        max_ver = commit1_version
    else:
        _min_ver, max_ver = get_allowed_version_range(config)
    if max_ver is None:
        raise FileNotReadyError("Could not resolve allowed version range, skip commit.")

    selected = _select_rotation_model(
        models_dir,
        previous_model,
        config.task.exp.group_id,
        cycle_key=max_ver,
        hotkey=config.chain.hotkey_ss58,
        claims_path=_rotation_claims_path(config),
    )

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
    commit_subtensor=None,
):
    """Consume COMMIT jobs. For each cycle: select the next model from the
    rotation directory and sign+publish its hash (miner_commit_1), upload to
    HF, then commit the hash+HF coords (miner_commit_2). Each step lives in its
    own helper for readability.

    `subtensor` (archive) is used for reads/phase checks. `commit_subtensor` is
    the node the commit extrinsics are SUBMITTED through: a fast lite (finney)
    node, since archive-node inclusion can take minutes and push the commit past
    the ~2-block MinerCommit2 window (the validator then can't score it).
    """
    if subtensor is None:
        subtensor = bittensor.Subtensor(config.chain.network)
    if commit_subtensor is None:
        commit_subtensor = subtensor
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
            # Prepare AHEAD of the window (during Train): rotation-model select
            # + multi-GB hash. Stamp the version/dedup key with the UPCOMING
            # MinerCommit1 start (the value validators use once the window is
            # open); fall back to in-window resolution if the API is down.
            upcoming_version = _upcoming_commit1_version(config)
            latest_checkpoint, previous_model = _prepare_checkpoint_for_commit(
                config, wallet, shared_state, previous_model,
                commit1_version=upcoming_version,
            )

            # Now block until the window opens and submit immediately — the hash
            # is already done, so the extrinsic lands in the first block(s).
            phase_response = wait_till(config, PhaseNames.miner_commit_1)
            _commit_signed_model_hash(config, wallet, commit_subtensor, latest_checkpoint)
            check_phase_expired(subtensor, phase_response)

            # HF upload runs between the two commits so the revision is known
            # by the time we write miner_commit_2. Failure returns (None, None)
            # and the chain commit goes out without r/rv — the miner is then
            # missing for this round and gets the zero-score penalty.
            hf_chain_repo_id, hf_revision = _upload_checkpoint_to_hf_safe(config, latest_checkpoint)

            phase_response = wait_till(config, PhaseNames.miner_commit_2)
            _commit_model_hash(
                config, wallet, commit_subtensor, latest_checkpoint,
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
def run_system(config, wallet, expert_manager, current_model_version: int = 0, current_model_hash: str = "xxx", subtensor=None, commit_subtensor=None):
    if subtensor is None:
        subtensor = bittensor.Subtensor(config.chain.network)
    if commit_subtensor is None:
        commit_subtensor = subtensor

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
        args=(config, commit_queue, wallet, shared_state, subtensor, commit_subtensor),
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

    wallet, subtensor, lite_subtensor = setup_chain_worker(config)

    expert_manager = ExpertManager(config)

    # Submit commit extrinsics through the fast lite (finney) node; the archive
    # node's slow inclusion was pushing commits past the MinerCommit2 window.
    run_system(config, wallet, expert_manager, subtensor=subtensor, commit_subtensor=lite_subtensor)
