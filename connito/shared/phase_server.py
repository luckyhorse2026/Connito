"""Local HTTP phase service — a drop-in for the owner cycle API.

Serves the three block-schedule endpoints by computing them locally with
``PhaseManager`` (chain block height + cycle config), so it never touches the
Cloudflare-protected owner API. It mirrors the owner's routes/shapes, so any
client that hits ``/get_phase`` etc. works unchanged.

    python3 -m connito.shared.phase_server \
        --path checkpoints/miner/<coldkey>/<hotkey>/foundation/config.yaml \
        --host 0.0.0.0 --port 8088

Then reach it at  http://<this-host-ip>:8088/get_phase

NOTE: --host 0.0.0.0 exposes it on every interface (i.e. the public IP). The
data is non-sensitive (public chain + config), but anyone who can reach the port
can read it; firewall the port if you only want your own machines to use it.
This is a thin read-only wrapper over PhaseManager — it carries no keys and
performs no chain writes.
"""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
from pathlib import Path

import bittensor
import uvicorn
from fastapi import FastAPI, HTTPException

from connito.shared.app_logging import configure_logging, structlog
from connito.shared.config import MinerConfig
from connito.shared import cycle
from connito.shared.cycle import PhaseResponse, apply_phase_period_overrides
from connito.shared.chain import _connect_subtensor_with_retry
from connito.sn_owner.cycle import PhaseManager

configure_logging()
logger = structlog.get_logger(__name__)

app = FastAPI(title="Connito Local Phase Service")

# A single background thread does ALL chain access (subtensor.block RPCs) and
# publishes the computed schedule into `_latest`. HTTP handlers serve from
# `_latest` with NO chain RPC and NO lock, so response time is O(1) and
# independent of how many miners poll — the earlier per-request-RPC-under-a-lock
# design serialized everything and timed out under 5-miner load.
_phase_manager: PhaseManager | None = None
_lock = threading.Lock()              # serializes chain access in the refresh thread
_latest: dict | None = None           # last published snapshot (atomic ref swap)
# Path to the validator-whitelist JSON (set in main()); served by the endpoint.
_whitelist_path: str = "validator_whitelist.json"


def _pm() -> PhaseManager:
    if _phase_manager is None:
        raise HTTPException(status_code=503, detail="phase manager not initialized")
    return _phase_manager


def _refresh_snapshot() -> dict:
    """Compute the schedule from chain (the ONLY place chain RPCs happen) and
    publish it atomically into `_latest`. Called by the refresh thread."""
    global _latest
    with _lock:
        snap = {
            "phase": _pm().get_phase(),
            "blocks": _pm().blocks_until_next_phase(),
            "prev": _pm().previous_phase_block_ranges(),
        }
    _latest = snap  # atomic publish; readers see old or new, never partial
    return snap


def _serve(key: str):
    snap = _latest
    if snap is None:
        raise HTTPException(status_code=503, detail="phase service warming up")
    return snap[key]


@app.get("/get_phase", response_model=PhaseResponse)
def read_phase() -> PhaseResponse:
    return _serve("phase")


@app.get("/blocks_until_next_phase", response_model=dict[str, tuple[int, int, int]])
def next_phase() -> dict[str, tuple[int, int, int]]:
    return _serve("blocks")


@app.get("/previous_phase_blocks", response_model=dict[str, tuple[int, int]])
def prev_phase() -> dict[str, tuple[int, int]]:
    return _serve("prev")


@app.get("/get_validator_whitelist", response_model=list[str])
def validator_whitelist() -> list[str]:
    # Owner-maintained list of force-permitted validator hotkeys, mirrored into
    # a local JSON file (re-sync from the owner API when it changes). Used by
    # get_chain_commits for validator/miner role-gating. Read each request so
    # edits apply without a restart; [] on missing/invalid file.
    try:
        with open(_whitelist_path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (OSError, ValueError):
        return []


@app.get("/get_init_peer_id", response_model=list[str])
def init_peer_id() -> list[str]:
    # Validator-only DHT bootstrap; not used by miners. Empty stub.
    return []


@app.get("/")
def root() -> dict:
    pm = _pm()
    return {
        "message": "Connito local phase service is running",
        "cycle_length": pm.cycle_length,
        "phases": [{"index": i, "name": p["name"], "length": p["length"]} for i, p in enumerate(pm.phases)],
    }


def _refresh_loop(config, interval: float, write_cache: bool) -> None:
    """Background thread — the ONLY chain caller. Recompute the schedule and
    publish it to `_latest` (served by the HTTP handlers) and, if enabled, the
    shared file cache. Runs as fast as the chain allows; HTTP stays instant even
    when a refresh is slow, because handlers read the last published snapshot."""
    cache_path = cycle._cycle_cache_path(config)
    base = config.cycle.owner_url
    logger.info("phase refresh loop started", interval_seconds=interval, write_cache=write_cache, cache_path=str(cache_path))
    while True:
        try:
            snap = _refresh_snapshot()
            if write_cache:
                cycle._cache_write_entry(cache_path, f"{base}/get_phase", snap["phase"].model_dump())
                cycle._cache_write_entry(cache_path, f"{base}/blocks_until_next_phase", snap["blocks"])
                cycle._cache_write_entry(cache_path, f"{base}/previous_phase_blocks", snap["prev"])
        except Exception as e:  # noqa: BLE001 — chain RPC can throw; retry next loop
            logger.warning("phase refresh failed", error=str(e))
        time.sleep(interval)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Connito local phase HTTP service")
    parser.add_argument("--path", type=str, required=True, help="Path to a miner config YAML.")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Bind address (default 0.0.0.0).")
    parser.add_argument("--port", type=int, default=8088, help="Bind port (default 8088).")
    parser.add_argument(
        "--write-cache",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Also publish the schedule to the shared file cache miners read (default on).",
    )
    parser.add_argument(
        "--refresh-interval",
        type=float,
        default=5.0,
        help="Seconds between chain refreshes of the served snapshot (default 5).",
    )
    return parser.parse_args()


def main() -> None:
    global _phase_manager, _whitelist_path
    args = _parse_args()
    config = MinerConfig.from_path(args.path)
    apply_phase_period_overrides(config)  # match the owner's live schedule
    _whitelist_path = os.environ.get("CONNITO_VALIDATOR_WHITELIST") or str(
        Path(config.run.root_path) / "validator_whitelist.json"
    )
    subtensor = _connect_subtensor_with_retry(config.chain.network)
    _phase_manager = PhaseManager(config, subtensor)
    logger.info(
        "local phase service starting",
        host=args.host,
        port=args.port,
        network=config.chain.network,
        cycle_length=_phase_manager.cycle_length,
        write_cache=args.write_cache,
        refresh_interval=args.refresh_interval,
    )
    # Prime the snapshot once so handlers don't 503 during the first refresh.
    try:
        _refresh_snapshot()
    except Exception as e:  # noqa: BLE001 — first chain call may be slow; thread retries
        logger.warning("initial snapshot refresh failed; thread will retry", error=str(e))
    # The refresh thread is essential (it feeds the HTTP handlers), so always run it.
    threading.Thread(
        target=_refresh_loop,
        args=(config, args.refresh_interval, args.write_cache),
        daemon=True,
    ).start()
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
