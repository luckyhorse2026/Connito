"""Local phase oracle — computes the owner cycle schedule on this host.

The owner phase API (``/get_phase`` etc.) is just a deterministic function of
the chain block height and the cycle config: see ``PhaseManager`` in
``connito/sn_owner/cycle.py`` — ``cycle_index = block // cycle_length`` with
``cycle_length = sum(phase periods)``. There is no owner-private state, so we
reproduce it locally from our own subtensor + config and write the results into
the shared cache that every miner already reads (see the cache note in
``connito/shared/cycle.py``). No owner-API call, nothing for Cloudflare to block.

The miner commit path only needs the three block-schedule endpoints
(get_phase / blocks_until_next_phase / previous_phase_blocks); the validator
whitelist + init-peer endpoints are owner-maintained and not used by miners, so
they are intentionally not produced here.

Run exactly one per host (any miner's config works):

    python3 -m connito.shared.phase_oracle \
        --path checkpoints/miner/<coldkey>/<hotkey>/foundation/config.yaml

It marks itself CONNITO_CYCLE_ORACLE=1 so the cache helpers write rather than
read. Source defaults to local computation; pass --source http to fetch from the
owner API instead (only useful where that API is reachable).
"""

from __future__ import annotations

import argparse
import os
import time

import bittensor

from connito.shared.app_logging import configure_logging, structlog
from connito.shared.config import MinerConfig
from connito.shared import cycle
from connito.sn_owner.cycle import PhaseManager

configure_logging()
logger = structlog.get_logger(__name__)

# Refresh cadence. Bittensor blocks are ~12s; recomputing every 8s keeps the
# cache inside the readers' default 30s freshness window with margin.
_DEFAULT_INTERVAL_SECONDS = 8.0

# HTTP-mode endpoints (only the block-schedule ones the miner needs).
_HTTP_ENDPOINTS = (
    ("get_phase", cycle.get_phase_from_api),
    ("blocks_until_next_phase", cycle.get_blocks_until_next_phase_from_api),
    ("previous_phase_blocks", cycle.get_blocks_from_previous_phase_from_api),
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Connito local phase oracle")
    parser.add_argument("--path", type=str, required=True, help="Path to a miner config YAML.")
    parser.add_argument(
        "--source",
        choices=["local", "http"],
        default=os.environ.get("CONNITO_CYCLE_ORACLE_SOURCE", "local"),
        help="local: compute from chain block + config (default). http: owner API.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("CONNITO_CYCLE_ORACLE_INTERVAL", _DEFAULT_INTERVAL_SECONDS)),
        help="Seconds between refreshes (default 8).",
    )
    return parser.parse_args()


def _write_local(config, pm: PhaseManager, cache_path) -> "object":
    """Compute the three block-schedule endpoints and write them into the cache
    under the exact URL keys the miners read (see *_from_api in cycle.py)."""
    base = config.cycle.owner_url
    phase = pm.get_phase()
    cycle._cache_write_entry(cache_path, f"{base}/get_phase", phase.model_dump())
    cycle._cache_write_entry(
        cache_path, f"{base}/blocks_until_next_phase", pm.blocks_until_next_phase()
    )
    cycle._cache_write_entry(
        cache_path, f"{base}/previous_phase_blocks", pm.previous_phase_block_ranges()
    )
    return phase


def _run_local(config, cache_path, interval: float) -> None:
    subtensor = bittensor.Subtensor(config.chain.network)
    pm = PhaseManager(config, subtensor)
    logger.info(
        "phase oracle (local) starting",
        network=config.chain.network,
        cycle_length=pm.cycle_length,
        phases=[(p["name"], p["length"]) for p in pm.phases],
        cache_path=str(cache_path),
        interval_seconds=interval,
    )
    while True:
        try:
            phase = _write_local(config, pm, cache_path)
            logger.debug(
                "phase oracle refresh ok",
                block=phase.block,
                phase=phase.phase_name,
                blocks_remaining=phase.blocks_remaining_in_phase,
            )
        except Exception as e:  # noqa: BLE001 — chain RPC can throw; recover next loop
            logger.warning("phase oracle local refresh failed; refreshing subtensor", error=str(e))
            try:
                subtensor = bittensor.Subtensor(config.chain.network)
                pm = PhaseManager(config, subtensor)
            except Exception as re:  # noqa: BLE001
                logger.warning("failed to refresh subtensor", error=str(re))
        time.sleep(interval)


def _run_http(config, cache_path, interval: float) -> None:
    logger.info(
        "phase oracle (http) starting",
        owner_url=config.cycle.owner_url,
        cache_path=str(cache_path),
        interval_seconds=interval,
    )
    while True:
        ok, failed = [], []
        for name, fn in _HTTP_ENDPOINTS:
            try:
                result = fn(config)
            except Exception as e:  # noqa: BLE001
                failed.append(name)
                logger.warning("phase oracle endpoint errored", endpoint=name, error=str(e))
                continue
            (ok if result is not None else failed).append(name)
        if failed:
            logger.warning("phase oracle http refresh: endpoints unavailable", ok=ok, failed=failed)
        else:
            logger.debug("phase oracle http refresh ok", endpoints=ok)
        time.sleep(interval)


def main() -> None:
    # Mark this process as the oracle BEFORE any cycle call so the cache helpers
    # write instead of read.
    os.environ[cycle._CYCLE_ORACLE_ENV] = "1"

    args = _parse_args()
    config = MinerConfig.from_path(args.path)
    cycle.apply_phase_period_overrides(config)  # match the owner's live schedule
    cache_path = cycle._cycle_cache_path(config)

    if args.source == "local":
        _run_local(config, cache_path, args.interval)
    else:
        _run_http(config, cache_path, args.interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("phase oracle stopped")
