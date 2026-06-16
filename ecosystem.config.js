// PM2 process definitions for the Connito miner fleet.
//
// Architecture: ONE phase-oracle process computes the cycle schedule locally
// from the chain block height + cycle config (the owner API is just this same
// deterministic math) and writes a shared JSON cache; every miner reads that
// cache. No owner-API call at all, so the Cloudflare datacenter-IP challenge is
// sidestepped entirely. See connito/shared/phase_oracle.py and the cache note
// in connito/shared/cycle.py.
//
// Start with:  HF_TOKEN=hf_xxx pm2 start ecosystem.config.js
const COLDKEY = "lucky-connitor";
const REPO = "/root/sn102-connitor/Connito";
// Single shared cache file, addressed absolutely so the oracle (writer) and all
// miners (readers) agree on the location regardless of cwd/root resolution.
const CACHE = `${REPO}/cache/cycle_api.json`;
// Any miner's config works for the oracle — it only reads owner_url + api_*.
const ORACLE_CONFIG = `checkpoints/miner/${COLDKEY}/h00000/foundation/config.yaml`;

// The oracle: the only process allowed to hit the owner cycle API.
const oracle = {
  name: "connito-phase-oracle",
  script: "python3",
  args: `-m connito.shared.phase_oracle --path ${ORACLE_CONFIG}`,
  interpreter: "none",
  cwd: REPO,
  autorestart: true,
  max_restarts: 50,
  restart_delay: 5000,
  min_uptime: "20s",
  merge_logs: true,
  time: true,
  out_file: "/root/.pm2/logs/connito-phase-oracle-out.log",
  error_file: "/root/.pm2/logs/connito-phase-oracle-error.log",
  env: {
    PYTHONUNBUFFERED: "1",
    CONNITO_CYCLE_ORACLE: "1",
    CONNITO_CYCLE_CACHE: CACHE,
    // Source defaults to "local": compute the schedule from chain block +
    // config, no owner-API call. Set CONNITO_CYCLE_ORACLE_SOURCE=http to fetch
    // from the owner API instead (only where that API is reachable).
  },
};

// Local HTTP phase service on 0.0.0.0:8088 — serves /get_phase etc. computed
// locally, reachable at http://<host-ip>:8088. Independent of the file-cache
// oracle (this one is for IP access / other machines). Applies the live
// phase_periods.json override on startup.
const PHASE_SERVER_PORT = 8088;
const server = {
  name: "connito-phase-server",
  script: "python3",
  args: `-m connito.shared.phase_server --path ${ORACLE_CONFIG} --host 0.0.0.0 --port ${PHASE_SERVER_PORT}`,
  interpreter: "none",
  cwd: REPO,
  autorestart: true,
  max_restarts: 50,
  restart_delay: 5000,
  min_uptime: "20s",
  merge_logs: true,
  time: true,
  out_file: "/root/.pm2/logs/connito-phase-server-out.log",
  error_file: "/root/.pm2/logs/connito-phase-server-error.log",
  env: {
    PYTHONUNBUFFERED: "1",
    // The server also publishes the schedule to this shared cache (background
    // thread, --write-cache default on), so local miners read it with no
    // owner_url change. This replaces the standalone phase oracle.
    CONNITO_CYCLE_CACHE: CACHE,
  },
};

// One miner process per hotkey. interpreter "none" makes PM2 exec `script`
// directly with `args`, so this runs: python3 -m connito.miner.model_io --path <cfg>
// `-m` is required — model_io.py uses absolute `connito.*` imports. cwd must be
// the repo root so `connito` is importable, find_project_root() anchors here
// (.git marker), and ../models resolves to /root/sn102-connitor/models.
const miner = (hotkey) => ({
  name: `connito-miner-${hotkey}`,
  script: "python3",
  args: `-m connito.miner.model_io --path checkpoints/miner/${COLDKEY}/${hotkey}/foundation/config.yaml`,
  interpreter: "none",
  cwd: REPO,
  autorestart: true,
  max_restarts: 10,
  restart_delay: 5000,
  // The commit loop is long-lived and mostly idle between phases; don't
  // let PM2 treat low memory churn as a crash.
  min_uptime: "30s",
  merge_logs: true,
  time: true,
  out_file: `/root/.pm2/logs/connito-miner-${hotkey}-out.log`,
  error_file: `/root/.pm2/logs/connito-miner-${hotkey}-error.log`,
  env: {
    PYTHONUNBUFFERED: "1",
    // Required for the HF upload of the selected model. Inherited from the
    // shell that runs `pm2 start`; export HF_TOKEN there (or hardcode here).
    HF_TOKEN: process.env.HF_TOKEN || "",
    // Get phase data over HTTP from the local phase server. CONNITO_OWNER_URL
    // overrides the locked owner_url so cycle-API calls hit the server; the
    // "always" fallback makes _get_json call out (the localhost URL is never in
    // the file cache, so it falls straight through to the HTTP request).
    CONNITO_OWNER_URL: `http://127.0.0.1:${PHASE_SERVER_PORT}`,
    CONNITO_CYCLE_CACHE_FALLBACK: "always",
    // Rotation-commit miners commit pre-downloaded models from ../models and
    // never use the validator model from Distribute. Skip the download worker
    // to drop the heavy per-cycle get_chain_commits archive query.
    CONNITO_SKIP_DOWNLOAD: "1",
  },
});

// Add more hotkeys here as you register them.
const HOTKEYS = ["h00000", "h00001", "h00002", "h00003", "h00004"];
// The phase server now writes the shared cache itself (see its env), so the
// standalone `oracle` is redundant and intentionally left out of the app list.
// Re-add it (apps: [oracle, ...]) only if you want a separate cache writer.
module.exports = {
  apps: [server, ...HOTKEYS.map(miner)],
};
