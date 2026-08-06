# Connito rotation-commit fleet — runbook

How to run this custom Connito setup (local phase server + model-rotation miners) and the `models/` shell helpers on another host.

## Layout

```
<BASE>/                          # e.g. /root/sn102-connitor
├── Connito/                     # git repo (custom branch)
│   ├── RUNBOOK.md               # this file
│   ├── ecosystem.config.js      # PM2: phase server + miners
│   ├── phase_periods.json       # MUST match owner live periods
│   ├── scripts/
│   │   ├── clone_recommended.sh # cron: pull recommend list
│   │   ├── auto_update.sh       # optional: pull /api/top kings
│   │   └── swap_models.sh       # manual HF tree URLs
│   ├── cache/
│   │   ├── cycle_api.json       # written by phase server
│   │   └── model_claims.json    # per-cycle rotation claims
│   ├── checkpoints/miner/<coldkey>/<hotkey>/foundation/config.yaml
│   └── expert_groups/exp_nemotron_c4/   # locked task = group 4
└── models/                      # rotation pool (sibling of Connito/)
    └── <owner>_<repo>_<7char>/  # each dir has model_expgroup_*.safetensors
```

Miners resolve the rotation pool as:

`Path(config.run.root_path).resolve().parent / "models"`

With PM2 `cwd = Connito/`, that is `<BASE>/models`. **Do not** put models inside `Connito/models`.

---

## What this setup does

1. **Local phase server** (`:8088`) computes the cycle schedule from chain block height + `phase_periods.json` (avoids Cloudflare-blocked owner API).
2. **Miners** use `CONNITO_OWNER_URL=http://127.0.0.1:8088` and `CONNITO_SKIP_DOWNLOAD=1`.
3. During Train they **claim** a dir from `<BASE>/models` that has a shard for the active expert group (`model_expgroup_4.safetensors` today), hash it, upload to HF at MinerCommit1, then Commit2 with `'e': 4` + `r`/`rv`.

Locked subnet task (upstream PR #203): **`exp_nemotron_c4` / expert group 4**. Validators ignore other groups.

---

## Prerequisites

| Need | Notes |
|------|--------|
| Linux host | Enough disk for models (~3–4 GB each; keep=10 → ~40–55 GB) |
| Python 3.10+ | With Connito deps; **bittensor 10.5** |
| `pm2` | Process manager |
| `hf` CLI | Hugging Face CLI (`pip install huggingface_hub` / `hf`) |
| `curl`, `flock`, `python3` | Used by models scripts |
| Bittensor wallet | Coldkey + hotkeys registered on netuid **102** |
| `HF_TOKEN` | Write access to upload repos (`athena2634/coho0000N` or your `hf.default_repo_name`) |
| Chain RPC | Config uses `network: archive`, `lite_network: finney` (commits go via lite) |

Wallet path (typical): `~/.bittensor/wallets/<coldkey>/hotkeys/<hotkey>`.

---

## 1. Get the code

```bash
# Parent directory
mkdir -p /path/to/sn102-connitor && cd /path/to/sn102-connitor

# Clone your fork / branch that has rotation-commit + local phase
git clone <your-connito-remote> Connito
cd Connito
git checkout feat/rotation-commit-local-phase   # or whatever branch you ship

# Install deps (follow Connito README / uv / pip as you use today)
# Confirm: python3 -c "import bittensor; print(bittensor.__version__)"  # expect 10.5.x
```

### Local patches you must keep (not always on upstream)

These are applied on the current host and are required:

1. **`phase_periods.json`** — sync to owner live lengths (today):

```json
{
  "distribute_period": 20,
  "train_period": 340,
  "commit_period": 11,
  "submission_period": 60,
  "validate_period": 10,
  "merge_period": 50
}
```

If Train/Submission lengths drift from owner, miners commit in the wrong window. Re-check owner `/` when the schedule changes. Cycle length in practice is **524** blocks (owner), even if miner `config.yaml` still shows older `cycle_length` / period defaults — the phase server overrides from this file.

2. **`connito/miner/model_io.py` — `_rotation_models_dir`** must use `.resolve()` before `.parent`, otherwise `root_path='.'` collapses to `Connito/models` and every commit fails with “No group-N shard”.

---

## 2. Configure for the new host

Edit `Connito/ecosystem.config.js`:

| Constant | Set to |
|----------|--------|
| `COLDKEY` | Your coldkey name (e.g. `lucky-connitor`) |
| `REPO` | Absolute path to `Connito/` on the new host |
| `HOTKEYS` | Hotkeys you run (e.g. `h00000` … `h00004`) |
| `PHASE_SERVER_PORT` | Default `8088` |

Ensure each hotkey has:

`checkpoints/miner/<COLDKEY>/<hotkey>/foundation/config.yaml`

Important fields:

- `chain.network: archive` / `lite_network: finney`
- `hf.default_repo_name`: unique per hotkey (e.g. `coho00000`)
- `task.expert_group_name: exp_nemotron_c4` (auto-updated from locked defaults on restart if config is stale)

Create the models dir (weights only — scripts live in `Connito/scripts/`):

```bash
mkdir -p /path/to/sn102-connitor/models
chmod +x /path/to/sn102-connitor/Connito/scripts/*.sh
```

---

## 3. Seed the model pool

Active shard name: **`model_expgroup_4.safetensors`** (group 4).

### Recommended (cron source of truth)

```bash
cd /path/to/sn102-connitor/Connito
export HF_TOKEN=hf_xxx   # if private/gated; public may work without

./scripts/clone_recommended.sh \
  --keep 10 \
  --endpoint 'http://95.216.38.46:8791/api/recommend?cohort=C'
```

- Downloads into `{owner}_{repo}_{7-char-rev}/`
- Keeps at most `--keep` weight dirs
- **Never prunes** current recommendations or dirs listed in `Connito/cache/model_claims.json` (miners claim during Train; pruning those mid-cycle causes Commit2 without `r`/`rv`)

Optional dry-run: add `--dry-run`.

### Optional: top/kings

```bash
./scripts/auto_update.sh --keep 10 --endpoint 'http://95.216.38.46:8791/api/top'
```

### Manual HF trees

```bash
./scripts/swap_models.sh \
  https://huggingface.co/owner/repo/tree/abcdef0 \
  https://huggingface.co/owner/repo2/tree/1234567
```

### Cron (every 15 minutes)

```cron
*/15 * * * * PATH=/usr/local/bin:/usr/bin:/bin HOME=/root \
  /path/to/sn102-connitor/Connito/scripts/clone_recommended.sh --keep 10 \
  --endpoint 'http://95.216.38.46:8791/api/recommend?cohort=C' \
  >> /path/to/sn102-connitor/models/clone_recommended.log 2>&1
```

Scripts share lock `/tmp/sn102_models.lock` (skip if another run is active).

Override claims path if needed:

```bash
export CONNITO_MODEL_CLAIMS=/path/to/sn102-connitor/Connito/cache/model_claims.json
```

---

## 4. Start Connito with PM2

```bash
cd /path/to/sn102-connitor/Connito
export HF_TOKEN=hf_xxx          # required for Commit1 HF upload

pm2 start ecosystem.config.js
pm2 save
```

Starts:

- `connito-phase-server` — `0.0.0.0:8088`
- `connito-miner-h00000` … (one per hotkey)

Miner env (set in ecosystem):

| Env | Value | Purpose |
|-----|--------|---------|
| `HF_TOKEN` | from shell | HF upload |
| `CONNITO_OWNER_URL` | `http://127.0.0.1:8088` | phase API |
| `CONNITO_CYCLE_CACHE_FALLBACK` | `always` | always HTTP to local server |
| `CONNITO_SKIP_DOWNLOAD` | `1` | rotation mode; no Distribute download / heavy archive commits query |

Useful commands:

```bash
pm2 list
pm2 logs connito-phase-server --lines 50
pm2 logs connito-miner-h00000 --lines 80
pm2 restart ecosystem.config.js
# or: pm2 restart connito-phase-server connito-miner-h00000 ...
```

Smoke-check phase server:

```bash
curl -sS http://127.0.0.1:8088/ | head -c 400
# expect JSON with cycle_length / phases (train 340, submission 60, …)
```

---

## 5. Verify a healthy commit cycle

Around MinerCommit2, each miner log should show something like:

```text
Committed status to chain  status={'e': 4, 'h': '...', 'v': ..., 'r': 'user/coho0000N', 'rv': '...'}
```

| Status | Meaning |
|--------|---------|
| `'e': 4` + `r` + `rv` | Good — scorable |
| `'e': 4` only (no `r`/`rv`) | HF upload failed (often pruned model dir) — **not scorable** |
| Only `'m': ...` then error | Commit1 ok, pipeline aborted (often RPC timeout) — missed Commit2 |

Quick check:

```bash
for n in h00000 h00001 h00002 h00003 h00004; do
  echo "==== $n ===="
  grep -E "Committed status|HF upload failed|FileNotFoundError|handshake" \
    ~/.pm2/logs/connito-miner-${n}-out.log | tail -5
done
```

Claims should list one dir per hotkey for the current `cycle_key`, and those dirs must exist under `models/` with `model_expgroup_4.safetensors`.

---

## 6. Operational pitfalls

1. **Wrong expert group** — pool must have `model_expgroup_4.*`. Old group-3-only dirs are useless for scoring.
2. **Prune race** — keep `--keep` ≥ number of miners (e.g. 10 for 5 miners). Claims protection in the shell scripts is mandatory; don’t use an old script that only protects “current recommendations”.
3. **Duplicate model hashes** — if two miners claim dirs with the same weights, validators may dedupe. Prefer a pool with distinct revisions; claims avoid same *directory* name, not same content hash.
4. **RPC / archive timeouts** — after Commit1, archive handshake flakes can abort HF+Commit2. Mild mitigation: free host resources during commit windows. Hard fix: retry around post-Commit1 phase checks (not shipped upstream yet).
5. **Owner API from datacenter IPs** — often Cloudflare-blocked; always use the local phase server.
6. **btcli rate limits** — for CLI overview, prefer a lite endpoint, e.g. `wss://lite.sub.latent.to:443`, not the default Finney entry if you hit `RPC work limit exceeded`.

---

## 7. Minimal “new machine” checklist

- [ ] `Connito/` on branch with rotation-commit + local phase server
- [ ] `_rotation_models_dir` `.resolve()` fix present
- [ ] `phase_periods.json` matches owner (340 / 60 / 11 today)
- [ ] `ecosystem.config.js` paths, coldkey, hotkeys updated
- [ ] Wallets on disk; hotkeys registered on netuid 102
- [ ] `HF_TOKEN` export before `pm2 start`
- [ ] `<BASE>/models` sibling of `Connito/`; scripts executable
- [ ] Seed ≥ N distinct group-4 models (`N` = miner count); cron installed
- [ ] `pm2 start ecosystem.config.js` → phase server + all miners online
- [ ] `curl :8088/` OK; next Commit2 shows `'e': 4` + `r`/`rv` for every hotkey

---

## Reference: current production values (this host)

| Item | Value |
|------|--------|
| Base | `/root/sn102-connitor` |
| Coldkey | `lucky-connitor` |
| Hotkeys | `h00000`–`h00004` |
| Phase port | `8088` |
| Recommend API | `http://95.216.38.46:8791/api/recommend?cohort=C` |
| Keep | `10` |
| Cron | `*/15 * * * *` → `clone_recommended.sh` |
| Expert group | `4` / `exp_nemotron_c4` |
| HF repos | `athena2634/coho0000N` (per hotkey `default_repo_name`) |
