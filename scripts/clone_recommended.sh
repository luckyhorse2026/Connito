#!/usr/bin/env bash
#
# clone_recommended.sh — clone the subnet's *recommended* models.
#
# Unlike auto_update.sh (which tracks the raw top/king list from /api/top),
# this pulls the ranked recommendations from /api/recommend — each carries a
# composite `score` (quality/trust/freshness/trajectory) plus val_loss — and
# clones them into  {owner}_{repo}_{7-char-revision}  directories.
#
# Behaviour (same safe pattern as the other scripts):
#   * download any recommended model not already present
#   * a failed/partial download is cleaned up and never triggers a prune
#   * keep at most KEEP dirs; when the count exceeds KEEP the OLDEST are removed
#     first, but a dir that is a CURRENT recommendation is never evicted
#   * optionally clone only the top --take N recommendations
#
# Usage:
#   ./clone_recommended.sh [--keep N] [--take N] [--endpoint URL] [--dry-run]
#
# Layout: lives in Connito/scripts/ (git) or the sibling models/ pool dir.
# Weight dirs always go to CONNITO_MODELS_DIR, or <BASE>/models by default.
#
# Exit codes: 0 ok, 2 usage/fetch error.

set -euo pipefail

# Serialize every invocation (cron or manual) on one shared lock so two updaters
# can never touch this folder at once — concurrent runs race on the same download
# dirs and prune against stale views. Non-blocking: if another run holds the lock,
# exit quietly and let the next tick retry.
if [ -z "${_MODELS_LOCKED:-}" ]; then
  exec env _MODELS_LOCKED=1 flock -n /tmp/sn102_models.lock "$0" "$@" || {
    echo "another models update is already running — skipping this run"; exit 0; }
fi

ENDPOINT="http://95.216.38.46:8791/api/recommend"
# Any expert-group shard qualifies — the subnet rotates active groups
# (exp_math=0, exp_legal=3, …). Requiring only group_0 deleted valid
# downloads (e.g. group_3/4-only repos) and emptied the rotation pool.
KEEP=10
TAKE=0            # 0 = clone every recommendation returned
DRY_RUN=0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -d "$SCRIPT_DIR/../connito" ]; then
  # Connito/scripts/*.sh
  REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
  MODELS_DIR="${CONNITO_MODELS_DIR:-$REPO_ROOT/../models}"
elif [ -d "$SCRIPT_DIR/../Connito" ]; then
  # <BASE>/models/*.sh (sibling of Connito/)
  REPO_ROOT="$(cd "$SCRIPT_DIR/../Connito" && pwd)"
  MODELS_DIR="${CONNITO_MODELS_DIR:-$SCRIPT_DIR}"
else
  REPO_ROOT="${CONNITO_REPO:-$(cd "$SCRIPT_DIR/.." && pwd)}"
  MODELS_DIR="${CONNITO_MODELS_DIR:-$SCRIPT_DIR}"
fi
mkdir -p "$MODELS_DIR"
MODELS_DIR="$(cd "$MODELS_DIR" && pwd)"

has_weights() {
  local d="$1"
  compgen -G "$d/model_expgroup_*.safetensors" >/dev/null \
    || compgen -G "$d/model_expgroup_*.pt" >/dev/null
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --keep)     KEEP="$2"; shift ;;
    --take)     TAKE="$2"; shift ;;
    --endpoint) ENDPOINT="$2"; shift ;;
    --dry-run)  DRY_RUN=1 ;;
    -h|--help)  sed -n '2,25p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

# --- fetch recommendations ---------------------------------------------------
JSON="$(curl -s --max-time 60 "$ENDPOINT")" || { echo "error: failed to fetch $ENDPOINT" >&2; exit 2; }
[ -n "$JSON" ] || { echo "error: empty response from $ENDPOINT" >&2; exit 2; }

# Parse into "repo<TAB>rev<TAB>dir<TAB>score<TAB>val_loss" lines, best score first.
# (JSON passed via env var, not stdin, so it can't clash with the heredoc.)
mapfile -t RECS < <(TOP_JSON="$JSON" TAKE="$TAKE" python3 - <<'PY'
import os, sys, json
try:
    doc = json.loads(os.environ["TOP_JSON"])
    recs = doc["recommendations"]
except Exception as e:
    sys.stderr.write(f"error: bad JSON from endpoint: {e}\n"); sys.exit(2)
# already ranked by the endpoint; keep that order
take = int(os.environ.get("TAKE", "0") or "0")
if take > 0:
    recs = recs[:take]
for r in recs:
    repo = (r.get("hf_repo_id") or "").strip()
    rev  = (r.get("hf_revision") or "").strip()
    if not (repo and rev):
        continue
    dir_ = f"{repo.replace('/', '_')}_{rev[:7]}"
    score = r.get("score", "")
    vloss = r.get("val_loss", "")
    print(f"{repo}\t{rev}\t{dir_}\t{score}\t{vloss}")
PY
)
[ "${#RECS[@]}" -gt 0 ] || { echo "error: recommendation list empty or unparseable" >&2; exit 2; }

declare -A WANT                       # dir -> is a current recommendation
declare -a R_REPO R_REV R_DIR R_SCORE R_VLOSS
for line in "${RECS[@]}"; do
  IFS=$'\t' read -r repo rev dir score vloss <<<"$line"
  WANT["$dir"]=1
  R_REPO+=("$repo"); R_REV+=("$rev"); R_DIR+=("$dir"); R_SCORE+=("$score"); R_VLOSS+=("$vloss")
done

# --- what to add (recommendations we don't already have) ---------------------
declare -a TO_ADD_IDX=()
for i in "${!R_DIR[@]}"; do
  has_weights "$MODELS_DIR/${R_DIR[$i]}" || TO_ADD_IDX+=("$i")
done

echo "endpoint       : $ENDPOINT"
echo "keep           : $KEEP"
echo "recommendations: ${#R_DIR[@]}${TAKE:+ (take=$TAKE)}"
printf '%-4s %-8s %-9s %-38s\n' "rank" "score" "val_loss" "model"
for i in "${!R_DIR[@]}"; do
  have=" "; has_weights "$MODELS_DIR/${R_DIR[$i]}" && have="*"
  printf '%-4s %-8s %-9s %s %s\n' "$((i+1))" "${R_SCORE[$i]}" "${R_VLOSS[$i]}" "$have" "${R_REPO[$i]} @ ${R_REV[$i]}"
done
echo "( * = already present )"
echo "to add         : ${#TO_ADD_IDX[@]}"

# --- download missing recommendations ----------------------------------------
if [ "$DRY_RUN" = 0 ]; then
  cd "$MODELS_DIR"
  for i in "${TO_ADD_IDX[@]}"; do
    echo ">> downloading ${R_REPO[$i]} @ ${R_REV[$i]} -> ${R_DIR[$i]}"
    # The `hf` CLI can exit non-zero (raising click.exceptions.Exit) even on a
    # fully successful download, so we DON'T trust its exit code — we verify that
    # a weights shard actually landed instead.
    hf download "${R_REPO[$i]}" --revision "${R_REV[$i]}" --local-dir "${R_DIR[$i]}" || true
    if has_weights "${R_DIR[$i]}"; then
      echo "   ok: ${R_DIR[$i]} ($(echo "${R_DIR[$i]}"/model_expgroup_*.safetensors 2>/dev/null | xargs -n1 basename | tr '\n' ' '))"
    else
      echo "warning: ${R_REPO[$i]} @ ${R_REV[$i]} produced no model_expgroup_* shard — skipping" >&2
      rm -rf "${MODELS_DIR:?}/${R_DIR[$i]}"   # drop any partial dir
    fi
  done
fi

# --- protect miner rotation claims ------------------------------------------
# Miners prepare (hash) a model during Train, then upload it at MinerCommit1.
# Cron prune used to delete those dirs in the gap → Commit2 without HF coords
# (no r/rv) so validators treat the miner as missing. Never evict a dir that
# any live miner has claimed for the current cycle.
CLAIMS_FILE="${CONNITO_MODEL_CLAIMS:-$REPO_ROOT/cache/model_claims.json}"
declare -A CLAIMED=()
if [ -f "$CLAIMS_FILE" ]; then
  while IFS= read -r name; do
    [ -n "$name" ] && CLAIMED["$name"]=1
  done < <(CLAIMS_FILE="$CLAIMS_FILE" python3 - <<'PY'
import json, os
try:
    data = json.load(open(os.environ["CLAIMS_FILE"], encoding="utf-8"))
except Exception:
    raise SystemExit(0)
for name in (data.get("claims") or {}).values():
    if isinstance(name, str) and name:
        print(name)
PY
)
fi
if [ "${#CLAIMED[@]}" -gt 0 ]; then
  echo "protected claims: ${!CLAIMED[*]}"
fi

# --- prune oldest while count > KEEP, never evicting recommendations/claims -
mapfile -t BY_AGE < <(
  cd "$MODELS_DIR"
  for d in */; do
    d="${d%/}"
    has_weights "$d" || continue
    printf '%s\t%s\n' "$(stat -c '%Y' "$d")" "$d"
  done | sort -n | cut -f2-
)

count="${#BY_AGE[@]}"
if [ "$count" -gt "$KEEP" ]; then
  need=$(( count - KEEP ))
  for d in "${BY_AGE[@]}"; do
    [ "$need" -le 0 ] && break
    [ -n "${WANT[$d]:-}" ] && continue          # never evict a current recommendation
    [ -n "${CLAIMED[$d]:-}" ] && continue        # never evict a miner-claimed model
    if [ "$DRY_RUN" = 1 ]; then
      echo "    - would remove (oldest): $d"
    else
      echo ">> removing (oldest, over keep=$KEEP): $d"
      rm -rf "${MODELS_DIR:?}/$d"
    fi
    need=$(( need - 1 ))
  done
  [ "$need" -gt 0 ] && echo "note: $need over-limit dir(s) kept (recommendations and/or miner claims)."
else
  echo "count $count <= keep $KEEP — nothing to prune."
fi

[ "$DRY_RUN" = 1 ] && { echo "(dry run — no changes made)"; exit 0; }

echo
echo "=== current sorted list (newest first) ==="
ls -lt --time-style=long-iso "$MODELS_DIR" | awk 'NR>1 && $NF!="swap_models.sh" && $NF!="auto_update.sh" && $NF!="clone_recommended.sh" && $NF!="auto_update.log"{print $6, $7, $8}'
