#!/usr/bin/env bash
#
# auto_update.sh — keep a rolling window of the most recent "king" models.
#
# Polls the subnet top endpoint, downloads any current top ("king") model not
# already present (named  {owner}_{repo}_{7-char-revision} ), and keeps at most
# KEEP (default 10) model directories. When the count exceeds KEEP, the OLDEST
# dirs are removed first — but a model that is in the CURRENT top list is never
# pruned. Models are NOT deleted merely for dropping out of the top; they age
# out only when newer kings push them past the KEEP limit.
#
# Usage:
#   ./auto_update.sh [--keep N] [--endpoint URL] [--dry-run]
#
# Layout: lives in Connito/scripts/ (git) or the sibling models/ pool dir.
# Weight dirs always go to CONNITO_MODELS_DIR, or <BASE>/models by default.
#
# Exit codes: 0 ok, 1 runtime error (download missing weights), 2 usage/fetch error.

set -euo pipefail

# Serialize with clone_recommended.sh (and any other invocation) on one shared
# lock — two updaters must never touch this folder at once.
if [ -z "${_MODELS_LOCKED:-}" ]; then
  exec env _MODELS_LOCKED=1 flock -n /tmp/sn102_models.lock "$0" "$@" || {
    echo "another models update is already running — skipping this run"; exit 0; }
fi

ENDPOINT="http://95.216.38.46:8791/api/top"
# Any expert-group shard qualifies (active group rotates: 0/3/4/…).
KEEP=10
DRY_RUN=0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -d "$SCRIPT_DIR/../connito" ]; then
  REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
  MODELS_DIR="${CONNITO_MODELS_DIR:-$REPO_ROOT/../models}"
elif [ -d "$SCRIPT_DIR/../Connito" ]; then
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
    --endpoint) ENDPOINT="$2"; shift ;;
    --dry-run)  DRY_RUN=1 ;;
    -h|--help)  sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

# --- fetch top list ----------------------------------------------------------
JSON="$(curl -s --max-time 60 "$ENDPOINT")" || { echo "error: failed to fetch $ENDPOINT" >&2; exit 2; }
[ -n "$JSON" ] || { echo "error: empty response from $ENDPOINT" >&2; exit 2; }

# Parse into "repo_id<TAB>revision<TAB>dir" lines (JSON via env, not stdin).
mapfile -t TOP < <(TOP_JSON="$JSON" python3 - <<'PY'
import os, sys, json
try:
    top = json.loads(os.environ["TOP_JSON"])["top"]
except Exception as e:
    sys.stderr.write(f"error: bad JSON from endpoint: {e}\n"); sys.exit(2)
for e in top:
    repo = e["hf_repo_id"].strip(); rev = e["hf_revision"].strip()
    if repo and rev:
        print(f"{repo}\t{rev}\t{repo.replace('/', '_')}_{rev[:7]}")
PY
)
[ "${#TOP[@]}" -gt 0 ] || { echo "error: top list empty or unparseable" >&2; exit 2; }

declare -A WANT                      # dir name -> is a current king
declare -a R_REPO R_REV R_DIR
for line in "${TOP[@]}"; do
  IFS=$'\t' read -r repo rev dir <<<"$line"
  WANT["$dir"]=1
  R_REPO+=("$repo"); R_REV+=("$rev"); R_DIR+=("$dir")
done

# --- what to add (current kings we don't already have) -----------------------
declare -a TO_ADD_IDX=()
for i in "${!R_DIR[@]}"; do
  has_weights "$MODELS_DIR/${R_DIR[$i]}" || TO_ADD_IDX+=("$i")
done

echo "endpoint : $ENDPOINT"
echo "keep     : $KEEP"
echo "top kings: ${#R_DIR[@]}"
echo "to add   : ${#TO_ADD_IDX[@]}"
for i in "${TO_ADD_IDX[@]}"; do echo "    + ${R_REPO[$i]} @ ${R_REV[$i]}  ->  ${R_DIR[$i]}"; done

if [ "$DRY_RUN" = 1 ]; then
  # Predict prune set from current dirs + the ones we would add.
  echo "(dry run — predicting prune)"
fi

# --- download missing kings --------------------------------------------------
if [ "$DRY_RUN" = 0 ]; then
  cd "$MODELS_DIR"
  for i in "${TO_ADD_IDX[@]}"; do
    echo ">> downloading ${R_REPO[$i]} @ ${R_REV[$i]} -> ${R_DIR[$i]}"
    # The `hf` CLI can exit non-zero (raising click.exceptions.Exit) even on a
    # fully successful download, so we DON'T trust its exit code — verify a
    # weights shard landed instead.
    hf download "${R_REPO[$i]}" --revision "${R_REV[$i]}" --local-dir "${R_DIR[$i]}" || true
    if ! has_weights "${R_DIR[$i]}"; then
      echo "warning: download failed for ${R_REPO[$i]} @ ${R_REV[$i]} — skipping" >&2
      rm -rf "${MODELS_DIR:?}/${R_DIR[$i]}"   # drop any partial dir
    fi
  done
  # Verify every current king is present (a king that won't download is fatal).
  for d in "${R_DIR[@]}"; do
    if ! has_weights "$MODELS_DIR/$d"; then
      echo "warning: king $d missing model_expgroup_* shard (repo may lack weights) — continuing" >&2
    fi
  done
fi

# --- protect miner rotation claims (same race as clone_recommended.sh) ------
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

# --- prune oldest while count > KEEP, never evicting kings/claims ----------
# Build list of real model dirs sorted oldest-first.
mapfile -t BY_AGE < <(
  cd "$MODELS_DIR"
  for d in */; do
    d="${d%/}"
    has_weights "$d" || continue
    printf '%s\t%s\n' "$(stat -c '%Y' "$d")" "$d"
  done | sort -n | cut -f2-
)

count="${#BY_AGE[@]}"
declare -a PRUNED=()
if [ "$count" -gt "$KEEP" ]; then
  need=$(( count - KEEP ))
  for d in "${BY_AGE[@]}"; do
    [ "$need" -le 0 ] && break
    [ -n "${WANT[$d]:-}" ] && continue          # never evict a current king
    [ -n "${CLAIMED[$d]:-}" ] && continue        # never evict a miner-claimed model
    if [ "$DRY_RUN" = 1 ]; then
      echo "    - would remove (oldest): $d"
    else
      echo ">> removing (oldest, over keep=$KEEP): $d"
      rm -rf "${MODELS_DIR:?}/$d"
    fi
    PRUNED+=("$d"); need=$(( need - 1 ))
  done
  if [ "$need" -gt 0 ]; then
    echo "note: $need over-limit dir(s) kept (kings and/or miner claims)."
  fi
else
  echo "count $count <= keep $KEEP — nothing to prune."
fi

[ "$DRY_RUN" = 1 ] && { echo "(dry run — no changes made)"; exit 0; }

echo
echo "=== current sorted list (newest first) ==="
ls -lt --time-style=long-iso "$MODELS_DIR" | awk 'NR>1 && $NF!="swap_models.sh" && $NF!="auto_update.sh" && $NF!="auto_update.log"{print $6, $7, $8}'
