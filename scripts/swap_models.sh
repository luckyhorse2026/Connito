#!/usr/bin/env bash
#
# swap_models.sh — download new HuggingFace model revisions and prune the oldest.
#
# Usage:
#   ./swap_models.sh URL [URL ...]
#
# For each HuggingFace "tree" URL given, downloads that repo at that revision
# into a directory named  {owner}_{repo}_{7-char-revision}  (the established
# naming rule). After all downloads succeed, removes the N oldest model
# directories, where N is the number of URLs supplied — keeping the total
# count stable. Removal happens only after every download verifies, so a
# failed download never costs you an existing model.
#
# Example:
#   ./swap_models.sh \
#     https://huggingface.co/doubtsjohn/c2-b11/tree/2063cbc \
#     https://huggingface.co/maximso/con14/tree/975bd92 \
#     https://huggingface.co/dailyzz/co9/tree/022d1e9
#
# Layout: lives in Connito/scripts/ (git) or the sibling models/ pool dir.
# Weight dirs always go to CONNITO_MODELS_DIR, or <BASE>/models by default.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -d "$SCRIPT_DIR/../connito" ]; then
  MODELS_DIR="${CONNITO_MODELS_DIR:-$SCRIPT_DIR/../../models}"
elif [ -d "$SCRIPT_DIR/../Connito" ]; then
  MODELS_DIR="${CONNITO_MODELS_DIR:-$SCRIPT_DIR}"
else
  MODELS_DIR="${CONNITO_MODELS_DIR:-$SCRIPT_DIR}"
fi
mkdir -p "$MODELS_DIR"
MODELS_DIR="$(cd "$MODELS_DIR" && pwd)"

# Any expert-group shard qualifies (active group rotates: 0/3/4/…).
has_weights() {
  local d="$1"
  compgen -G "$d/model_expgroup_*.safetensors" >/dev/null \
    || compgen -G "$d/model_expgroup_*.pt" >/dev/null
}

if [ "$#" -lt 1 ]; then
  echo "usage: $0 URL [URL ...]" >&2
  exit 2
fi

# --- parse URLs into repo_id / revision / target dir -------------------------
declare -a REPO_IDS REVS DIRS
for url in "$@"; do
  # strip protocol + host -> "owner/repo/tree/<rev>"
  path="${url#http*://huggingface.co/}"
  if [[ "$path" != */tree/* ]]; then
    echo "error: not a HF tree URL (missing /tree/<rev>): $url" >&2
    exit 2
  fi
  repo_id="${path%%/tree/*}"          # owner/repo
  rev="${path#*/tree/}"               # revision (commit/branch)
  rev="${rev%%/*}"                    # drop any trailing path
  if [ -z "$repo_id" ] || [ -z "$rev" ]; then
    echo "error: could not parse owner/repo/revision from: $url" >&2
    exit 2
  fi
  short="${rev:0:7}"                  # 7-char suffix per naming rule
  dir="${repo_id//\//_}_${short}"     # owner_repo_<7char>
  REPO_IDS+=("$repo_id"); REVS+=("$rev"); DIRS+=("$dir")
done

N="${#DIRS[@]}"

# --- determine the N oldest dirs to prune (computed BEFORE downloading) ------
# Only consider existing model dirs (those containing the weights file).
mapfile -t OLDEST < <(
  cd "$MODELS_DIR"
  for d in */; do
    d="${d%/}"
    has_weights "$d" || continue
    printf '%s\t%s\n' "$(stat -c '%Y' "$d")" "$d"
  done | sort -n | head -n "$N" | cut -f2-
)

echo "Plan:"
echo "  add ($N):"
for i in "${!DIRS[@]}"; do echo "    ${REPO_IDS[$i]} @ ${REVS[$i]}  ->  ${DIRS[$i]}"; done
echo "  remove oldest (${#OLDEST[@]}):"
for d in "${OLDEST[@]}"; do echo "    $d"; done
echo

# --- download all new revisions ----------------------------------------------
cd "$MODELS_DIR"
for i in "${!DIRS[@]}"; do
  dir="${DIRS[$i]}"
  echo ">> downloading ${REPO_IDS[$i]} @ ${REVS[$i]} -> $dir"
  # The `hf` CLI can exit non-zero (raising click.exceptions.Exit) even on a
  # fully successful download; don't let that abort us under `set -e`. The
  # verify-weights loop below is the real success check.
  hf download "${REPO_IDS[$i]}" --revision "${REVS[$i]}" --local-dir "$dir" || true
done

# --- verify each download produced a weights shard ---------------------------
for dir in "${DIRS[@]}"; do
  if ! has_weights "$MODELS_DIR/$dir"; then
    echo "error: $dir is missing model_expgroup_* shard — aborting before any removal" >&2
    exit 1
  fi
done
echo "all $N downloads verified."

# --- prune oldest (only now that downloads are safe) -------------------------
for d in "${OLDEST[@]}"; do
  # never delete something we just created
  skip=
  for nd in "${DIRS[@]}"; do [ "$d" = "$nd" ] && skip=1; done
  [ -n "$skip" ] && continue
  echo ">> removing oldest: $d"
  rm -rf "${MODELS_DIR:?}/$d"
done

echo
echo "=== current sorted list (newest first) ==="
ls -lt --time-style=long-iso "$MODELS_DIR" | awk 'NR>1{print $6, $7, $8}'
