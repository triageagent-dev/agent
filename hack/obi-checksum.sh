#!/usr/bin/env bash
# hack/obi-checksum.sh
#
# Integrity record for the in-tree OBI fork. third_party/obi is maintained only
# here, with no upstream to diff against, so the record is one SHA-256 over
# every git-tracked file under it (path and content), CHECKSUM itself excluded.
# Untracked and gitignored files (generated *_bpfel.go, *.o) never enter it.
#
#   hack/obi-checksum.sh           compare against third_party/obi/CHECKSUM, exit 1 on a mismatch
#   hack/obi-checksum.sh --write   recompute and store it
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

dir=third_party/obi
file=$dir/CHECKSUM

if command -v sha256sum >/dev/null 2>&1; then
  sha=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
  sha=(shasum -a 256)
else
  echo "obi-checksum: neither sha256sum nor shasum is installed" >&2
  exit 2
fi

compute() {
  # Sorted in the C locale so the order is the same on every host. The per-file
  # lines carry the path, so renames and moves change the hash too.
  git ls-files -z -- "$dir" ":(exclude)$file" |
    LC_ALL=C sort -z |
    xargs -0 "${sha[@]}" |
    "${sha[@]}" | cut -d' ' -f1
}

case "${1:-}" in
  --write)
    got=$(compute)
    printf '%s\n' "$got" >"$file"
    echo "obi-checksum: wrote $file ($got)"
    ;;
  "")
    got=$(compute)
    want=$(tr -d ' \t\r\n' <"$file" 2>/dev/null || true)
    if [ "$got" != "$want" ]; then
      echo "obi-checksum: $file does not match the tracked tree under $dir" >&2
      echo "  recorded: ${want:-<missing>}" >&2
      echo "  computed: $got" >&2
      echo "  If the change to the fork is deliberate, run: make obi-checksum" >&2
      exit 1
    fi
    echo "obi-checksum: ok ($got)"
    ;;
  *)
    echo "usage: $0 [--write]" >&2
    exit 2
    ;;
esac
