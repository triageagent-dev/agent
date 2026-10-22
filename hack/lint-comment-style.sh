#!/usr/bin/env bash
# hack/lint-comment-style.sh
#
# Gate against audit-storytelling comments: a source comment should state
# the invariant the code upholds, not cite which numbered finding/report
# produced it — a citation drifts the moment the report is renamed or
# removed, and tells a future reader nothing they can act on that the
# invariant itself doesn't already say.
#
# Scans every tracked *.go file (excluding third_party/, which is vendored
# and not ours to rewrite) for a comment line containing, case-insensitively:
# "audit finding", "security audit", "finding <digit>", or "was assumed to".
# A hit only counts when it is actually inside a comment: the trimmed line
# starts with //, *, or /*, or the line has // somewhere before the match —
# a match inside a string literal or identifier is not flagged.
#
# Exits 0 with no output when clean, 1 with a file:line list otherwise.
#
# bash 3.2 compatible (macOS's default /bin/bash) — no mapfile, no
# associative arrays.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

files=()
while IFS= read -r f; do
	case "$f" in
	third_party/*) continue ;;
	esac
	files+=("$f")
done < <(git ls-files '*.go')

if [ ${#files[@]} -eq 0 ]; then
	echo "hack/lint-comment-style.sh: no tracked *.go files found (outside third_party/)" >&2
	exit 0
fi

if ! awk '
BEGIN {
	patterns[1] = "audit finding"
	patterns[2] = "security audit"
	patterns[3] = "finding [0-9]"
	patterns[4] = "was assumed to"
	found = 0
}
{
	line = $0
	lower = tolower(line)
	trimmed = line
	sub(/^[ \t]+/, "", trimmed)
	is_comment_start = (trimmed ~ /^(\/\/|\*|\/\*)/)

	hit = 0
	for (i = 1; i <= 4; i++) {
		if (match(lower, patterns[i])) {
			pos = RSTART
			prefix = substr(lower, 1, pos - 1)
			if (is_comment_start || prefix ~ /\/\//) {
				hit = 1
			}
		}
	}
	if (hit) {
		printf "%s:%d: %s\n", FILENAME, FNR, line
		found = 1
	}
}
END {
	if (found) exit 1
}
' "${files[@]}"; then
	echo "hack/lint-comment-style.sh: found audit-storytelling comment(s) above — state the invariant, not the finding number" >&2
	exit 1
fi

exit 0
