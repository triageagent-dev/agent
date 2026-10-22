#!/bin/sh
# Install the Triage agent on a Linux host and join it to the core with a
# one-time agent key (core ADR-090). The core returns this command, filled in,
# when a host key is minted:
#
#   curl -fsSL <release>/install.sh | sudo sh -s -- \
#     --core <core-host> --ca SHA256:<fp> --tenant <tenant-id> \
#     --name <cluster-name> --key tak_<id>_<secret>
#
# It installs the deb or rpm of this release, writes /etc/triage-agent/env,
# puts the key in the join state directory (mode 0600) and starts the
# service. The agent removes the key once the core has issued its
# certificate, so a copy left in shell history cannot join anything else.
set -eu

# Stamped by the release job; a script run from a checkout passes --version.
VERSION="@VERSION@"
REPO="https://github.com/triageagent-dev/agent/releases/download"

CORE="" CA="" TENANT="" NAME="" KEY="" COLLECTORS="--log-metrics-enabled --use-metrics-enabled"

die() { echo "install.sh: $*" >&2; exit 1; }

while [ $# -gt 0 ]; do
	case "$1" in
	--core) CORE="$2"; shift 2 ;;
	--ca) CA="$2"; shift 2 ;;
	--tenant) TENANT="$2"; shift 2 ;;
	--name) NAME="$2"; shift 2 ;;
	--key) KEY="$2"; shift 2 ;;
	--version) VERSION="$2"; shift 2 ;;
	--collectors) COLLECTORS="$2"; shift 2 ;;
	*) die "unknown argument $1" ;;
	esac
done

[ -n "$CORE" ] || die "--core is required"
[ -n "$CA" ] || die "--ca is required"
[ -n "$TENANT" ] || die "--tenant is required"
[ -n "$NAME" ] || die "--name is required (the cluster name the key was minted for)"
[ -n "$KEY" ] || die "--key is required"
case "$VERSION" in @*@ | "") die "--version is required when the script is not from a release" ;; esac
case "$CA" in SHA256:*) ;; *) die "--ca must be SHA256:<hex>" ;; esac
case "$KEY" in tak_*_*) ;; *) die "--key must be an agent key (tak_...)" ;; esac
[ "$(id -u)" -eq 0 ] || die "run as root (sudo sh -s -- ...)"
command -v systemctl >/dev/null 2>&1 || die "systemd is required"

case "$(uname -m)" in
x86_64 | amd64) DEB_ARCH=amd64 RPM_ARCH=x86_64 ;;
aarch64 | arm64) DEB_ARCH=arm64 RPM_ARCH=aarch64 ;;
*) die "unsupported architecture $(uname -m)" ;;
esac

TAG="v${VERSION#v}"
PKGVER="${VERSION#v}"
if command -v dpkg >/dev/null 2>&1; then
	PKG="triage-agent_${PKGVER}_${DEB_ARCH}.deb"
elif command -v rpm >/dev/null 2>&1; then
	PKG="triage-agent-${PKGVER}-1.${RPM_ARCH}.rpm"
else
	die "neither dpkg nor rpm found"
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
echo "Downloading ${PKG} (${TAG})"
curl -fsSL -o "$TMP/$PKG" "$REPO/$TAG/$PKG"
curl -fsSL -o "$TMP/sums" "$REPO/$TAG/triage-agent_linux_sha256sums.txt"
WANT="$(awk -v f="$PKG" '$2 == f || $2 == "*"f { print $1 }' "$TMP/sums")"
[ -n "$WANT" ] || die "no checksum for $PKG in the release"
if command -v sha256sum >/dev/null 2>&1; then
	GOT="$(sha256sum "$TMP/$PKG" | awk '{print $1}')"
else
	GOT="$(shasum -a 256 "$TMP/$PKG" | awk '{print $1}')"
fi
[ "$WANT" = "$GOT" ] || die "checksum mismatch for $PKG"

case "$PKG" in
*.deb) dpkg -i "$TMP/$PKG" ;;
*.rpm) rpm -U --replacepkgs "$TMP/$PKG" ;;
esac

umask 077
ENV=/etc/triage-agent/env
[ -f "$ENV" ] && cp -p "$ENV" "$ENV.bak"
cat >"$ENV" <<EOF
# Written by install.sh. See /usr/share/doc or the agent README for every
# setting; after an edit run: systemctl restart triage-agent
TA_ENDPOINT=--triage-endpoint=wss://${CORE}/events/ws
TA_TENANT=--target-tenant=${TENANT}
TA_JOIN_CA_FINGERPRINT=--join-ca-fingerprint=${CA}
TA_JOIN_STATE_DIR=--join-state-dir=/var/lib/triage-agent/join
TA_CLUSTER_NAME=--cluster-name=${NAME}
TA_COLLECTORS=${COLLECTORS}
EOF
chmod 0600 "$ENV"

mkdir -p /var/lib/triage-agent/join
chmod 0700 /var/lib/triage-agent/join
printf '%s\n' "$KEY" >/var/lib/triage-agent/join/enroll.key
chmod 0600 /var/lib/triage-agent/join/enroll.key

systemctl daemon-reload
systemctl enable triage-agent >/dev/null
systemctl restart triage-agent
echo "triage-agent installed and started as ${NAME}; it joins the core with its one-time key."
echo "Follow it with: journalctl -u triage-agent -f"
