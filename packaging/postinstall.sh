#!/bin/sh
# Runs after the package files are in place. Only tells systemd about the new
# unit; the service is neither enabled nor started, because it needs
# /etc/triage-agent/env edited first.
set -e
if [ -d /run/systemd/system ]; then
	systemctl daemon-reload >/dev/null 2>&1 || true
fi
