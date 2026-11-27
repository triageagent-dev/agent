// promevents runs as its own small Deployment, so it is its own module:
// building it does not build the triage-agent it lives beside.
module github.com/triageagent-dev/agent/promevents

go 1.27.3

// CVE-2026-98432: a listener never asks the kernel for Multipath TCP.
// Go 1.24+ enables MPTCP by default on Linux; the kernel bug it exposes
// is fixed only in a patched kernel, and no client of ours needs MPTCP.
godebug multipathtcp=0
