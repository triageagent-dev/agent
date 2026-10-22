// promevents runs as its own small Deployment, so it is its own module:
// building it does not build the triage-agent it lives beside.
module github.com/triageagent-dev/agent/promevents

go 1.27.3
