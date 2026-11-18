#!/usr/bin/env bash
# Renders the chart with the defaults and with typed-values.yaml and validates
# every object against the Kubernetes schemas, so a value the API would refuse
# (a boolean where a string is required) fails here and not at install.
set -euo pipefail
cd "$(dirname "$0")/../.."

chart=charts/triage-agent
helm lint "$chart"
for values in "" hack/chart-test/typed-values.yaml; do
  echo "== render ${values:-defaults}"
  helm template chart-test "$chart" ${values:+-f "$values"} \
    | kubeconform -strict -summary -ignore-missing-schemas -kubernetes-version 1.30.0
done
