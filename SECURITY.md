# Security Policy

## Reporting a vulnerability

Please **do not** open a public issue for a security problem.

Report it through GitHub's private vulnerability reporting on this repository
(Security → Report a vulnerability), which opens a channel visible only to the
maintainers.

Include, as far as you can establish them: affected version or commit, the
component (controller, Helm chart, push mode, or the `gcpevents` function), what
an attacker gains, and the smallest reproduction you have. A partial report sent
early is more useful than a complete one sent late.

You can expect an acknowledgement within a week. If a fix is warranted we will
agree a disclosure timeline with you before publishing.

## Supported versions

Only the latest release on `main` receives security fixes. Older tags are not
patched.

## Scope notes

Two areas are worth knowing about before reporting:

- **Push mode** (`push/`) makes an *outbound* WebSocket connection and requires an
  explicit `--triage-endpoint`; it is inert unless configured. It authenticates
  to the core only with its join certificate (`--join-ca-fingerprint`).
- **`gcpevents/`** is a separate Go module deployed as a Cloud Run function. It
  reads all credentials from the environment and Google Application Default
  Credentials — it must never carry embedded secrets, and a report showing
  otherwise is in scope.

Findings in the deployment *configuration* of a particular installation (cluster
RBAC, chart values, IAM bindings chosen by an operator) belong to that installation
rather than here, unless the chart's own defaults are what make it unsafe.
