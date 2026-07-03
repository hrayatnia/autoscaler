# autoscaler

[![CI](https://github.com/hrayatnia/autoscaler/actions/workflows/ci.yml/badge.svg)](https://github.com/hrayatnia/autoscaler/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.22%2B-00ADD8?logo=go)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

`autoscaler` is a single-binary Go service that spawns ephemeral,
self-hosted GitHub Actions runner containers on demand, triggered by
GitHub webhook events. It replaces a static, always-on pool of runner
containers with runners that are created only when a job is queued and
destroyed as soon as that job finishes.

## Why

A static pool of self-hosted runners has two recurring costs:

- **Idle cost.** Runner containers sit around consuming RAM/CPU even
  when no workflow is running, and every repo pays that cost whether
  or not it's actively building.
- **Manual concurrency pain.** Sizing the pool means guessing at
  per-repo burst concurrency ahead of time, then re-tuning it by hand
  as workloads change — too few runners and jobs queue, too many and
  you're back to paying idle cost.

Scaling runners up from zero on webhook events and back down to zero
when there's no work removes both problems.

## Features

- **Scale-to-zero** — runners are spawned per `workflow_job` event and
  exit after exactly one job (`myoung34/github-runner` with
  `EPHEMERAL=true` plus `docker run --rm`); no runner sits idle waiting
  for work.
- **Per-repo concurrency caps** — each configured repo has its own
  `max_concurrency`, so a burst of jobs in one repo can't starve
  another.
- **Ghost-runner reaping** — a periodic cleanup loop finds offline
  GitHub runner registrations and dead/orphaned local containers and
  removes both, without operator intervention.
- **Reconcile loop** — a periodic catch-up pass polls for queued jobs
  that a missed or delayed webhook didn't trigger a spawn for.
- **Operator portal** — an embedded web UI and JSON API
  (`/api/state`, `/api/reconcile`, `/api/cleanup`, `/api/pause`,
  `/api/resume`, `/api/stop`) for inspecting live state and driving
  ops actions by hand.
- **Security hardening**:
  - The GitHub PAT is written to a `0600` temp env-file passed to
    `docker run --env-file`, then immediately unlinked — it's never
    on process argv or in logs.
  - A per-repo `sync.Mutex` closes the TOCTOU window where two webhook
    events could both pass the concurrency-cap check.
  - Config file permissions are checked at startup and a warning is
    logged if the file is world-readable.
  - The portal supports optional bearer-token / cookie auth with a
    constant-time compare, and binds to `127.0.0.1` by default in the
    provided compose file.

## Prerequisites

- Go 1.22+ (see `go.mod`) if you want to build from source; the
  provided Docker image builds this for you.
- Docker (and Docker Compose) on the host that will run both the
  autoscaler and the ephemeral runner containers.
- A [smee.io](https://smee.io) URL — used to relay GitHub webhooks to
  a host with no public inbound ingress. Create one at
  `https://smee.io/new`.
- A GitHub personal access token with permission to manage self-hosted
  runners and workflow runs for the target repos (classic PAT with
  `repo` scope, or fine-grained `Actions: read+write` +
  `Administration: read+write`).

## Quick start

```bash
cp config.example.json config.json
$EDITOR config.json   # fill smee_url, github_pat, and your repos block
chmod 600 config.json
docker compose up -d --build
curl -s http://127.0.0.1:8088/healthz   # → ok
```

For each repo you list in `config.json`, configure its GitHub webhook
with the smee URL as the payload URL, content type
`application/json`, and the **Workflow jobs** event.

## Architecture

The autoscaler receives `workflow_job` events over an outbound SSE
connection to smee.io (no inbound ingress required), spawns an
ephemeral runner container per queued job up to the per-repo
concurrency cap, and runs two background loops alongside it: a
reconciler that polls for jobs a missed webhook didn't catch, and a
cleanup loop that reaps ghost runner registrations and orphaned
containers. An embedded HTTP portal exposes live state and manual
controls.

See [`docs/ARCH.md`](./docs/ARCH.md) for the full component
breakdown, data flow, and security model, and
[`docs/RUNBOOK.md`](./docs/RUNBOOK.md) for deployment, day-2
operations, and troubleshooting.

## Development

```bash
gofmt -l .          # must be empty
go vet ./...
go build ./...
go test -race ./...
```

[`.github/workflows/ci.yml`](./.github/workflows/ci.yml) runs the same
four checks on every push to `main` and every pull request.

## Status

This is a personal proof-of-concept, built for a specific self-hosted
runner setup and shared as-is. It is not officially supported —
there's no SLA, and issues/PRs may or may not get attention. Use it,
fork it, adapt it; just don't expect maintenance.

## License

MIT — see [`LICENSE`](./LICENSE).
