# Autoscaler — Architecture

`github.com/hrayatnia/autoscaler` is a single-binary Go service that
spawns ephemeral self-hosted GitHub Actions runner containers on demand.
It runs co-resident with the Docker daemon on the host that will execute
the jobs (typically a Mac with Docker Desktop), and replaces a previous
static-pool docker-compose stack of always-on runner containers.

## Goals

1. **No idle cost** — runners exist only while there is work for them.
2. **Per-repo concurrency caps** — bursty workflows in one repo never
   exhaust capacity for another.
3. **Self-healing** — ghost runners on GitHub (registrations whose host
   container is dead) get reaped without operator intervention.
4. **No secret leaks** — the GitHub PAT and smee URL never appear in
   process argv, logs, or world-readable files.

## Component diagram

```
                    ┌────────────────────────────────────────────────┐
                    │                  autoscaler                    │
                    │                                                │
   GitHub ──webhook── smee.io ──SSE──▶ smee.Client                   │
                    │                     │                          │
                    │                     ▼                          │
                    │            webhook.Handler ──┐                 │
                    │                              │ ctx, repo       │
                    │   ┌──────tick (30s)───────┐  ▼                 │
   GitHub API ◀───────── reconciler.Reconciler ───▶ spawner.Spawner ───▶ docker run
                    │                              ▲                 │
                    │   ┌──────tick (5m)────────┐  │ shared pat,     │
   GitHub API ◀───────── cleanup.Cleaner ───────┘  │ http client     │
                    │                              │                 │
                    │            portal.Server ────┘                 │
                    │                ▲                               │
                    │                │ /api/state, /api/pause, …     │
                    │            (HTTP 127.0.0.1:8088)               │
                    └────────────────────────────────────────────────┘
```

## Component responsibilities

### `cmd/autoscaler` (entrypoint)
- Loads config; sets up structured (slog/JSON) logger.
- Builds the four singletons (spawner, webhook handler, cleaner,
  reconciler) sharing a single `Spawner` (which owns the PAT and the
  HTTP client).
- Wires a `signal.NotifyContext` root context. Every goroutine derives
  from it; SIGTERM cancels root, then `sync.WaitGroup.Wait` drains.
- Runs the HTTP server with `Read/Write/Idle/ReadHeader` timeouts and a
  bounded graceful-shutdown.

### `internal/spawner`
The trust boundary for the PAT.
- `Spawn(ctx, repo)` is the only public mutation. It is **serialised
  per-repo** by an internal `sync.Map[string]*sync.Mutex`, closing the
  TOCTOU window where two webhook events both pass the cap check.
- `idle-skip`: before spawning, polls GitHub for existing runners with
  matching labels; if any is online & not busy, skips (avoids over-
  provisioning when static-pool runners coexist).
- **PAT handling**: the token is written to a `0o600` temp env-file and
  passed to `docker run --env-file`, then immediately unlinked. The PAT
  is never on the argv. `Authorization: Bearer <pat>` headers are not
  logged.
- **macOS Docker corner-case**: on Docker Desktop, containers whose
  `--rm` hangs (deep mount stacks, OOM-during-exit) stay visible in
  `docker ps` with cached `Up …` status, even though `State.Status` is
  `dead`. `countRunning` runs an additional `docker inspect` per
  candidate and only counts those whose `.State.Status == running`.
- `DockerExecer` and `HTTPDoer` are interfaces: the production
  implementations shell to `docker` and `*http.Client`, tests inject
  fakes (see `spawner_concurrency_test.go`).

### `internal/webhook`
- HMAC verification via `crypto/hmac` + constant-time compare. Default
  off in smee.io mode (the smee URL itself is the secret, and smee
  re-serializes the JSON body, breaking byte-exact HMAC).
- Label matching is case-insensitive and uses any-of semantics. The
  `self-hosted` label that GitHub always adds is ignored implicitly
  because no repo configures it as a match label.

### `internal/smee`
- Implements SSE parsing for smee.io's stream (which emits implicit-
  `event: message` events with `id:` and `data:` lines).
- Reconnect loop with bounded exponential backoff (1s → 30s).
- Holds a single long-lived `*http.Client` with `Timeout=0` (the stream
  is long-poll). Injectable for tests.

### `internal/reconciler`
- Periodic catch-up loop. Webhook delivery is best-effort: smee can
  disconnect, the autoscaler can be down during a queued event, GitHub
  webhooks can fail silently. `queued` jobs in GitHub never re-fire a
  `queued` event, so we must poll.
- Each tick: list `queued` workflow_runs per repo; for each, list jobs,
  filter to ones matching our `match_labels`, spawn up to per-repo cap.
- Short-circuits once `available` matches are found, bounding API usage.

### `internal/cleanup`
- Periodic ghost-reaping loop.
- **Phase 1 — GitHub ghosts:** finds `auto-*` runners whose status is
  `offline`. Force-cancels their in_progress runs (starts GitHub's
  heartbeat timeout clock). Attempts `DELETE /actions/runners/{id}`;
  GitHub returns 422 ("currently running a job") until heartbeat
  timeout — expected, silently retried next tick.
- **Phase 2 — local orphans:** lists managed containers via
  `docker ps`, resolves `.State.Status` via `docker inspect`, and reaps
  any whose state ≠ running (corpse path, no grace) or whose name has
  no corresponding GitHub registration *and* whose age ≥ `localGrace`
  (default 5 m; the grace avoids racing freshly spawned runners that
  haven't completed their registration handshake).

### `internal/portal`
- Embedded HTML + JSON API for operator UI.
- Optional bearer-token auth (`Authorization: Bearer <token>` or
  `portal_token` cookie). Constant-time compare. Disabled when
  `portal_token` is empty (the PoC behavior); on by default no
  authentication is required because the listen socket binds to
  127.0.0.1 in the compose file (defense in depth).
- Endpoints:
  - `GET  /` — UI (embedded HTML).
  - `GET  /healthz` — liveness.
  - `GET  /stats` — lifetime counters.
  - `GET  /api/state` — full live snapshot.
  - `POST /api/reconcile?repo=<owner/name>` — trigger one reconcile pass.
  - `POST /api/cleanup` — trigger one cleanup pass (synchronous).
  - `POST /api/pause` / `POST /api/resume` — toggle spawn admission.
  - `POST /api/stop?name=auto-…` — stop one ephemeral container.

## Concurrency model

- **One** root context (`signal.NotifyContext`) cancels every loop.
- **One** `sync.WaitGroup` waits for the three top-level goroutines
  (reconciler, cleaner, HTTP server) to drain.
- **One** per-repo mutex (`sync.Map`) inside the spawner serializes the
  cap check + docker run.
- All `docker` exec and GitHub HTTP calls are ctx-bound; SIGTERM cancels
  them immediately.

## Security model

| Asset | Boundary | Hardening |
|---|---|---|
| GitHub PAT | Process memory + env-file fd | `0o600` env-file unlinked after `docker run`; never in argv or logs |
| smee URL | Process memory + JSON config | Config file perm warning at startup; only scheme+host logged |
| Webhook secret | Process memory + JSON config | Same; constant-time HMAC compare when enabled |
| Portal endpoints | TCP port 127.0.0.1:8088 | Optional bearer-token; loopback-only bind in docker-compose |
| Docker socket | `/var/run/docker.sock` | Mounted into autoscaler **and** runner containers (necessary for runner-in-container jobs); the autoscaler container is the trust boundary |

## Why smee.io?

The Mac host has no public ingress. smee.io provides an SSE long-poll
relay: GitHub webhooks go to a public smee URL; smee streams them to
the autoscaler's outbound SSE connection. The downside is that smee
re-serializes the JSON body, which breaks byte-exact HMAC validation —
hence `verify_hmac` defaults to false. We rely on the smee URL itself
being secret.

## Why ephemeral runners?

`myoung34/github-runner` with `EPHEMERAL=true` runs exactly one job
then exits. Combined with `docker run --rm`, this gives:
- Fresh filesystem per job (no test pollution between PRs).
- No update treadmill (the runner self-updates on registration, but
  there's only ever one job per process).
- Natural scale-to-zero (no idle cost).

The trade-off: a small (~10–30s) cold-start per job for image pull and
runner registration handshake. Idle-skip mitigates this when there
*is* a coincidentally idle matching runner.

## Design alternatives considered

- **Direct GitHub webhook (no smee):** would require public ingress
  to the Mac. Not currently feasible.
- **Static pool of long-lived runners:** what the deprecated
  `docker-compose.yml.deprecated` did. Idle RAM cost; manual
  concurrency tuning.
- **Pull-based polling only (no webhooks):** would work, but reconcile
  latency = polling interval. We get webhook-driven low latency *and*
  reconcile catch-up.
