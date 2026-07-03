# Autoscaler — Runbook

## Deploy / restart

```bash
cd <repo-root>
docker compose up -d --build
```

`--build` is cheap (Alpine base + Go static binary); always rebuild on
deploy. The compose stack:

- Binds the listen port to `127.0.0.1:8088` on the host.
- Mounts `config.json` read-only into `/etc/autoscaler/config.json`.
- Mounts the Docker socket so the autoscaler can `docker run` runner
  containers.
- Sets `restart: unless-stopped`.

## Health checks

| Check | Expected |
|---|---|
| `curl -s http://127.0.0.1:8088/healthz` | `ok` |
| `curl -s http://127.0.0.1:8088/stats | jq` | non-zero `total_spawned` after first webhook |
| `docker ps --filter label=subzero-autoscaler=true` | one row per live ephemeral |
| GitHub repo → Settings → Actions → Runners | `auto-*` runners in `Idle` / `Active` |

## First-time setup

1. Create a `https://smee.io/<token>` URL.
2. Configure each repo's webhook: payload URL = the smee URL,
   content-type = `application/json`, events = **Workflow jobs**.
3. Create a GitHub PAT with `repo` scope (classic) or fine-grained
   permissions: Actions:read+write, Administration:read+write (for
   runner registration).
4. Copy `config.example.json` → `config.json`; fill smee URL, PAT,
   per-repo blocks. `chmod 600 config.json`.
5. `docker compose up -d --build`.

## Common ops

### Pause / resume spawning
Useful before a deploy of the host or during incidents.

```bash
curl -X POST http://127.0.0.1:8088/api/pause
curl -X POST http://127.0.0.1:8088/api/resume
```

Already-running ephemerals continue; only new spawn requests are
short-circuited with `ErrPaused`.

### Trigger a reconcile pass
If a webhook delivery was missed and you don't want to wait 30s:

```bash
curl -X POST http://127.0.0.1:8088/api/reconcile
# or for one repo:
curl -X POST "http://127.0.0.1:8088/api/reconcile?repo=yourorg/frontend"
```

### Trigger a cleanup pass (drain ghosts now)
Synchronous; returns the summary.

```bash
curl -X POST http://127.0.0.1:8088/api/cleanup | jq
```

### Stop a runaway ephemeral
```bash
curl -X POST "http://127.0.0.1:8088/api/stop?name=auto-mac-docker-backend-abcd"
```

### Read live state
```bash
curl -s http://127.0.0.1:8088/api/state | jq
```

## Troubleshooting matrix

| Symptom | Likely cause | Fix |
|---|---|---|
| `concurrency cap hit` repeated in logs | One repo's jobs back-to-back; cap too low | Raise `max_concurrency` for that repo in `config.json`, restart |
| `idle check failed; spawning anyway` | GitHub API rate-limit or 5xx | Usually transient; check `gh api rate_limit` |
| Stats `total_errors` ticking up | `docker run` failing — bad image, no socket, OOM | `docker logs subzero-runner-autoscaler` — stderr captured in error message |
| Ghost runners not draining | Ephemeral container died mid-job; GitHub keeps registration ~30 m | Cleanup loop will `DELETE` after heartbeat timeout. Force: `POST /api/cleanup` |
| `--rm` containers stuck in `Removing` (macOS Docker Desktop) | Known Docker Desktop bug with deep mount stacks | Cleanup reaps them on next tick; manual `docker rm -f <id>` works |
| `smee connection lost` in logs, no spawns | smee.io down, or transient network | Auto-reconnects with 1–30s backoff; check https://status.smee.io |
| `bad signature` rejections | `verify_hmac: true` with smee.io | Smee re-serializes the body, breaks HMAC. Set `verify_hmac: false` |
| Spawned runner can't see the repo | PAT lacks Actions/Administration write | Rotate PAT with correct scopes |

## Rotating the GitHub PAT

1. Mint a new PAT on GitHub.
2. Edit `config.json` → `github_pat`.
3. `docker compose restart autoscaler`.
4. Verify with `curl -s :8088/stats` (no immediate spike in
   `total_errors`).
5. Revoke the old PAT.

In-flight ephemeral runners are NOT affected (they already
exchanged the PAT for an action runner registration token).

## Rotating the smee URL

1. Create a new smee URL.
2. Update each repo's webhook to point to it (GitHub UI or `gh api`).
3. Update `config.json` → `smee_url`.
4. `docker compose restart autoscaler`.

## Rotating the portal token

Already 127.0.0.1-bound, so primarily a defense-in-depth knob. Set
`portal_token` in `config.json`, restart. Operators must then send
`Authorization: Bearer <token>` or set the `portal_token` cookie when
hitting `/api/*`.

## Logs

JSON to stdout. Tail:

```bash
docker logs -f subzero-runner-autoscaler
docker logs subzero-runner-autoscaler 2>&1 | jq -c 'select(.level=="ERROR")'
```

Useful filters:

- All spawns: `jq -c 'select(.msg=="runner spawned")'`
- Cap hits: `jq -c 'select(.msg=="concurrency cap hit")'`
- Cleanup summaries: `jq -c 'select(.msg=="cleanup tick complete")'`

## Rollback

> Note: this section describes the rollback procedure from the
> author's original deployment, where a previous static-pool stack was
> preserved as `docker-compose.yml.deprecated` + `register.sh.deprecated`
> in the parent directory. Those files are artifacts of that specific
> setup and are **not** included in this repo — adapt the procedure to
> however you manage your own previous deployment.

```bash
docker compose -f <repo-root>/docker-compose.yml down
cd <repo-root>/..
mv docker-compose.yml.deprecated docker-compose.yml
mv register.sh.deprecated register.sh
./register.sh
```

Be aware both stacks fight if run concurrently — never have both up at
once.

## Capacity planning

- Each ephemeral runner is one `myoung34/github-runner` container plus
  whatever the workflow steps spawn. Memory headroom on the host
  should be ~1.5× peak `Σ(max_concurrency)` × typical job RSS.
- The autoscaler itself is < 50 MB RSS.
- GitHub API budget: ~5000 req/h per PAT. Per cleanup tick the cost
  is `O(repos × (1 + queued_runs + ghosts))`. At 5 repos / 5-minute
  ticks this is well under budget.

## Upgrading

The autoscaler is a single Go binary inside a container image:

```bash
cd <repo-root>
git pull   # if you've put this under git
docker compose up -d --build
```

The reconciler will pick up any queued jobs missed during the brief
restart. No state is lost; the only "state" is the labels on live
containers, which survive the restart untouched.
