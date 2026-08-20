# lakebase_snapper — Databricks App

An always-on version of the `lakebase_snapper` CLI, packaged as a **Databricks App**. It
schedules ASH samples + cumulative snapshots against one or more Lakebase endpoints,
persists everything in a dedicated `lakebase_snapper` Postgres database, and serves a Plotly
dashboard.

- **App name:** `lakebase-snapper` (its URL is `<APP_URL>` — see DEPLOY.md to obtain it)
- **Workspace / profile:** your Databricks workspace + CLI profile (`<PROFILE>`)
- **Repository DB:** `lakebase_snapper` (schema `snap`) on your Lakebase project

> This is a template. Every environment-specific value appears as a `<PLACEHOLDER>`.
> **Recommended:** from the repo root run `./configure.sh -p <OAUTH_PROFILE>` — it discovers your settings via
> the REST API, fills the placeholders in `app.yaml`, and can deploy for you. See
> [DEPLOY.md](DEPLOY.md); it also documents how to obtain each value by hand.

## Architecture

```
FastAPI (app.py)
 ├─ lifespan: open repo pool → init schema → seed targets → start scheduler
 ├─ scheduler.py (APScheduler, in-process)
 │    ├─ ash tick      every SAMPLE_INTERVAL_SECS  (default 2s)  → snap.ash_samples
 │    ├─ snapshot tick every SNAPSHOT_INTERVAL_SECS (default 300s) → snap.snapshots + snap.snap_pgss
 │    └─ purge tick    every 6h                     → delete rows older than RETENTION_DAYS
 ├─ core.py : repo ConnectionPool (OAuth-per-connection) + per-target auto-refreshing conns
 ├─ repo.py : schema DDL, writers, and all chart queries
 └─ static/index.html : single-page Plotly dashboard (target dropdown, time window, auto-refresh)
```

Collection SQL is the same as the CLI: ASH from `pg_stat_activity` (state='active',
`statement_timestamp()` groups a poll), snapshots from `pg_stat_statements` + system stats.

## Dashboard

The single page (auto-refreshing, with a target dropdown and a time-window selector that
includes a custom "last N minutes") shows:

- **Average Active Sessions by wait class**: stacked AAS over time, with the configured
  **CU ceiling** drawn as a red step line (it steps when the endpoint's CU changes) and a
  **"CPU used (from ASH)"** overlay (average sessions on CPU, a proxy for used vCPU, since
  the platform's live vCPU metric is not exposed to the app).
- **Wait class mix (active)** and **Wait class mix (incl. idle-in-txn)**: two donuts. The
  active one is CPU + wait classes for `state='active'`; the second adds an **Idle in txn**
  slice (sessions holding a transaction open, which hold locks/snapshots). Benign plain-idle
  pool connections are not shown.
- **Top SQL by CPU & wait time**: per statement, active time split into CPU + each wait
  class (estimated as sample_count x sample-interval). Each bar shows the source
  **database**; system/maintenance databases (`postgres`, `template*`) are flagged in amber
  with a gear marker. The app's own collector/repo queries are excluded (the
  `pg_stat_statements` analogue of the ASH `application_name` exclusion), so it reflects
  real workload.
- **Load profile**: transactions/s, **buffer cache hit %**, and **LFC hit %** (Neon Local
  File Cache) on a second axis.
- **Latest snapshot diff**: top SQL by execution-time delta between the first and last
  snapshot in the **selected time window**, with a **db** column (maintenance DBs flagged
  the same way). Own queries are excluded here too.

Header controls: **Snapshot now** (force an immediate snapshot of every enabled target) and
**Start / Stop auto collection** (see Notes for scale-to-zero).

## Files

| File | Purpose |
|------|---------|
| `app.py` | FastAPI app: lifespan, JSON API, `/api/diag`, static serving |
| `core.py` | Auth + connections (repo pool, target conns), collection SQL |
| `repo.py` | Repository schema + write helpers + chart queries |
| `scheduler.py` | APScheduler jobs (sample / snapshot / purge) |
| `static/index.html` | Plotly dashboard |
| `app.yaml` | App command + env (targets, cadence, PG connection) |
| `requirements.txt` | Deps — **`databricks-sdk>=0.132`** (older SDKs lack `w.postgres`) |
| `../configure.sh` | (repo root) REST-driven setup — fills `app.yaml` placeholders via OAuth and can deploy |
| `run_local.sh` | Run locally against Lakebase with your CLI profile |
| `loadgen.py` | (in `../`) load generator for producing observable activity |

## Configuration (`app.yaml` env)

| Var | Meaning |
|-----|---------|
| `REPO_ENDPOINT` | Lakebase endpoint path hosting the `lakebase_snapper` repo DB |
| `PGHOST` / `PGPORT` / `PGSSLMODE` | Repo connection host/port/ssl |
| `PGUSER` | **App service-principal client-id** (Postgres role name) |
| `REPO_DBNAME` / `REPO_SCHEMA` | `lakebase_snapper` / `snap` |
| `SEED_TARGETS` | JSON array of monitored targets `[{label, endpoint, dbname}]` |
| `SAMPLE_INTERVAL_SECS` / `SNAPSHOT_INTERVAL_SECS` / `RETENTION_DAYS` | Cadence + retention |
| `INCLUDE_IDLE_IN_TXN` | Sample idle-in-transaction sessions (Idle-in-txn pie slice); default on |
| `INCLUDE_IDLE` | Sample benign plain-idle sessions; default off (not shown in the UI) |
| `INSTANCE_CONNECT_DB` | DB the instance-wide `*` target connects through (default `postgres`) |

Runtime API: `GET/POST /api/targets`, `POST /api/targets/{id}/enabled`,
`POST /api/snapshot-now` (force a snapshot), `POST /api/collection` (`{"enabled": true|false}`
to start/stop collection), plus the chart endpoints (`/api/summary`, `/api/ash-timeline`,
`/api/waits` [`?include_idle=1`], `/api/top-sql`, `/api/load-profile`, `/api/cu-current`,
`/api/cu-timeline`, `/api/snap-diff`, `/api/status`, `/api/diag`).

## One-time service-principal wiring

The app runs as an auto-created service principal (client-id
`<SERVICE-PRINCIPAL-CLIENT-ID>`). Autoscaling Lakebase projects **cannot** be
attached as an app "database resource", so the app mints tokens in-process and the SP must
be granted access two ways:

1. **Platform ACL** — let the SP mint DB credentials for the project:
   ```bash
   databricks api patch /api/2.0/permissions/database-projects/<LAKEBASE_PROJECT> -p <PROFILE> \
     --json '{"access_control_list":[{"service_principal_name":"<SERVICE-PRINCIPAL-CLIENT-ID>","permission_level":"CAN_USE"}]}'
   ```
2. **Postgres role + grants** — connect as an admin and:
   ```sql
   CREATE EXTENSION IF NOT EXISTS databricks_auth;
   SELECT databricks_create_role('<SERVICE-PRINCIPAL-CLIENT-ID>','SERVICE_PRINCIPAL');
   GRANT pg_monitor TO "<SERVICE-PRINCIPAL-CLIENT-ID>";              -- see other sessions in pg_stat_activity/_statements
   GRANT CONNECT ON DATABASE lakebase_snapper TO "<SERVICE-PRINCIPAL-CLIENT-ID>";
   -- in lakebase_snapper:
   GRANT USAGE, CREATE ON SCHEMA snap TO "<SERVICE-PRINCIPAL-CLIENT-ID>";
   GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA snap TO "<SERVICE-PRINCIPAL-CLIENT-ID>";
   GRANT USAGE,SELECT,UPDATE ON ALL SEQUENCES IN SCHEMA snap TO "<SERVICE-PRINCIPAL-CLIENT-ID>";
   ALTER DEFAULT PRIVILEGES IN SCHEMA snap GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO "<SERVICE-PRINCIPAL-CLIENT-ID>";
   ALTER DEFAULT PRIVILEGES IN SCHEMA snap GRANT USAGE,SELECT,UPDATE ON SEQUENCES TO "<SERVICE-PRINCIPAL-CLIENT-ID>";
   ```
   To monitor a target on a **different** instance, repeat the role + `pg_monitor` grant there.

> **LFC hit ratio (optional).** The load profile's *LFC hit %* reads Neon's `neon_lfc_stats`.
> Enable it once per monitored database:
> `CREATE EXTENSION IF NOT EXISTS neon; GRANT SELECT ON neon_lfc_stats TO "<SERVICE-PRINCIPAL-CLIENT-ID>";`
> Without it the app still runs; only the LFC line stays empty for that database. (The
> instance-wide `*` target reads through `INSTANCE_CONNECT_DB`, whose `neon` view may be
> absent, so LFC can be blank there; it is endpoint-wide and visible on per-database targets.)

## Run locally

```bash
cd ~/lakebase_snapper/app
./run_local.sh          # uses profile <PROFILE>, your identity; serves on :8000
# open http://localhost:8000
```

## Deploy / redeploy

```bash
cd ~/lakebase_snapper/app
EMAIL=$(databricks current-user me -p <PROFILE> -o json | jq -r '.userName')
databricks sync . "/Workspace/Users/$EMAIL/lakebase-snapper" \
  --exclude .venv --exclude __pycache__ --exclude .git --full -p <PROFILE>
databricks apps deploy lakebase-snapper --source-code-path "/Workspace/Users/$EMAIL/lakebase-snapper" -p <PROFILE>
```

## Logs & health

- Logs need an **OAuth (U2M)** profile (PAT profiles get "OAuth Token not supported"):
  ```bash
  databricks auth login --host https://<WORKSPACE-HOST> --profile <PROFILE>-oauth
  databricks apps logs lakebase-snapper -p <PROFILE>-oauth
  ```
- `GET /api/diag` — live self-test: repo connectivity, per-target connectivity + `pg_monitor`
  visibility, and effective env. First stop when charts are empty.
- `GET /healthz` — always 200 (readiness probe).

## Notes

- **Keeps the endpoint awake / scale to zero:** while collecting, the app polls every few
  seconds, so the monitored endpoint won't scale to zero. Use **Stop auto collection** in the
  header to pause sampling and drop the target connections; once collection is stopped and
  the dashboard is idle (set Auto-refresh to off), the repo pool releases too and the
  endpoint can scale to zero.
- **ASH needs activity:** an idle target yields no *active* ASH rows; snapshots still accrue
  from cumulative counters. Idle-in-transaction sessions are captured (for the incl-idle-in-txn
  pie); benign plain-idle connections are not captured by default (`INCLUDE_IDLE`).
- **Real workload only:** Top SQL and the snapshot diff exclude the app's own collector/repo
  queries. Statements in system/maintenance databases (`postgres`, `template*`), e.g. Lakebase
  platform monitoring, are still shown but flagged in amber (gear icon) since they are not
  your application workload. The instance-wide `*` target connects through `INSTANCE_CONNECT_DB`
  (default `postgres`), which is why some maintenance traffic surfaces there.
- **SDK pin matters:** `w.postgres.generate_database_credential` requires
  `databricks-sdk>=0.132`; the App base image ships an older SDK, so the pin is required.
