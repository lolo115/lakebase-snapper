# lakebase_snapper — Databricks App

An always-on version of the `lakebase_snapper` CLI, packaged as a **Databricks App**. It
schedules ASH samples + cumulative snapshots against one or more Lakebase endpoints,
persists everything in a dedicated `lakebase_snapper` Postgres database, and serves a Plotly
dashboard.

- **App name:** `lakebase-snapper` (its URL is `<APP_URL>` — see DEPLOY.md to obtain it)
- **Workspace / profile:** your Databricks workspace + CLI profile (`<PROFILE>`)
- **Repository DB:** `lakebase_snapper` (schema `snap`) on your Lakebase project

> This is a template. Every environment-specific value appears as a `<PLACEHOLDER>`;
> **DEPLOY.md** lists each one with the exact CLI/REST command to obtain it.

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
| `INCLUDE_IDLE_IN_TXN` | Also sample `idle in transaction` sessions |

Targets can also be managed at runtime: `GET/POST /api/targets`,
`POST /api/targets/{id}/enabled`.

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

- **Keeps the endpoint awake:** the collector polls every few seconds, so the monitored
  endpoint won't scale to zero while the app runs. Expected for a monitoring app.
- **ASH needs activity:** an idle target yields no ASH rows (only *active* sessions are
  recorded); snapshots still accrue from cumulative counters.
- **SDK pin matters:** `w.postgres.generate_database_credential` requires
  `databricks-sdk>=0.132`; the App base image ships an older SDK, so the pin is required.
