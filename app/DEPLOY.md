# Deploying & configuring lakebase-snapper

A step-by-step guide to deploy the **lakebase-snapper** app on Databricks and point it at
the specific database(s) you want to monitor.

> New here? Read [README.md](README.md) for the architecture. This doc is the operational
> how-to.

---

## 1. The mental model: repository vs. targets

lakebase-snapper deals with **two different kinds of database**, and keeping them straight
is the whole game:

| | **Repository DB** | **Target DB(s)** |
|---|---|---|
| What | Where the app **stores** snapshots + ASH samples | What the app **observes** |
| How many | Exactly one | One or more |
| Configured by | `REPO_*` env vars | `SEED_TARGETS` env (or the `/api/targets` API) |
| App needs | `CREATE`/DML on schema `snap` | `pg_monitor` (to see other sessions) |
| Example | `lakebase_snapper` DB on your Lakebase | the `postgres` DB you care about |

A **target** is just a pair: **`(endpoint, dbname)`** plus a friendly `label`. "Targeting a
specific database" = adding the right `(endpoint, dbname)` to `SEED_TARGETS`.

The repository and a target can live on the **same** Lakebase instance (simplest) or on
**different** instances. Each distinct Lakebase *instance* the app touches needs the app's
service principal registered as a Postgres role there.

---

## 2. Prerequisites

- Databricks CLI ≥ 0.290 and a workspace with Lakebase (autoscaling `databricks postgres`).
- A profile authenticated to the workspace (`databricks auth login ... --profile <p>`).
  - For **log access** you need an **OAuth (U2M)** profile — a PAT profile returns
    `OAuth Token not supported`. Create one: `databricks auth login --host <workspace-url> --profile <p>-oauth`.
- `psql` (`brew install postgresql@16`) and `jq`.
- Permission to create apps and to grant on the Lakebase project.

Worked example values used below (replace with your own):

```
PROFILE=<PROFILE>
PROJECT=<LAKEBASE_PROJECT>
REPO_ENDPOINT=projects/<LAKEBASE_PROJECT>/branches/production/endpoints/primary
REPO_DB=lakebase_snapper           # dedicated repository database
APP=lakebase-snapper
```

### Configuration values & how to obtain them

Every `<PLACEHOLDER>` in `app.yaml`, `run_local.sh`, and the commands below resolves to one
of these. Each has a one-liner to fetch it (CLI shown; REST equivalent noted where handy).

| Placeholder | What it is | How to obtain |
|---|---|---|
| `<PROFILE>` | Your Databricks CLI profile | `databricks auth profiles` — or the name you passed to `databricks auth login --profile ...` |
| `<WORKSPACE-HOST>` | Workspace hostname (for OAuth login) | `databricks auth env -p <PROFILE>` → the `DATABRICKS_HOST` value (drop `https://`) |
| `<LAKEBASE_PROJECT>` | Autoscaling Lakebase project id | `databricks postgres list-projects -p <PROFILE> -o json \| jq -r '.[].name'` (use the part after `projects/`) |
| `<LAKEBASE_ENDPOINT>` | Full endpoint path `projects/<p>/branches/<b>/endpoints/<e>` | `databricks postgres list-endpoints projects/<LAKEBASE_PROJECT>/branches/production -p <PROFILE> -o json \| jq -r '.[].name'` |
| `<PGHOST>` | Endpoint hostname | `databricks postgres list-endpoints projects/<LAKEBASE_PROJECT>/branches/production -p <PROFILE> -o json \| jq -r '.[0].status.hosts.host'` |
| `<SERVICE-PRINCIPAL-CLIENT-ID>` | The app's service principal client-id (= its Postgres role name) | `databricks apps get <APP> -p <PROFILE> -o json \| jq -r '.service_principal_client_id'`  ·  REST: `GET /api/2.0/apps/<APP>` → `service_principal_client_id` |
| `<APP_URL>` | Deployed app URL | `databricks apps get <APP> -p <PROFILE> -o json \| jq -r '.url'` |
| `<DATABASE>` | A database to monitor on a target endpoint | connect with `psql` and run `\l`, or `SELECT datname FROM pg_database WHERE datistemplate = false;` |

> The service principal does not exist until you create the app (§3.1), so `<SERVICE-PRINCIPAL-CLIENT-ID>`
> is the one value you fill in *after* the first `databricks apps create`.

---

## 3. Deploy the app (from scratch)

### 3.1 Create the app and note its service principal

```bash
databricks apps create "$APP" --description "Lakebase performance sampler" -p "$PROFILE"

# The app runs as an auto-created service principal (SP). Grab its client-id:
SP=$(databricks apps get "$APP" -p "$PROFILE" -o json | jq -r '.service_principal_client_id')
echo "SP=$SP"
```

### 3.2 Let the SP mint database tokens (platform ACL)

Autoscaling Lakebase projects can't be attached as an app "database resource", so the app
mints OAuth tokens in-process. Grant the SP `CAN_USE` on the **project** (this PATCH is
additive — existing ACLs are preserved):

```bash
databricks api patch /api/2.0/permissions/database-projects/$PROJECT -p "$PROFILE" \
  --json "{\"access_control_list\":[{\"service_principal_name\":\"$SP\",\"permission_level\":\"CAN_USE\"}]}"
```

### 3.3 Create the repository database

```bash
HOST=$(databricks postgres list-endpoints "${REPO_ENDPOINT%/endpoints/*}" -p "$PROFILE" -o json | jq -r '.[0].status.hosts.host')
TOKEN=$(databricks postgres generate-database-credential "$REPO_ENDPOINT" -p "$PROFILE" -o json | jq -r '.token')
EMAIL=$(databricks current-user me -p "$PROFILE" -o json | jq -r '.userName')

PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=postgres user=$EMAIL sslmode=require" \
  -c "CREATE DATABASE $REPO_DB;"
PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=$REPO_DB user=$EMAIL sslmode=require" \
  -c "CREATE SCHEMA IF NOT EXISTS snap;"
```

### 3.4 Register the SP as a Postgres role and grant it

Run once per **Lakebase instance** the app touches. On the instance hosting the repository:

```bash
PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=postgres user=$EMAIL sslmode=require" <<SQL
CREATE EXTENSION IF NOT EXISTS databricks_auth;
SELECT databricks_create_role('$SP','SERVICE_PRINCIPAL');   -- role name IS the client-id
GRANT pg_monitor TO "$SP";                                  -- see all sessions on this instance
GRANT CONNECT ON DATABASE $REPO_DB TO "$SP";
SQL

# Give the SP ownership of the repository schema so it can create/maintain its own tables.
# (Ownership transfer requires you to be a member of the SP role first.)
PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=$REPO_DB user=$EMAIL sslmode=require" <<SQL
GRANT "$SP" TO "$EMAIL";
ALTER SCHEMA snap OWNER TO "$SP";
GRANT CONNECT ON DATABASE $REPO_DB TO "$SP";
SQL
```

> The app creates its tables on first start (it owns `snap`). If you migrated an existing
> `snap` schema, also `ALTER TABLE snap.<t> OWNER TO "$SP"` each table, or you'll see a
> harmless `must be owner of table` warning at startup.

### 3.5 Configure `app.yaml`

Set the repository connection + the SP as `PGUSER`, and seed at least one target. `PGHOST`
is the endpoint host from 3.3.

```yaml
env:
  - name: REPO_ENDPOINT
    value: 'projects/<LAKEBASE_PROJECT>/branches/production/endpoints/primary'
  - name: PGHOST
    value: '<PGHOST>'
  - name: PGPORT
    value: '5432'
  - name: PGSSLMODE
    value: 'require'
  - name: REPO_DBNAME
    value: 'lakebase_snapper'
  - name: REPO_SCHEMA
    value: 'snap'
  - name: PGUSER
    value: '<SERVICE-PRINCIPAL-CLIENT-ID>'          # from 3.1
  - name: SEED_TARGETS               # see §4
    value: '[{"label":"prod / appdb","endpoint":"projects/<LAKEBASE_PROJECT>/branches/production/endpoints/primary","dbname":"appdb"}]'
  - name: SAMPLE_INTERVAL_SECS
    value: '2'
  - name: SNAPSHOT_INTERVAL_SECS
    value: '300'
  - name: RETENTION_DAYS
    value: '7'
```

Pin `databricks-sdk>=0.132` in `requirements.txt` — the Apps base image ships an older SDK
without the `w.postgres` API, which otherwise fails token minting.

### 3.6 Sync and deploy

```bash
WS="/Workspace/Users/$EMAIL/$APP"
databricks sync . "$WS" --exclude .venv --exclude __pycache__ --exclude .git --exclude .databricks --full -p "$PROFILE"
databricks apps deploy "$APP" --source-code-path "$WS" -p "$PROFILE"
```

### 3.7 Verify

```bash
databricks apps get "$APP" -p "$PROFILE" -o json | jq '.app_status'   # expect RUNNING
databricks apps logs "$APP" -p "$PROFILE-oauth" | tail -20            # scheduler started, snapshot N
```

Open the app URL (from `databricks apps get`) in a browser and hit **`/api/diag`** — it
live-tests repo + every target and reports `pg_monitor` visibility. Empty charts almost
always trace back to a red line here.

---

## 4. Target a specific database (the main event)

Targets are a JSON array in `SEED_TARGETS`. Each entry:

```json
{ "label": "human name", "endpoint": "projects/<p>/branches/<b>/endpoints/<e>", "dbname": "<database>" }
```

### 4.1 A database on the SAME instance as the repository

Nothing extra to grant (the SP already has `pg_monitor` on this instance from §3.4). Just
name the database:

```json
[
  {"label":"orders (prod)",  "endpoint":"projects/<LAKEBASE_PROJECT>/branches/production/endpoints/primary", "dbname":"orders"},
  {"label":"billing (prod)", "endpoint":"projects/<LAKEBASE_PROJECT>/branches/production/endpoints/primary", "dbname":"billing"}
]
```

Put that in `app.yaml`'s `SEED_TARGETS` and redeploy (§3.6). Or add at runtime (§4.3).

### 4.2 A database on a DIFFERENT Lakebase instance / project

Two one-time grants on that **other** instance/project, then add the target:

```bash
OTHER_PROJECT=some-other-prj
OTHER_ENDPOINT=projects/$OTHER_PROJECT/branches/production/endpoints/primary

# (a) let the SP mint tokens for that project
databricks api patch /api/2.0/permissions/database-projects/$OTHER_PROJECT -p "$PROFILE" \
  --json "{\"access_control_list\":[{\"service_principal_name\":\"$SP\",\"permission_level\":\"CAN_USE\"}]}"

# (b) register the SP + pg_monitor on that instance
OHOST=$(databricks postgres list-endpoints "${OTHER_ENDPOINT%/endpoints/*}" -p "$PROFILE" -o json | jq -r '.[0].status.hosts.host')
OTOK=$(databricks postgres generate-database-credential "$OTHER_ENDPOINT" -p "$PROFILE" -o json | jq -r '.token')
PGPASSWORD=$OTOK psql "host=$OHOST port=5432 dbname=postgres user=$EMAIL sslmode=require" <<SQL
CREATE EXTENSION IF NOT EXISTS databricks_auth;
SELECT databricks_create_role('$SP','SERVICE_PRINCIPAL');
GRANT pg_monitor TO "$SP";
SQL
```

Then add the target (note the different `endpoint`):

```json
{"label":"analytics (other)", "endpoint":"projects/some-other-prj/branches/production/endpoints/primary", "dbname":"analytics"}
```

The app still writes everything back to the single repository DB; the UI's **target
dropdown** switches which one you're looking at.

### 4.3 Add / enable / disable targets at runtime (no redeploy)

The app exposes a small API (behind the app's OIDC, so call it from a browser session or
with an app-scoped token):

```
GET  /api/targets                      # list
POST /api/targets                      # {"label":..,"endpoint":..,"dbname":..}
POST /api/targets/{id}/enabled         # {"enabled": true|false}
```

Runtime-added targets persist in the repo `targets` table. `SEED_TARGETS` is only re-applied
(idempotently) at startup, so it's the durable way to declare targets across redeploys.

> **ASH needs activity.** A target with no *active* sessions produces no ASH rows — that's
> correct, not a bug. Snapshots still accrue from cumulative `pg_stat_statements` counters.

---

## 5. Cadence & retention

| Env | Default | Meaning |
|-----|---------|---------|
| `SAMPLE_INTERVAL_SECS` | `2` | ASH poll interval per target |
| `SNAPSHOT_INTERVAL_SECS` | `300` | Cumulative-counter snapshot interval |
| `RETENTION_DAYS` | `7` | Age at which samples/snapshots are purged (6-hourly job) |
| `INCLUDE_IDLE_IN_TXN` | `false` | Also sample `idle in transaction` sessions |

Lower `SAMPLE_INTERVAL_SECS` for finer ASH resolution (more rows, more load); raise
`SNAPSHOT_INTERVAL_SECS` to reduce overhead. Change in `app.yaml` and redeploy.

> **Cost:** the collector keeps a connection open and polls continuously, so a monitored
> endpoint **won't scale to zero** while the app runs. Expected for a monitoring app —
> disable idle targets (§4.3) to let their endpoints sleep.

---

## 6. Troubleshoot

| Symptom | Cause & fix |
|---|---|
| App `CRASHED` ~30s after start; logs show `'WorkspaceClient' object has no attribute 'postgres'` then `PoolTimeout` | SDK too old. Pin `databricks-sdk>=0.132` and redeploy. |
| `databricks apps logs` → `OAuth Token not supported for current auth type pat` | Use an OAuth profile (`--profile <p>-oauth`). |
| `/api/diag` repo line red, `must be owner of table` at startup | SP doesn't own schema `snap`. Do the ownership transfer in §3.4. |
| `/api/diag` target red, `pg_monitor: false` | SP not granted `pg_monitor` on that instance (§3.4 / §4.2). |
| Target red, connection error | SP missing `CAN_USE` on that project, or wrong `endpoint`/`dbname`. |
| Charts empty but target green | No *active* sessions on the target (ASH), or fewer than 2 snapshots yet (snapshot diff). Generate load or wait one snapshot interval. |

Diagnostics: **`GET /api/diag`** (connectivity + privileges + env), **`GET /healthz`**
(always 200), `databricks apps logs "$APP" -p "$PROFILE-oauth"`.

---

## 7. Teardown

```bash
databricks apps delete "$APP" -p "$PROFILE"
databricks workspace delete "/Workspace/Users/$EMAIL/$APP" --recursive -p "$PROFILE"
# optional: drop the repository DB (irreversible — deletes all history)
PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=postgres user=$EMAIL sslmode=require" \
  -c "DROP DATABASE $REPO_DB;"
```

---

## Appendix — full environment reference

| Env | Required | Notes |
|-----|----------|-------|
| `REPO_ENDPOINT` | yes | Lakebase endpoint path hosting the repository DB |
| `PGHOST` / `PGPORT` / `PGSSLMODE` | yes | Repo connection (`5432` / `require`) |
| `REPO_DBNAME` / `REPO_SCHEMA` | yes | Repository database + schema (`snap`) |
| `PGUSER` | yes | App SP client-id = its Postgres role name |
| `SEED_TARGETS` | yes | JSON array of `{label, endpoint, dbname}` |
| `SAMPLE_INTERVAL_SECS` / `SNAPSHOT_INTERVAL_SECS` / `RETENTION_DAYS` | no | Cadence + retention |
| `INCLUDE_IDLE_IN_TXN` | no | Sample idle-in-transaction sessions |

Local runs (`run_local.sh`) use your own identity via the CLI profile instead of the SP, so
you can develop without the SP grants — you just need `pg_monitor` yourself (the
project-owner role has it via `databricks_superuser`).
