# lakebase_snapper — ASH + AWR-style performance sampler for Databricks Lakebase

A **client-side** Postgres performance tool for managed endpoints (Lakebase, but works
against any Postgres). It does *not* need OS/host access or a custom server extension — it
just needs a SQL connection and read access to `pg_stat_activity` / `pg_stat_statements`
(you get this automatically on Lakebase via the `databricks_superuser` / `pg_monitor`
roles).

> **Why not [pgsnapper](https://github.com/tanelpoder/pgsnapper)?** That tool is an
> eBPF observer that attaches kernel probes to the Postgres backend processes and needs
> root on the database host. On a managed service you have no host access, so it can't
> run there. `lakebase_snapper` takes the SQL-client approach instead (the classic Oracle
> ASH/AWR pattern), which is the only thing possible on a managed endpoint.

Two capabilities:

| Mode | Analogous to | What it does |
|------|--------------|--------------|
| **ASH sampler** (`sample` → `ash-report`) | Oracle ASH / `pg_stat_activity` polling | Polls active sessions every ~1s, stores each, then reports Average Active Sessions (AAS), top waits, top SQL, top users/apps for any window |
| **Snapshots** (`snap` → `snap-report`) | Oracle AWR / statspack | Captures cumulative counters (`pg_stat_statements` + system stats), diffs two points in time into a load profile + top-SQL-by-time report |

All data lands in a **local SQLite** file (default `~/lakebase_snapper/lakebase_snapper.db`). This is
deliberate: Lakebase scales to zero and its in-memory `pg_stat_*` views reset on
suspend/restart, so the durable history has to live on the client.

## Install

Already set up in this directory:
- `.venv/` with `psycopg[binary]`
- `lakebase_snapper.py` — the tool
- `lakebase-snapper` — wrapper that runs it with the venv
- `loadgen.py` — optional load generator for testing

To recreate elsewhere:
```bash
python3 -m venv .venv
./.venv/bin/pip install --index-url https://pypi-proxy.cloud.databricks.com/simple "psycopg[binary]"
```

## Connect

**Lakebase (default):** point it at a CLI profile + endpoint. It resolves the host, your
email, and mints an OAuth token automatically (and refreshes it — tokens live ~1h).
```bash
export LAKEBASE_SNAPPER_PROFILE=<PROFILE>
export LAKEBASE_SNAPPER_ENDPOINT=projects/<project>/branches/<branch>/endpoints/<endpoint>
./lakebase-snapper probe
```
Or per-invocation: `./lakebase-snapper --profile <PROFILE> --endpoint projects/.../endpoints/primary probe`.
Add `--dbname <db>` if not `postgres`.

**Any Postgres (escape hatch):** `export LAKEBASE_SNAPPER_DSN='host=... dbname=... user=... password=...'`

## Use

```bash
# 0. verify connectivity + privileges + pg_stat_statements
./lakebase-snapper probe

# 1. ensure pg_stat_statements (already installed on Lakebase); --reset zeroes counters
./lakebase-snapper setup [--reset]

# --- ASH workflow -------------------------------------------------
# sample active sessions every 0.5s for 60s (Ctrl-C to stop; omit --duration to run open-ended)
./lakebase-snapper sample --interval 0.5 --duration 60
./lakebase-snapper sample --include-idle-in-txn      # also capture 'idle in transaction'

# report over everything, or a window (ISO8601 UTC)
./lakebase-snapper ash-report
./lakebase-snapper ash-report --since 2026-08-18T15:42:00 --until 2026-08-18T15:43:00 --top 15

# --- Snapshot / AWR workflow --------------------------------------
./lakebase-snapper snap --label before        # baseline
#   ... let the workload run ...
./lakebase-snapper snap --label after         # second point
./lakebase-snapper list-snaps                 # find the ids
./lakebase-snapper snap-report --from 1 --to 2 --top 15
```

The `query_id` shown in `ash-report` matches the `query_id` in `snap-report`, so you can
pivot from "what sessions were waiting on" to "how much time/IO that statement consumed."

## Testing with load

```bash
./.venv/bin/python loadgen.py <workers> <seconds>   # e.g. 4 workers, 20s
```
Runs a mix of CPU aggregates, temp-spilling sorts, and `pg_sleep()`s so you get a
realistic ASH breakdown (CPU / Timeout / IO) and snapshot deltas.

## Lakebase-specific notes

- **Privileges:** the Lakebase project-owner role is a member of `databricks_superuser`
  (inherits `pg_monitor` + `pg_read_all_data`), so you can see other sessions' query text
  and reset `pg_stat_statements`. `probe` confirms this.
- **Keeps the endpoint awake:** an open sampling connection querying every ~1s prevents
  scale-to-zero for as long as it runs, which incurs compute cost. Sample during the
  window you care about rather than 24/7.
- **In-memory stats reset on suspend.** `pg_stat_statements` and `pg_stat_activity` are
  cleared when the endpoint suspends/restarts. Snapshots stored locally survive; but a
  `snap-report` spanning a restart will show reset (negative) counters filtered out.
- **Token expiry:** OAuth tokens last ~1h. The tool refreshes proactively (>45 min) and
  reconnects on failure, so long sampling runs are fine.

## Relationship to Databricks' built-in observability

Lakebase ships **Advanced Postgres telemetry** that streams query plans, session history,
and top queries into Delta tables in Unity Catalog, with prebuilt dashboards. If you want
durable, always-on, in-platform history, prefer that. `lakebase_snapper` is for **ad-hoc,
client-side, on-demand** investigation — a POC bench session, reproducing a customer
issue, or a quick "what's running right now" — with zero platform setup and results you
control locally.
