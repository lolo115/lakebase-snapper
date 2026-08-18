#!/usr/bin/env python3
"""
lakebase_snapper - a client-side performance sampler for Databricks Lakebase (managed Postgres).

Two complementary capabilities, inspired by Oracle ASH + AWR (and Tanel Poder's snapper),
implemented purely as a SQL *client* so it works on a managed endpoint with no OS/host
access and no server-side extension beyond the already-available pg_stat_statements:

  1. ASH sampler  -> polls pg_stat_activity on a short interval and records every active
                     session (Active Session History). Aggregating these samples gives you
                     Average Active Sessions (AAS) and a top-of-waits / top-of-queries
                     breakdown for any time window.

  2. Snapshots    -> captures cumulative counters (pg_stat_statements + system stats) at a
                     point in time. Diffing two snapshots gives an AWR-style "what ran
                     between T1 and T2" report.

Everything is persisted to a *local* SQLite repository, which is deliberate: Lakebase
scales to zero and its in-memory pg_stat_* views reset on suspend/restart, so the durable
record has to live on the client side.

Auth: by default it drives the `databricks` CLI to resolve the endpoint host and mint a
short-lived OAuth token (auto-refreshed), so it is Lakebase-native. You can also point it
at any Postgres with --dsn / LAKEBASE_SNAPPER_DSN.

See README.md for usage.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

try:
    import psycopg
except ImportError:
    sys.exit("psycopg is required. Install with: pip install 'psycopg[binary]'")


# --------------------------------------------------------------------------------------
# Connection handling (Lakebase-native, with a plain-DSN escape hatch)
# --------------------------------------------------------------------------------------

# Lakebase OAuth tokens live ~1h; refresh well before that so long sampling runs survive.
TOKEN_TTL_SECONDS = 45 * 60


class Target:
    """Resolves connection parameters and hands out a live psycopg connection,
    transparently refreshing the Lakebase OAuth token and reconnecting on failure."""

    def __init__(self, args):
        self.dsn = args.dsn or os.environ.get("LAKEBASE_SNAPPER_DSN")
        self.profile = args.profile
        self.endpoint = args.endpoint  # projects/.../branches/.../endpoints/...
        self.dbname = args.dbname
        self._conn = None
        self._token_at = 0.0
        # Stable identity string used to tag rows in the repository.
        if self.dsn:
            self.identity = _dsn_identity(self.dsn)
        else:
            self.identity = f"{self.endpoint}#{self.dbname}"

    # -- Databricks CLI helpers --------------------------------------------------------
    def _cli(self, *cmd):
        full = ["databricks", *cmd, "--profile", self.profile, "--output", "json"]
        out = subprocess.run(full, capture_output=True, text=True)
        if out.returncode != 0:
            raise RuntimeError(f"`{' '.join(full)}` failed:\n{out.stderr.strip()}")
        return json.loads(out.stdout)

    def _resolve_host(self):
        branch = self.endpoint.rsplit("/endpoints/", 1)[0]
        ep_name = self.endpoint.rsplit("/", 1)[1]
        endpoints = self._cli("postgres", "list-endpoints", branch)
        for ep in endpoints:
            if ep.get("name", "").endswith("/" + ep_name) or ep.get("name") == self.endpoint:
                return ep["status"]["hosts"]["host"]
        raise RuntimeError(f"endpoint {self.endpoint} not found under {branch}")

    def _resolve_user(self):
        return self._cli("current-user", "me")["userName"]

    def _mint_token(self):
        return self._cli("postgres", "generate-database-credential", self.endpoint)["token"]

    # -- connection lifecycle ----------------------------------------------------------
    def _connect(self):
        if self.dsn:
            conn = psycopg.connect(self.dsn, autocommit=True)
        else:
            if not hasattr(self, "_host"):
                self._host = self._resolve_host()
                self._user = self._resolve_user()
            token = self._mint_token()
            self._token_at = time.time()
            conn = psycopg.connect(
                host=self._host, port=5432, dbname=self.dbname,
                user=self._user, password=token, sslmode="require", autocommit=True,
            )
        # Keep the sampler out of pg_stat_activity's own results as much as possible.
        conn.execute("SET application_name = 'lakebase_snapper'")
        return conn

    def conn(self):
        """Return a healthy connection, refreshing token / reconnecting as needed."""
        need_new = self._conn is None or self._conn.closed
        if not need_new and not self.dsn and (time.time() - self._token_at) > TOKEN_TTL_SECONDS:
            # Proactively refresh before the token expires.
            try:
                self._conn.close()
            except Exception:
                pass
            need_new = True
        if need_new:
            self._conn = self._connect()
        return self._conn

    def execute(self, sql, params=None, retry=True):
        try:
            cur = self.conn().execute(sql, params or ())
            return cur.fetchall() if cur.description is not None else []
        except (psycopg.OperationalError, psycopg.InterfaceError):
            if not retry:
                raise
            # Endpoint may have scaled to zero / token expired; rebuild once and retry.
            try:
                if self._conn:
                    self._conn.close()
            except Exception:
                pass
            self._conn = None
            cur = self.conn().execute(sql, params or ())
            return cur.fetchall() if cur.description is not None else []


def _dsn_identity(dsn: str) -> str:
    try:
        info = psycopg.conninfo.conninfo_to_dict(dsn)
        return f"{info.get('host','?')}:{info.get('port','5432')}/{info.get('dbname','?')}"
    except Exception:
        return "dsn"


def now_utc():
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------------------
# Local repository (SQLite) - durable across Lakebase scale-to-zero
# --------------------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS ash_samples (
    target       TEXT NOT NULL,
    sample_time  TEXT NOT NULL,          -- ISO8601 UTC (server clock_timestamp)
    pid          INTEGER,
    usename      TEXT,
    application_name TEXT,
    client_addr  TEXT,
    backend_type TEXT,
    state        TEXT,
    wait_class   TEXT,                    -- 'CPU' when active with no wait, else wait_event_type
    wait_event   TEXT,
    query_id     TEXT,
    query        TEXT,
    xact_secs    REAL,                    -- age of open transaction at sample time
    query_secs   REAL                     -- age of current query at sample time
);
CREATE INDEX IF NOT EXISTS ix_ash_target_time ON ash_samples(target, sample_time);

CREATE TABLE IF NOT EXISTS snapshots (
    snap_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    target       TEXT NOT NULL,
    snap_time    TEXT NOT NULL,           -- ISO8601 UTC (server now())
    label        TEXT,
    sys          TEXT                     -- JSON of system-level cumulative counters
);

CREATE TABLE IF NOT EXISTS snap_pgss (
    snap_id      INTEGER NOT NULL,
    queryid      TEXT,
    userid       INTEGER,
    dbid         INTEGER,
    toplevel     INTEGER,
    query        TEXT,
    metrics      TEXT,                     -- JSON of pg_stat_statements numeric columns
    PRIMARY KEY (snap_id, queryid, userid, dbid, toplevel)
);
"""


class Repo:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.executescript(SCHEMA)
        self.db.commit()

    def insert_samples(self, target, rows):
        self.db.executemany(
            """INSERT INTO ash_samples
               (target, sample_time, pid, usename, application_name, client_addr,
                backend_type, state, wait_class, wait_event, query_id, query,
                xact_secs, query_secs)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [(target, *r) for r in rows],
        )
        self.db.commit()

    def new_snapshot(self, target, snap_time, label, sys_json):
        cur = self.db.execute(
            "INSERT INTO snapshots (target, snap_time, label, sys) VALUES (?,?,?,?)",
            (target, snap_time, label, sys_json),
        )
        self.db.commit()
        return cur.lastrowid

    def insert_pgss(self, snap_id, rows):
        self.db.executemany(
            """INSERT OR REPLACE INTO snap_pgss
               (snap_id, queryid, userid, dbid, toplevel, query, metrics)
               VALUES (?,?,?,?,?,?,?)""",
            [(snap_id, *r) for r in rows],
        )
        self.db.commit()


# --------------------------------------------------------------------------------------
# SQL run against the Lakebase endpoint
# --------------------------------------------------------------------------------------

# ASH: one row per active session. state='active' == executing (on CPU or mid-query wait).
# statement_timestamp() is constant for all rows of a single poll, so it uniquely
# identifies one sampling iteration -> the denominator for Average Active Sessions.
# clock_timestamp() (which advances per row) is used only for age math below.
ASH_SQL = """
SELECT statement_timestamp(),
       pid, usename, application_name, host(client_addr), backend_type, state,
       CASE WHEN wait_event IS NULL THEN 'CPU' ELSE wait_event_type END AS wait_class,
       wait_event,
       query_id::text,
       left(query, 4000),
       EXTRACT(EPOCH FROM (clock_timestamp() - xact_start)),
       EXTRACT(EPOCH FROM (clock_timestamp() - query_start))
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND ( state = 'active' {idle_in_txn} )
"""

# System-level cumulative counters, summed across databases where it makes sense.
SYS_SQL = """
SELECT json_build_object(
  'server_now', now(),
  'db', (SELECT json_build_object(
            'xact_commit', COALESCE(sum(xact_commit),0),
            'xact_rollback', COALESCE(sum(xact_rollback),0),
            'blks_read', COALESCE(sum(blks_read),0),
            'blks_hit', COALESCE(sum(blks_hit),0),
            'tup_returned', COALESCE(sum(tup_returned),0),
            'tup_fetched', COALESCE(sum(tup_fetched),0),
            'tup_inserted', COALESCE(sum(tup_inserted),0),
            'tup_updated', COALESCE(sum(tup_updated),0),
            'tup_deleted', COALESCE(sum(tup_deleted),0),
            'temp_files', COALESCE(sum(temp_files),0),
            'temp_bytes', COALESCE(sum(temp_bytes),0),
            'deadlocks', COALESCE(sum(deadlocks),0),
            'blk_read_time', COALESCE(sum(blk_read_time),0),
            'blk_write_time', COALESCE(sum(blk_write_time),0))
         FROM pg_stat_database WHERE datname IS NOT NULL),
  'bgwriter', (SELECT row_to_json(b) FROM pg_stat_bgwriter b)
)
"""


def fetch_pgss(target):
    """Return (rows, metric_columns). Column set is resolved dynamically so the tool
    works across pg_stat_statements versions (1.9 .. 1.11+)."""
    candidate_cols = [
        "calls", "total_exec_time", "total_plan_time", "rows", "plans",
        "shared_blks_hit", "shared_blks_read", "shared_blks_dirtied", "shared_blks_written",
        "local_blks_hit", "local_blks_read", "local_blks_dirtied", "local_blks_written",
        "temp_blks_read", "temp_blks_written",
        "blk_read_time", "blk_write_time",
        "shared_blk_read_time", "shared_blk_write_time",
        "temp_blk_read_time", "temp_blk_write_time",
        "wal_records", "wal_fpi", "wal_bytes",
    ]
    present = {r[0] for r in target.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='pg_stat_statements'")}
    cols = [c for c in candidate_cols if c in present]
    metric_select = ", ".join(cols)
    sql = f"""
      SELECT queryid::text, userid, dbid,
             {"toplevel" if "toplevel" in present else "true AS toplevel"},
             left(query, 4000),
             {metric_select}
      FROM pg_stat_statements
      WHERE queryid IS NOT NULL
    """
    rows = target.execute(sql)
    out = []
    for r in rows:
        queryid, userid, dbid, toplevel, query = r[0], r[1], r[2], r[3], r[4]
        # Coerce Decimal (e.g. wal_bytes) to float so it survives JSON round-trips.
        metrics = {c: (float(r[5 + i]) if isinstance(r[5 + i], Decimal) else r[5 + i])
                   for i, c in enumerate(cols)}
        out.append((queryid, userid, dbid, int(bool(toplevel)), query, json.dumps(metrics)))
    return out, cols


# --------------------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------------------

def cmd_probe(args, target, repo):
    v = target.execute("SELECT version()")[0][0]
    who = target.execute("SELECT current_user")[0][0]
    roles = {}
    for role in ("pg_monitor", "pg_read_all_data", "databricks_superuser"):
        roles[role] = target.execute("SELECT pg_has_role(current_user,%s,'MEMBER')", (role,))[0][0]
    pgss = target.execute("SELECT count(*) FROM pg_extension WHERE extname='pg_stat_statements'")[0][0]
    print(f"Connected to: {target.identity}")
    print(f"  {v}")
    print(f"  current_user      : {who}")
    print(f"  pg_monitor        : {roles['pg_monitor']}")
    print(f"  pg_read_all_data  : {roles['pg_read_all_data']}")
    print(f"  databricks_superuser: {roles['databricks_superuser']}")
    print(f"  pg_stat_statements installed: {'yes' if pgss else 'NO (run: lakebase_snapper setup)'}")


def cmd_setup(args, target, repo):
    target.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
    print("pg_stat_statements ensured.")
    if args.reset:
        target.execute("SELECT pg_stat_statements_reset()")
        print("pg_stat_statements counters reset.")


def cmd_sample(args, target, repo):
    idle_clause = "OR state = 'idle in transaction'" if args.include_idle_in_txn else ""
    sql = ASH_SQL.format(idle_in_txn=idle_clause)
    deadline = time.time() + args.duration if args.duration else None
    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.__setitem__("flag", True))

    print(f"Sampling {target.identity} every {args.interval}s"
          + (f" for {args.duration}s" if args.duration else " (Ctrl-C to stop)")
          + f" -> {args.repo}")
    n_samples = 0
    n_rows = 0
    while not stop["flag"]:
        t0 = time.time()
        try:
            rows = target.execute(sql)
        except Exception as e:
            print(f"  [warn] sample skipped: {e}", file=sys.stderr)
            rows = []
        # Normalize sample_time to ISO string.
        norm = []
        for r in rows:
            # SQLite can't bind Decimal (xact_secs/query_secs come back as numeric).
            r = [float(v) if isinstance(v, Decimal) else v for v in r]
            r[0] = r[0].astimezone(timezone.utc).isoformat()
            norm.append(r)
        if norm:
            repo.insert_samples(target.identity, norm)
        n_samples += 1
        n_rows += len(norm)
        if n_samples % 10 == 0:
            print(f"  {n_samples} samples, {n_rows} active-session rows captured", flush=True)
        if deadline and time.time() >= deadline:
            break
        # Keep a steady cadence regardless of query time.
        sleep = args.interval - (time.time() - t0)
        if sleep > 0:
            time.sleep(sleep)
    print(f"Done. {n_samples} samples, {n_rows} active-session rows into {args.repo}")


def cmd_snap(args, target, repo):
    sys_json = target.execute(SYS_SQL)[0][0]
    if not isinstance(sys_json, str):
        sys_json = json.dumps(sys_json)
    snap_time = target.execute("SELECT now()")[0][0].astimezone(timezone.utc).isoformat()
    snap_id = repo.new_snapshot(target.identity, snap_time, args.label, sys_json)
    pgss_rows, cols = fetch_pgss(target)
    repo.insert_pgss(snap_id, pgss_rows)
    print(f"Snapshot {snap_id} taken at {snap_time} "
          f"({len(pgss_rows)} statements, metrics: {', '.join(cols)})")
    print(f"Diff later with:  lakebase_snapper snap-report --from {snap_id} --to <newer-id>")


def cmd_list_snaps(args, target, repo):
    rows = repo.db.execute(
        "SELECT snap_id, snap_time, label, target FROM snapshots ORDER BY snap_id").fetchall()
    if not rows:
        print("No snapshots yet. Take one with: lakebase_snapper snap")
        return
    print(f"{'ID':>4}  {'snap_time (UTC)':<32}  {'label':<20}  target")
    for sid, st, lbl, tgt in rows:
        print(f"{sid:>4}  {st:<32}  {(lbl or ''):<20}  {tgt}")


def _fmt(n, unit=""):
    if isinstance(n, float):
        return f"{n:,.1f}{unit}"
    return f"{n:,}{unit}"


def cmd_ash_report(args, target, repo):
    where = ["target = ?"]
    params = [target.identity]
    if args.since:
        where.append("sample_time >= ?"); params.append(args.since)
    if args.until:
        where.append("sample_time <= ?"); params.append(args.until)
    wsql = " AND ".join(where)

    total, distinct_samples = repo.db.execute(
        f"SELECT count(*), count(DISTINCT sample_time) FROM ash_samples WHERE {wsql}",
        params).fetchone()
    if not total:
        print("No ASH samples match. Run `lakebase_snapper sample` first (check --since/--until).")
        return

    # Time span & Average Active Sessions.
    tmin, tmax = repo.db.execute(
        f"SELECT min(sample_time), max(sample_time) FROM ash_samples WHERE {wsql}", params).fetchone()
    span = (datetime.fromisoformat(tmax) - datetime.fromisoformat(tmin)).total_seconds() or 1.0
    aas = total / distinct_samples if distinct_samples else 0

    print("=" * 78)
    print(f"ASH report for {target.identity}")
    print(f"  window   : {tmin}  ->  {tmax}  ({span:,.0f}s)")
    print(f"  samples  : {distinct_samples} distinct timestamps, {total} active-session rows")
    print(f"  Avg Active Sessions (AAS): {aas:.2f}")
    print("=" * 78)

    def top(title, group_expr, limit=args.top):
        rows = repo.db.execute(
            f"""SELECT {group_expr} AS g, count(*) c
                FROM ash_samples WHERE {wsql}
                GROUP BY g ORDER BY c DESC LIMIT {limit}""", params).fetchall()
        print(f"\n{title}")
        print(f"  {'%':>6}  {'AAS':>6}  {'samples':>8}  item")
        for g, c in rows:
            pct = 100.0 * c / total
            print(f"  {pct:6.1f}  {c/distinct_samples:6.2f}  {c:8}  {g if g is not None else '<null>'}")

    top("Top wait classes (CPU = on-CPU, else wait_event_type):", "wait_class")
    top("Top wait events:", "COALESCE(wait_class,'') || ' / ' || COALESCE(wait_event,'CPU')")
    top("Top backend types:", "backend_type")
    top("Top users:", "usename")
    top("Top applications:", "application_name")

    # Top SQL by activity, with query text.
    rows = repo.db.execute(
        f"""SELECT query_id, count(*) c,
                   max(substr(replace(replace(query,char(10),' '),char(9),' '),1,90)) q
            FROM ash_samples WHERE {wsql}
            GROUP BY query_id ORDER BY c DESC LIMIT {args.top}""", params).fetchall()
    print("\nTop SQL by active samples:")
    print(f"  {'%':>6}  {'AAS':>6}  {'query_id':>20}  query")
    for qid, c, q in rows:
        pct = 100.0 * c / total
        print(f"  {pct:6.1f}  {c/distinct_samples:6.2f}  {str(qid):>20}  {q or '<null>'}")


def cmd_snap_report(args, target, repo):
    a = repo.db.execute("SELECT snap_time, sys FROM snapshots WHERE snap_id=?", (args.frm,)).fetchone()
    b = repo.db.execute("SELECT snap_time, sys FROM snapshots WHERE snap_id=?", (args.to,)).fetchone()
    if not a or not b:
        print("One or both snapshot IDs not found. See `lakebase_snapper list-snaps`.")
        return
    t_a = datetime.fromisoformat(a[0]); t_b = datetime.fromisoformat(b[0])
    elapsed = (t_b - t_a).total_seconds()
    if elapsed <= 0:
        print("Second snapshot must be newer than the first."); return

    print("=" * 78)
    print(f"Snapshot diff  {args.frm} -> {args.to}   ({elapsed:,.0f}s elapsed)")
    print(f"  {a[0]}  ->  {b[0]}")
    print("=" * 78)

    # System counters delta + per-second rate.
    sa = json.loads(a[1]); sb = json.loads(b[1])
    dba, dbb = sa.get("db", {}), sb.get("db", {})
    print("\nSystem load profile (delta over window, and per second):")
    print(f"  {'metric':<18}  {'delta':>16}  {'per second':>16}")
    for k in ("xact_commit", "xact_rollback", "blks_read", "blks_hit",
              "tup_returned", "tup_fetched", "tup_inserted", "tup_updated",
              "tup_deleted", "temp_files", "temp_bytes", "deadlocks"):
        d = (dbb.get(k, 0) or 0) - (dba.get(k, 0) or 0)
        print(f"  {k:<18}  {_fmt(d):>16}  {_fmt(d/elapsed):>16}")
    # Cache hit ratio over the window.
    dr = (dbb.get("blks_read", 0) - dba.get("blks_read", 0))
    dh = (dbb.get("blks_hit", 0) - dba.get("blks_hit", 0))
    if dr + dh > 0:
        print(f"  {'cache hit ratio':<18}  {100.0*dh/(dr+dh):>15.2f}%")

    # Per-statement deltas.
    def load_pgss(snap_id):
        out = {}
        for qid, uid, dbid, top, q, m in repo.db.execute(
            "SELECT queryid,userid,dbid,toplevel,query,metrics FROM snap_pgss WHERE snap_id=?",
            (snap_id,)):
            out[(qid, uid, dbid, top)] = (q, json.loads(m))
        return out

    pa, pb = load_pgss(args.frm), load_pgss(args.to)
    deltas = []
    for key, (q, mb) in pb.items():
        ma = pa.get(key, (None, {}))[1]
        d_exec = mb.get("total_exec_time", 0) - ma.get("total_exec_time", 0)
        d_calls = mb.get("calls", 0) - ma.get("calls", 0)
        d_rows = mb.get("rows", 0) - ma.get("rows", 0)
        d_read = mb.get("shared_blks_read", 0) - ma.get("shared_blks_read", 0)
        d_hit = mb.get("shared_blks_hit", 0) - ma.get("shared_blks_hit", 0)
        # Skip statements whose counters were reset or didn't move.
        if d_calls <= 0 and d_exec <= 0:
            continue
        deltas.append((d_exec, d_calls, d_rows, d_read, d_hit, key[0], q))

    tot_exec = sum(d[0] for d in deltas) or 1.0
    deltas.sort(reverse=True)
    print(f"\nTop SQL by execution time delta  (total DB exec time in window: {tot_exec:,.0f} ms):")
    print(f"  {'%time':>6}  {'exec_ms':>12}  {'calls':>10}  {'ms/call':>9}  {'rows':>10}  {'blk_read':>10}  query_id")
    for d_exec, d_calls, d_rows, d_read, d_hit, qid, q in deltas[:args.top]:
        pct = 100.0 * d_exec / tot_exec
        mspc = d_exec / d_calls if d_calls else 0
        qtxt = " ".join((q or "").split())[:70]
        print(f"  {pct:6.1f}  {d_exec:12,.0f}  {d_calls:10,}  {mspc:9,.2f}  {d_rows:10,}  {d_read:10,}  {qid}")
        print(f"          {qtxt}")


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(
        prog="lakebase_snapper",
        description="Client-side ASH sampler + snapshot analyzer for Databricks Lakebase (Postgres).")
    # Connection (shared).
    p.add_argument("--profile", default=os.environ.get("LAKEBASE_SNAPPER_PROFILE"),
                   help="Databricks CLI profile (Lakebase auth).")
    p.add_argument("--endpoint", default=os.environ.get("LAKEBASE_SNAPPER_ENDPOINT"),
                   help="projects/<p>/branches/<b>/endpoints/<e>")
    p.add_argument("--dbname", default=os.environ.get("LAKEBASE_SNAPPER_DBNAME", "postgres"),
                   help="database name (default: postgres)")
    p.add_argument("--dsn", default=None,
                   help="plain libpq DSN, bypasses Databricks auth (or LAKEBASE_SNAPPER_DSN)")
    p.add_argument("--repo", default=os.path.expanduser("~/lakebase_snapper/lakebase_snapper.db"),
                   help="SQLite repository path")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("probe", help="test connection and print capabilities").set_defaults(fn=cmd_probe)

    sp = sub.add_parser("setup", help="ensure pg_stat_statements extension")
    sp.add_argument("--reset", action="store_true", help="also reset pg_stat_statements counters")
    sp.set_defaults(fn=cmd_setup)

    ss = sub.add_parser("sample", help="run the ASH sampler loop")
    ss.add_argument("--interval", type=float, default=1.0, help="seconds between samples (default 1)")
    ss.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until Ctrl-C)")
    ss.add_argument("--include-idle-in-txn", action="store_true",
                    help="also capture 'idle in transaction' sessions")
    ss.set_defaults(fn=cmd_sample)

    sn = sub.add_parser("snap", help="capture a cumulative-counter snapshot")
    sn.add_argument("--label", default=None, help="optional label")
    sn.set_defaults(fn=cmd_snap)

    sub.add_parser("list-snaps", help="list stored snapshots").set_defaults(fn=cmd_list_snaps)

    ar = sub.add_parser("ash-report", help="report from stored ASH samples")
    ar.add_argument("--since", help="ISO8601 lower bound (UTC)")
    ar.add_argument("--until", help="ISO8601 upper bound (UTC)")
    ar.add_argument("--top", type=int, default=10, help="rows per section (default 10)")
    ar.set_defaults(fn=cmd_ash_report)

    dr = sub.add_parser("snap-report", help="diff two snapshots (AWR-style)")
    dr.add_argument("--from", dest="frm", type=int, required=True, help="baseline snap_id")
    dr.add_argument("--to", type=int, required=True, help="newer snap_id")
    dr.add_argument("--top", type=int, default=15, help="top statements (default 15)")
    dr.set_defaults(fn=cmd_snap_report)
    return p


def main():
    args = build_parser().parse_args()
    if not args.dsn and not os.environ.get("LAKEBASE_SNAPPER_DSN"):
        if not (args.profile and args.endpoint):
            sys.exit("Provide --profile and --endpoint (Lakebase), or --dsn / LAKEBASE_SNAPPER_DSN.")
    os.makedirs(os.path.dirname(os.path.abspath(args.repo)), exist_ok=True)
    repo = Repo(args.repo)
    target = Target(args)
    args.fn(args, target, repo)


if __name__ == "__main__":
    main()
