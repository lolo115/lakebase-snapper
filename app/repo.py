"""Repository schema (Postgres, schema `snap`) + write helpers + chart queries."""

from __future__ import annotations

import json
from psycopg.types.json import Jsonb
from core import repo_pool

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS targets (
    id          serial PRIMARY KEY,
    label       text UNIQUE NOT NULL,
    endpoint    text NOT NULL,
    dbname      text NOT NULL DEFAULT 'postgres',
    enabled     boolean NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ash_samples (
    target_id     int NOT NULL REFERENCES targets(id) ON DELETE CASCADE,
    sample_time   timestamptz NOT NULL,
    pid           int,
    usename       text,
    application_name text,
    client_addr   text,
    backend_type  text,
    state         text,
    wait_class    text,
    wait_event    text,
    query_id      text,
    query         text,
    xact_secs     double precision,
    query_secs    double precision,
    datname       text
);
CREATE INDEX IF NOT EXISTS ix_ash ON ash_samples(target_id, sample_time);
-- migrate existing deployments that predate the datname column
ALTER TABLE ash_samples ADD COLUMN IF NOT EXISTS datname text;

CREATE TABLE IF NOT EXISTS snapshots (
    snap_id     serial PRIMARY KEY,
    target_id   int NOT NULL REFERENCES targets(id) ON DELETE CASCADE,
    snap_time   timestamptz NOT NULL,
    label       text,
    sys         jsonb
);
CREATE INDEX IF NOT EXISTS ix_snap ON snapshots(target_id, snap_time);

CREATE TABLE IF NOT EXISTS snap_pgss (
    snap_id     int NOT NULL REFERENCES snapshots(snap_id) ON DELETE CASCADE,
    queryid     text,
    userid      int,
    dbid        int,
    toplevel    boolean,
    query       text,
    metrics     jsonb,
    datname     text,
    PRIMARY KEY (snap_id, queryid, userid, dbid, toplevel)
);
ALTER TABLE snap_pgss ADD COLUMN IF NOT EXISTS datname text;
"""


def init_schema():
    with repo_pool.connection() as conn:
        conn.execute(SCHEMA_DDL)
        conn.commit()


# ------------------------------------------------------------------ targets
def upsert_target(label, endpoint, dbname="postgres", enabled=True):
    with repo_pool.connection() as conn:
        row = conn.execute(
            """INSERT INTO targets (label, endpoint, dbname, enabled)
               VALUES (%s,%s,%s,%s)
               ON CONFLICT (label) DO UPDATE
                 SET endpoint=EXCLUDED.endpoint, dbname=EXCLUDED.dbname, enabled=EXCLUDED.enabled
               RETURNING id""",
            (label, endpoint, dbname, enabled)).fetchone()
        conn.commit()
        return row[0]


def list_targets(only_enabled=False):
    q = "SELECT id, label, endpoint, dbname, enabled FROM targets"
    if only_enabled:
        q += " WHERE enabled"
    q += " ORDER BY label"
    with repo_pool.connection() as conn:
        return [dict(zip(("id", "label", "endpoint", "dbname", "enabled"), r))
                for r in conn.execute(q).fetchall()]


def set_enabled(target_id, enabled):
    with repo_pool.connection() as conn:
        conn.execute("UPDATE targets SET enabled=%s WHERE id=%s", (enabled, target_id))
        conn.commit()


# ------------------------------------------------------------------ writes
def insert_ash(target_id, rows):
    if not rows:
        return
    with repo_pool.connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO ash_samples
                   (target_id, sample_time, pid, usename, application_name, client_addr,
                    backend_type, state, wait_class, wait_event, query_id, query,
                    xact_secs, query_secs, datname)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                [(target_id, *r) for r in rows])
        conn.commit()


def insert_snapshot(target_id, snap_time, label, sys_obj, pgss_rows):
    with repo_pool.connection() as conn:
        snap_id = conn.execute(
            "INSERT INTO snapshots (target_id, snap_time, label, sys) VALUES (%s,%s,%s,%s) RETURNING snap_id",
            (target_id, snap_time, label, Jsonb(sys_obj))).fetchone()[0]
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO snap_pgss (snap_id, queryid, userid, dbid, toplevel, query, metrics, datname)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT DO NOTHING""",
                [(snap_id, q, u, d, t, qt, Jsonb(m), dn) for (q, u, d, t, qt, m, dn) in pgss_rows])
        conn.commit()
        return snap_id


def purge_older_than(days):
    with repo_pool.connection() as conn:
        conn.execute("DELETE FROM ash_samples WHERE sample_time < now() - make_interval(days => %s)", (days,))
        conn.execute("DELETE FROM snapshots WHERE snap_time < now() - make_interval(days => %s)", (days,))
        conn.commit()


# ------------------------------------------------------------------ chart queries
def ash_timeline(target_id, since_min, bucket_secs):
    """AAS by wait_class per time bucket -> stacked area."""
    sql = """
    WITH s AS (
      SELECT date_bin(make_interval(secs => %(b)s), sample_time, 'epoch') AS bucket,
             sample_time, wait_class
      FROM ash_samples
      WHERE target_id=%(t)s AND sample_time >= now() - make_interval(mins => %(m)s)
        AND state <> 'idle'
    ),
    n AS (SELECT bucket, count(DISTINCT sample_time) nsamp FROM s GROUP BY 1)
    SELECT to_char(s.bucket AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS bucket,
           s.wait_class,
           round((count(*)::numeric / NULLIF(n.nsamp,0)), 3) AS aas
    FROM s JOIN n USING (bucket)
    GROUP BY s.bucket, s.wait_class, n.nsamp
    ORDER BY s.bucket
    """
    with repo_pool.connection() as conn:
        return [dict(zip(("bucket", "wait_class", "aas"), r))
                for r in conn.execute(sql, {"t": target_id, "m": since_min, "b": bucket_secs}).fetchall()]


def wait_mix(target_id, since_min, include_idle=False):
    """Wait-class mix over the window. Default excludes fully-idle sessions (the classic
    active-only pie). With include_idle=True, idle / idle-in-transaction samples are kept
    and relabelled as their own classes so the pie also shows idle time."""
    if include_idle:
        cls = ("CASE WHEN state='idle' THEN 'Idle' "
               "WHEN state='idle in transaction' THEN 'Idle in txn' "
               "ELSE wait_class END")
        idle_filter = ""
    else:
        cls = "wait_class"
        idle_filter = "AND state <> 'idle'"
    sql = f"""
    SELECT {cls} AS wait_class, wait_event, count(*) c
    FROM ash_samples
    WHERE target_id=%s AND sample_time >= now() - make_interval(mins => %s)
      {idle_filter}
    GROUP BY 1,2 ORDER BY c DESC
    """
    with repo_pool.connection() as conn:
        return [dict(zip(("wait_class", "wait_event", "c"), r))
                for r in conn.execute(sql, (target_id, since_min)).fetchall()]


def top_sql_ash(target_id, since_min, limit=10):
    """Active-sample breakdown by wait class (CPU = on-CPU) for the top `limit` queries.

    Returns flat rows [{query_id, wait_class, c, query}] for the top queries by total
    active samples, so the UI can stack CPU time vs each wait class per SQL. Estimated
    active time per segment is c * SAMPLE_INTERVAL_SECS (computed client-side)."""
    sql = """
    WITH win AS (
      SELECT query_id, COALESCE(datname,'?') AS datname,
             COALESCE(wait_class,'CPU') AS klass, query
      FROM ash_samples
      WHERE target_id=%(t)s AND sample_time >= now() - make_interval(mins => %(m)s)
        AND query_id IS NOT NULL AND state <> 'idle'
    ),
    top AS (
      SELECT query_id, datname, count(*) c FROM win
      GROUP BY query_id, datname ORDER BY c DESC LIMIT %(l)s
    )
    SELECT w.query_id, w.datname, w.klass, count(*) c,
           max(left(regexp_replace(w.query, '\\s+', ' ', 'g'), 110)) q
    FROM win w JOIN top t USING (query_id, datname)
    GROUP BY w.query_id, w.datname, w.klass
    ORDER BY w.query_id, w.datname
    """
    with repo_pool.connection() as conn:
        return [dict(zip(("query_id", "datname", "wait_class", "c", "query"), r))
                for r in conn.execute(sql, {"t": target_id, "m": since_min, "l": limit}).fetchall()]


def summary(target_id, since_min):
    sql = """
    SELECT count(*) total_rows,
           count(DISTINCT sample_time) samples,
           round(count(*)::numeric / NULLIF(count(DISTINCT sample_time),0), 2) aas,
           min(sample_time), max(sample_time)
    FROM ash_samples
    WHERE target_id=%s AND sample_time >= now() - make_interval(mins => %s)
      AND state <> 'idle'
    """
    with repo_pool.connection() as conn:
        r = conn.execute(sql, (target_id, since_min)).fetchone()
    return {"total_rows": r[0] or 0, "samples": r[1] or 0, "aas": float(r[2] or 0),
            "first": r[3].isoformat() if r[3] else None,
            "last": r[4].isoformat() if r[4] else None}


def load_profile(target_id, since_min):
    """Per-interval system rates derived from consecutive snapshots."""
    sql = """
    WITH s AS (
      SELECT snap_time,
             (sys->'db'->>'xact_commit')::bigint xc,
             (sys->'db'->>'blks_read')::bigint br,
             (sys->'db'->>'blks_hit')::bigint bh,
             (sys->'db'->>'temp_bytes')::bigint tb,
             (sys->'db'->>'tup_returned')::bigint tr,
             (sys->'lfc'->>'hits')::bigint lh,
             (sys->'lfc'->>'misses')::bigint lm
      FROM snapshots
      WHERE target_id=%s AND snap_time >= now() - make_interval(mins => %s)
      ORDER BY snap_time
    ),
    d AS (
      SELECT snap_time,
             EXTRACT(EPOCH FROM (snap_time - lag(snap_time) OVER w)) secs,
             xc - lag(xc) OVER w dxc,
             br - lag(br) OVER w dbr,
             bh - lag(bh) OVER w dbh,
             tb - lag(tb) OVER w dtb,
             tr - lag(tr) OVER w dtr,
             lh - lag(lh) OVER w dlh,
             lm - lag(lm) OVER w dlm
      FROM s WINDOW w AS (ORDER BY snap_time)
    )
    SELECT to_char(snap_time AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"') t,
           round((dxc/NULLIF(secs,0))::numeric,2) tps,
           CASE WHEN (dbr+dbh)>0 THEN round(100.0*dbh/(dbr+dbh),2) ELSE NULL END cache_hit_pct,
           CASE WHEN (dlh+dlm)>0 THEN round(100.0*dlh/(dlh+dlm),2) ELSE NULL END lfc_hit_pct,
           round((dtb/NULLIF(secs,0))::numeric,0) temp_bytes_per_s,
           round((dtr/NULLIF(secs,0))::numeric,0) tup_returned_per_s
    FROM d WHERE secs IS NOT NULL AND secs > 0 AND dxc >= 0
    ORDER BY t
    """
    with repo_pool.connection() as conn:
        return [dict(zip(("t", "tps", "cache_hit_pct", "lfc_hit_pct",
                          "temp_bytes_per_s", "tup_returned_per_s"), r))
                for r in conn.execute(sql, (target_id, since_min)).fetchall()]


def cu_timeline(target_id, since_min):
    """Configured autoscaling CU (upper bound) captured at each snapshot, for overlaying
    on the AAS chart. Returns [{t, max_cu, min_cu}] ordered by time."""
    sql = """
    SELECT to_char(snap_time AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"') t,
           (sys->'cu'->>'max')::float max_cu,
           (sys->'cu'->>'min')::float min_cu
    FROM snapshots
    WHERE target_id=%s AND snap_time >= now() - make_interval(mins => %s)
      AND sys ? 'cu'
    ORDER BY snap_time
    """
    with repo_pool.connection() as conn:
        return [dict(zip(("t", "max_cu", "min_cu"), r))
                for r in conn.execute(sql, (target_id, since_min)).fetchall()]


def current_max_cu(target_id):
    """Most recent configured max CU for the target's ENDPOINT (shared by all its targets),
    so the dashboard can always draw a CU reference line even for a freshly added target
    that has no snapshots of its own yet."""
    sql = """
    SELECT (s.sys->'cu'->>'max')::float
    FROM snapshots s
    WHERE s.target_id IN (SELECT id FROM targets
                          WHERE endpoint = (SELECT endpoint FROM targets WHERE id=%s))
      AND s.sys ? 'cu'
    ORDER BY s.snap_time DESC LIMIT 1
    """
    with repo_pool.connection() as conn:
        r = conn.execute(sql, (target_id,)).fetchone()
    return {"max_cu": float(r[0]) if r and r[0] is not None else None}


def latest_snap_diff(target_id, limit=12):
    """Top SQL by execution-time delta between the two most recent snapshots."""
    with repo_pool.connection() as conn:
        ids = conn.execute(
            "SELECT snap_id, snap_time FROM snapshots WHERE target_id=%s ORDER BY snap_time DESC LIMIT 2",
            (target_id,)).fetchall()
        if len(ids) < 2:
            return {"available": False}
        to_id, to_t = ids[0]
        from_id, from_t = ids[1]
        rows = conn.execute("""
            SELECT b.queryid, b.datname,
                   left(regexp_replace(b.query,'\\s+',' ','g'),120) q,
                   (b.metrics->>'total_exec_time')::float - COALESCE((a.metrics->>'total_exec_time')::float,0) d_exec,
                   (b.metrics->>'calls')::bigint - COALESCE((a.metrics->>'calls')::bigint,0) d_calls,
                   (b.metrics->>'rows')::bigint - COALESCE((a.metrics->>'rows')::bigint,0) d_rows
            FROM snap_pgss b
            LEFT JOIN snap_pgss a
              ON a.snap_id=%s AND a.queryid=b.queryid AND a.userid=b.userid
             AND a.dbid=b.dbid AND a.toplevel=b.toplevel
            WHERE b.snap_id=%s
        """, (from_id, to_id)).fetchall()
    items = []
    for qid, dn, q, d_exec, d_calls, d_rows in rows:
        if (d_calls or 0) <= 0 and (d_exec or 0) <= 0:
            continue
        items.append({"query_id": qid, "datname": dn or "?", "query": q,
                      "exec_ms": round(d_exec or 0, 1),
                      "calls": d_calls or 0, "rows": d_rows or 0,
                      "ms_per_call": round((d_exec or 0) / d_calls, 2) if d_calls else 0})
    total = sum(i["exec_ms"] for i in items) or 1.0
    for i in items:
        i["pct"] = round(100.0 * i["exec_ms"] / total, 1)
    items.sort(key=lambda x: x["exec_ms"], reverse=True)
    return {"available": True, "from": from_t.isoformat(), "to": to_t.isoformat(),
            "total_exec_ms": round(total, 1), "items": items[:limit]}
