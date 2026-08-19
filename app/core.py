"""Core connection + sampling logic for the lakebase_snapper Databricks App.

Two kinds of Postgres connection:

  * Repository pool  -> the dedicated `lakebase_snapper` database (schema `snap`) where all
    snapshots / ASH samples are persisted. Uses the psycopg_pool + OAuthConnection
    pattern from the Databricks Apps skill (fresh token per connection, recycle < 1h).

  * Target connections -> each *monitored* Lakebase endpoint. The app's identity needs
    pg_monitor on the target so pg_stat_activity / pg_stat_statements expose other
    sessions. Tokens are minted per endpoint and refreshed before expiry.

Dual-mode auth: inside a Databricks App, WorkspaceClient() uses the injected service
principal; locally it falls back to the DATABRICKS_PROFILE CLI profile.
"""

from __future__ import annotations

import os
import threading
import time
from decimal import Decimal

import psycopg
from psycopg_pool import ConnectionPool
from databricks.sdk import WorkspaceClient

IS_APP = bool(os.environ.get("DATABRICKS_APP_NAME") or os.environ.get("DATABRICKS_CLIENT_ID"))
TOKEN_TTL_SECONDS = 45 * 60

# In an App, WorkspaceClient() uses the injected SP env credentials. Only force a CLI
# profile when we're explicitly local AND a profile was named — never fall back to a
# "DEFAULT" profile that won't exist in the App container (that would crash at import).
_prof = os.environ.get("DATABRICKS_CONFIG_PROFILE") or os.environ.get("DATABRICKS_PROFILE")
if _prof and not IS_APP:
    _w = WorkspaceClient(profile=_prof)
else:
    _w = WorkspaceClient()


def workspace_client() -> WorkspaceClient:
    return _w


def mint_token(endpoint: str) -> str:
    """Mint a short-lived Lakebase OAuth token for a specific endpoint path."""
    return _w.postgres.generate_database_credential(endpoint=endpoint).token


def endpoint_cu(endpoint: str):
    """Configured autoscaling CU bounds for an endpoint, read at call time so config
    changes (up/downscale) are captured. Returns {min, max, state} or None on failure.

    Note: the platform exposes the *configured* autoscaling bounds, not the sub-minute
    live-allocated CU, so this reflects the provisioned CU range."""
    try:
        st = _w.postgres.get_endpoint(name=endpoint).status
        return {"min": float(st.autoscaling_limit_min_cu),
                "max": float(st.autoscaling_limit_max_cu),
                "state": str(getattr(st, "current_state", "") or "")}
    except Exception:
        return None


# --------------------------------------------------------------------------------------
# Repository connection pool (the lakebase_snapper database)
# --------------------------------------------------------------------------------------

REPO_ENDPOINT = os.environ["REPO_ENDPOINT"]         # projects/.../endpoints/primary
REPO_HOST = os.environ["PGHOST"]
REPO_PORT = os.environ.get("PGPORT", "5432")
REPO_DB = os.environ.get("REPO_DBNAME", "lakebase_snapper")
REPO_USER = os.environ["PGUSER"]                    # SP client-id (remote) or email (local)
REPO_SSLMODE = os.environ.get("PGSSLMODE", "require")
REPO_SCHEMA = os.environ.get("REPO_SCHEMA", "snap")


class RepoConnection(psycopg.Connection):
    """psycopg connection that mints a fresh OAuth token for the repo endpoint,
    and pins the search_path to the repository schema."""

    @classmethod
    def connect(cls, conninfo="", **kwargs):
        kwargs["password"] = mint_token(REPO_ENDPOINT)
        conn = super().connect(conninfo, **kwargs)
        conn.execute(f"SET search_path TO {REPO_SCHEMA}, public")
        conn.execute("SET application_name = 'lakebase_snapper_app/repo'")
        return conn


# min_size=0 + a short max_idle so that when collection is stopped and the dashboard is
# idle, the repo connections drop and the endpoint can scale to zero (the repo DB lives on
# the monitored endpoint). During collection the 2s ASH inserts keep the pool warm.
repo_pool = ConnectionPool(
    conninfo=(f"dbname={REPO_DB} user={REPO_USER} host={REPO_HOST} "
              f"port={REPO_PORT} sslmode={REPO_SSLMODE}"),
    connection_class=RepoConnection,
    min_size=0, max_size=8, max_lifetime=TOKEN_TTL_SECONDS, max_idle=120.0,
    open=False,
)


# --------------------------------------------------------------------------------------
# Target connections (monitored endpoints) - one persistent conn per (endpoint, dbname)
# --------------------------------------------------------------------------------------

class TargetConn:
    """A persistent, auto-refreshing connection to a monitored Lakebase endpoint."""

    def __init__(self, endpoint: str, dbname: str):
        self.endpoint = endpoint
        self.dbname = dbname
        self._conn = None
        self._token_at = 0.0
        self._lock = threading.Lock()

    def _connect(self):
        conn = psycopg.connect(
            host=REPO_HOST if self.endpoint == REPO_ENDPOINT else _host_for(self.endpoint),
            port=REPO_PORT, dbname=self.dbname, user=REPO_USER,
            password=mint_token(self.endpoint), sslmode=REPO_SSLMODE, autocommit=True,
        )
        conn.execute("SET application_name = 'lakebase_snapper_app/collector'")
        self._token_at = time.time()
        return conn

    def execute(self, sql, params=None):
        with self._lock:
            if (self._conn is None or self._conn.closed
                    or (time.time() - self._token_at) > TOKEN_TTL_SECONDS):
                self._reset()
            try:
                cur = self._conn.execute(sql, params or ())
                return cur.fetchall() if cur.description is not None else []
            except (psycopg.OperationalError, psycopg.InterfaceError):
                self._reset()
                cur = self._conn.execute(sql, params or ())
                return cur.fetchall() if cur.description is not None else []

    def _reset(self):
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            pass
        self._conn = self._connect()

    def close(self):
        """Drop the persistent connection (reopened lazily on next use)."""
        with self._lock:
            try:
                if self._conn and not self._conn.closed:
                    self._conn.close()
            except Exception:
                pass
            self._conn = None


_host_cache: dict[str, str] = {}


def _host_for(endpoint: str) -> str:
    """Resolve the connection host for a monitored endpoint (cached)."""
    if endpoint in _host_cache:
        return _host_cache[endpoint]
    branch = endpoint.rsplit("/endpoints/", 1)[0]
    ep_name = endpoint.rsplit("/", 1)[1]
    proj = branch.split("/branches/")[0].split("/")[-1]
    br = branch.split("/branches/")[1]
    eps = _w.postgres.list_endpoints(name=f"projects/{proj}/branches/{br}")
    for ep in eps:
        if ep.name.endswith("/" + ep_name):
            host = ep.status.hosts.host
            _host_cache[endpoint] = host
            return host
    raise RuntimeError(f"endpoint {endpoint} not found")


_target_conns: dict[tuple, TargetConn] = {}
_target_lock = threading.Lock()


def target_conn(endpoint: str, dbname: str) -> TargetConn:
    key = (endpoint, dbname)
    with _target_lock:
        if key not in _target_conns:
            _target_conns[key] = TargetConn(endpoint, dbname)
        return _target_conns[key]


def close_target_conns():
    """Close every persistent target connection, so a stopped collector no longer holds
    monitored endpoints awake (lets them scale to zero)."""
    with _target_lock:
        for tc in _target_conns.values():
            tc.close()


# --------------------------------------------------------------------------------------
# Collection SQL (ported from the lakebase_snapper CLI)
# --------------------------------------------------------------------------------------

# {DATNAME} / {DBFILTER} are substituted per target: scoped to one database, or left
# instance-wide (all databases) for a target whose dbname is the '*' sentinel.
ASH_SQL_TMPL = """
SELECT statement_timestamp(),
       pid, usename, application_name, host(client_addr), backend_type, state,
       CASE WHEN wait_event IS NULL THEN 'CPU' ELSE wait_event_type END AS wait_class,
       wait_event, query_id::text, left(query, 4000),
       EXTRACT(EPOCH FROM (clock_timestamp() - xact_start)),
       EXTRACT(EPOCH FROM (clock_timestamp() - query_start))
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND coalesce(application_name,'') NOT LIKE 'lakebase_snapper_app/%%'  -- doubled percent (parameterized); ignore our own connections
  {DATNAME}
  AND ( state = 'active'
        OR (%(idle_in_txn)s AND state = 'idle in transaction')
        OR (%(include_idle)s AND state = 'idle') )
"""

SYS_SQL_TMPL = """
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
            'deadlocks', COALESCE(sum(deadlocks),0))
         FROM pg_stat_database WHERE {DBFILTER})
)
"""

_PGSS_CANDIDATE_COLS = [
    "calls", "total_exec_time", "total_plan_time", "rows", "plans",
    "shared_blks_hit", "shared_blks_read", "shared_blks_dirtied", "shared_blks_written",
    "local_blks_hit", "local_blks_read", "temp_blks_read", "temp_blks_written",
    "blk_read_time", "blk_write_time", "shared_blk_read_time", "shared_blk_write_time",
    "wal_records", "wal_fpi", "wal_bytes",
]


def collect_ash(tc: TargetConn, include_idle_in_txn: bool, scoped: bool = True,
                include_idle: bool = False):
    datname = "AND datname = current_database()" if scoped else ""
    rows = tc.execute(ASH_SQL_TMPL.replace("{DATNAME}", datname),
                      {"idle_in_txn": include_idle_in_txn, "include_idle": include_idle})
    out = []
    for r in rows:
        r = [float(v) if isinstance(v, Decimal) else v for v in r]
        out.append(tuple(r))
    return out


def collect_sys(tc: TargetConn, scoped: bool = True):
    dbf = "datname = current_database()" if scoped else "datname IS NOT NULL"
    return tc.execute(SYS_SQL_TMPL.replace("{DBFILTER}", dbf))[0][0]


def collect_pgss(tc: TargetConn, scoped: bool = True):
    present = {r[0] for r in tc.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='pg_stat_statements'")}
    if not present:
        # pg_stat_statements isn't installed in this database (the view is per-database).
        # Skip statement capture gracefully so the snapshot (system stats + CU) still lands.
        return [], []
    cols = [c for c in _PGSS_CANDIDATE_COLS if c in present]
    toplevel = "toplevel" if "toplevel" in present else "true AS toplevel"
    dbid_filter = ("AND dbid = (SELECT oid FROM pg_database WHERE datname = current_database())"
                   if scoped else "")
    sql = (f"SELECT queryid::text, userid, dbid, {toplevel}, left(query,4000), "
           f"{', '.join(cols)} FROM pg_stat_statements "
           "WHERE queryid IS NOT NULL "
           f"{dbid_filter}")
    out = []
    for r in tc.execute(sql):
        metrics = {c: (float(r[5 + i]) if isinstance(r[5 + i], Decimal) else r[5 + i])
                   for i, c in enumerate(cols)}
        out.append((r[0], r[1], r[2], bool(r[3]), r[4], metrics))
    return out, cols
