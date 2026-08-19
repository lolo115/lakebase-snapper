"""lakebase_snapper Databricks App — FastAPI backend.

Opens the repository pool, initializes the schema, seeds monitored targets from the
SEED_TARGETS env var, starts the background collection scheduler, and serves both the
JSON API and the single-page Plotly dashboard.
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import repo
import scheduler
from core import repo_pool, target_conn, IS_APP

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("lakebase_snapper.app")

HERE = os.path.dirname(os.path.abspath(__file__))


def _seed_targets():
    raw = os.environ.get("SEED_TARGETS")
    if not raw:
        return
    try:
        for t in json.loads(raw):
            repo.upsert_target(t["label"], t["endpoint"],
                               t.get("dbname", "postgres"), t.get("enabled", True))
            log.info("seeded target %s", t["label"])
    except Exception as e:
        log.warning("SEED_TARGETS parse/seed failed: %s", e)


def _startup_db():
    """Open the repo pool + init schema + seed targets. Logs the real error instead of
    crashing the app, so the dashboard and /api/diag stay reachable for diagnosis."""
    # Surface the true connection error directly (the pool would otherwise mask it as a
    # generic PoolTimeout).
    try:
        with repo_pool.connection(timeout=15.0):
            pass
    except Exception:
        log.exception("repo connectivity check FAILED (PGUSER=%s IS_APP=%s host=%s db=%s)",
                      os.environ.get("PGUSER"), os.environ.get("DATABRICKS_APP_NAME") is not None,
                      os.environ.get("PGHOST"), os.environ.get("REPO_DBNAME"))
        return
    try:
        repo.init_schema()
        _seed_targets()
    except Exception:
        log.exception("schema init / seed failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        repo_pool.open()  # non-blocking; connections are established lazily
        _startup_db()
        scheduler.start()
    except Exception:
        log.exception("startup encountered an error; app will serve for diagnosis")
    yield
    try:
        scheduler.shutdown()
        repo_pool.close()
    except Exception:
        pass


app = FastAPI(title="lakebase_snapper — Lakebase performance dashboard", lifespan=lifespan)


# ----------------------------------------------------------------- targets API
class TargetIn(BaseModel):
    label: str
    endpoint: str
    dbname: str = "postgres"
    enabled: bool = True


class EnabledIn(BaseModel):
    enabled: bool


@app.get("/api/targets")
def api_targets():
    return repo.list_targets()


@app.post("/api/targets")
def api_add_target(t: TargetIn):
    tid = repo.upsert_target(t.label, t.endpoint, t.dbname, t.enabled)
    return {"id": tid}


@app.post("/api/targets/{target_id}/enabled")
def api_set_enabled(target_id: int, body: EnabledIn):
    repo.set_enabled(target_id, body.enabled)
    return {"ok": True}


@app.get("/api/status")
def api_status():
    return scheduler.status()


@app.get("/api/diag")
def api_diag():
    """Live connectivity self-test — safe to expose (no secrets), invaluable for debugging."""
    out = {
        "is_app": IS_APP,
        "env": {k: (os.environ.get(k) if k != "PGUSER" else os.environ.get(k))
                for k in ("PGHOST", "PGPORT", "PGUSER", "REPO_DBNAME", "REPO_SCHEMA",
                          "REPO_ENDPOINT", "DATABRICKS_APP_NAME")},
        "repo": {}, "targets": []}
    # Repo connectivity
    try:
        with repo_pool.connection(timeout=15.0) as conn:
            who = conn.execute("SELECT current_user, current_database(), "
                               "current_setting('search_path')").fetchone()
        out["repo"] = {"ok": True, "current_user": who[0], "database": who[1], "search_path": who[2]}
    except Exception as e:
        out["repo"] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
    # Each target: connectivity + pg_monitor visibility
    try:
        targets = repo.list_targets()
    except Exception as e:
        out["targets_error"] = f"{type(e).__name__}: {e}"
        return out
    for t in targets:
        r = {"label": t["label"], "endpoint": t["endpoint"], "dbname": t["dbname"],
             "enabled": t["enabled"]}
        try:
            tc = target_conn(t["endpoint"], t["dbname"])
            mon = tc.execute("SELECT pg_has_role(current_user,'pg_monitor','MEMBER')")[0][0]
            seen = tc.execute("SELECT count(*) FROM pg_stat_activity")[0][0]
            r.update({"ok": True, "pg_monitor": mon, "activity_rows_visible": seen})
        except Exception as e:
            r.update({"ok": False, "error": f"{type(e).__name__}: {e}"})
        out["targets"].append(r)
    return out


# ----------------------------------------------------------------- chart data API
def _require_target(target_id: int):
    if not any(t["id"] == target_id for t in repo.list_targets()):
        raise HTTPException(404, "unknown target_id")


@app.get("/api/summary")
def api_summary(target_id: int, mins: int = 60):
    _require_target(target_id)
    return repo.summary(target_id, mins)


@app.get("/api/ash-timeline")
def api_ash_timeline(target_id: int, mins: int = 60, bucket: int = 10):
    _require_target(target_id)
    return repo.ash_timeline(target_id, mins, bucket)


@app.get("/api/cu-timeline")
def api_cu_timeline(target_id: int, mins: int = 60):
    _require_target(target_id)
    return repo.cu_timeline(target_id, mins)


@app.get("/api/waits")
def api_waits(target_id: int, mins: int = 60):
    _require_target(target_id)
    return repo.wait_mix(target_id, mins)


@app.get("/api/top-sql")
def api_top_sql(target_id: int, mins: int = 60, limit: int = 10):
    _require_target(target_id)
    return repo.top_sql_ash(target_id, mins, limit)


@app.get("/api/load-profile")
def api_load_profile(target_id: int, mins: int = 240):
    _require_target(target_id)
    return repo.load_profile(target_id, mins)


@app.get("/api/snap-diff")
def api_snap_diff(target_id: int, limit: int = 12):
    _require_target(target_id)
    return repo.latest_snap_diff(target_id, limit)


# ----------------------------------------------------------------- static UI
app.mount("/static", StaticFiles(directory=os.path.join(HERE, "static")), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(HERE, "static", "index.html"))


@app.get("/healthz")
def healthz():
    return {"ok": True}
