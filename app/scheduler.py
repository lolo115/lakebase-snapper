"""Background collection scheduler: ASH sampling + periodic snapshots per target."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler

import repo
from core import target_conn, collect_ash, collect_sys, collect_pgss, endpoint_cu

log = logging.getLogger("lakebase_snapper.scheduler")

SAMPLE_INTERVAL = float(os.environ.get("SAMPLE_INTERVAL_SECS", "2"))
SNAPSHOT_INTERVAL = float(os.environ.get("SNAPSHOT_INTERVAL_SECS", "300"))
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "7"))
INCLUDE_IDLE_IN_TXN = os.environ.get("INCLUDE_IDLE_IN_TXN", "false").lower() == "true"
# A target with dbname '*' is instance-wide (all databases). We must still connect to a
# real database to read the shared stats views; use this one (needs pg_stat_statements).
INSTANCE_SENTINEL = "*"
INSTANCE_CONNECT_DB = os.environ.get("INSTANCE_CONNECT_DB", "postgres")

_scheduler: BackgroundScheduler | None = None


def _target_conn_scope(t):
    """Return (TargetConn, scoped) for a target, handling the instance-wide '*' sentinel."""
    scoped = t["dbname"] != INSTANCE_SENTINEL
    connect_db = t["dbname"] if scoped else INSTANCE_CONNECT_DB
    return target_conn(t["endpoint"], connect_db), scoped


def _sample_tick():
    for t in repo.list_targets(only_enabled=True):
        try:
            tc, scoped = _target_conn_scope(t)
            rows = collect_ash(tc, INCLUDE_IDLE_IN_TXN, scoped)
            repo.insert_ash(t["id"], rows)
        except Exception as e:
            log.warning("ASH sample failed for %s: %s", t["label"], e)


def _snapshot_target(t, label="auto"):
    """Capture one snapshot for a single target. Returns (snap_id, n_statements)."""
    tc, scoped = _target_conn_scope(t)
    sys_obj = dict(collect_sys(tc, scoped) or {})
    # Capture the configured autoscaling CU bounds at snapshot time so the
    # dashboard can plot the provisioned CU ceiling over the window.
    cu = endpoint_cu(t["endpoint"])
    if cu:
        sys_obj["cu"] = cu
    pgss_rows, _ = collect_pgss(tc, scoped)
    snap_time = datetime.now(timezone.utc)
    snap_id = repo.insert_snapshot(t["id"], snap_time, label, sys_obj, pgss_rows)
    return snap_id, len(pgss_rows)


def _snapshot_tick():
    for t in repo.list_targets(only_enabled=True):
        try:
            snap_id, n = _snapshot_target(t, "auto")
            log.info("snapshot %s for %s (%d statements)", snap_id, t["label"], n)
        except Exception as e:
            log.warning("snapshot failed for %s: %s", t["label"], e)


def snapshot_now():
    """Take a snapshot for every enabled target right now (on-demand, from the UI).
    Returns a per-target result list so the caller can report success/failure."""
    results = []
    for t in repo.list_targets(only_enabled=True):
        try:
            snap_id, n = _snapshot_target(t, "manual")
            log.info("manual snapshot %s for %s (%d statements)", snap_id, t["label"], n)
            results.append({"target_id": t["id"], "label": t["label"],
                            "snap_id": snap_id, "statements": n, "ok": True})
        except Exception as e:
            log.warning("manual snapshot failed for %s: %s", t["label"], e)
            results.append({"target_id": t["id"], "label": t["label"],
                            "ok": False, "error": f"{type(e).__name__}: {e}"})
    return results


def _purge_tick():
    try:
        repo.purge_older_than(RETENTION_DAYS)
    except Exception as e:
        log.warning("purge failed: %s", e)


def start():
    global _scheduler
    if _scheduler:
        return _scheduler
    sch = BackgroundScheduler(timezone="UTC")
    sch.add_job(_sample_tick, "interval", seconds=SAMPLE_INTERVAL,
                max_instances=1, coalesce=True, id="ash")
    sch.add_job(_snapshot_tick, "interval", seconds=SNAPSHOT_INTERVAL,
                max_instances=1, coalesce=True, id="snap", next_run_time=datetime.now(timezone.utc))
    sch.add_job(_purge_tick, "interval", hours=6, id="purge")
    sch.start()
    _scheduler = sch
    log.info("scheduler started: sample=%.1fs snapshot=%.0fs retention=%dd",
             SAMPLE_INTERVAL, SNAPSHOT_INTERVAL, RETENTION_DAYS)
    return sch


def status():
    if not _scheduler:
        return {"running": False}
    return {"running": True,
            "sample_interval_secs": SAMPLE_INTERVAL,
            "snapshot_interval_secs": SNAPSHOT_INTERVAL,
            "retention_days": RETENTION_DAYS,
            "jobs": [{"id": j.id, "next_run": j.next_run_time.isoformat() if j.next_run_time else None}
                     for j in _scheduler.get_jobs()]}


def shutdown():
    if _scheduler:
        _scheduler.shutdown(wait=False)
