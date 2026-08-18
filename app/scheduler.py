"""Background collection scheduler: ASH sampling + periodic snapshots per target."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler

import repo
from core import target_conn, collect_ash, collect_sys, collect_pgss

log = logging.getLogger("lakebase_snapper.scheduler")

SAMPLE_INTERVAL = float(os.environ.get("SAMPLE_INTERVAL_SECS", "2"))
SNAPSHOT_INTERVAL = float(os.environ.get("SNAPSHOT_INTERVAL_SECS", "300"))
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "7"))
INCLUDE_IDLE_IN_TXN = os.environ.get("INCLUDE_IDLE_IN_TXN", "false").lower() == "true"

_scheduler: BackgroundScheduler | None = None


def _sample_tick():
    for t in repo.list_targets(only_enabled=True):
        try:
            tc = target_conn(t["endpoint"], t["dbname"])
            rows = collect_ash(tc, INCLUDE_IDLE_IN_TXN)
            repo.insert_ash(t["id"], rows)
        except Exception as e:
            log.warning("ASH sample failed for %s: %s", t["label"], e)


def _snapshot_tick():
    for t in repo.list_targets(only_enabled=True):
        try:
            tc = target_conn(t["endpoint"], t["dbname"])
            sys_obj = collect_sys(tc)
            pgss_rows, _ = collect_pgss(tc)
            snap_time = datetime.now(timezone.utc)
            snap_id = repo.insert_snapshot(t["id"], snap_time, "auto", sys_obj, pgss_rows)
            log.info("snapshot %s for %s (%d statements)", snap_id, t["label"], len(pgss_rows))
        except Exception as e:
            log.warning("snapshot failed for %s: %s", t["label"], e)


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
