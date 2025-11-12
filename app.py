# app.py
import os
import time
import logging
import threading
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Optional

from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

# -----------------------
# Configuration (env)
# -----------------------
DB_PATH = os.environ.get("SCHED_DB_PATH", "tasks.db")  
USE_JOBSTORE = os.environ.get("USE_JOBSTORE", "false").lower() == "true"  
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
CRON_FIELD_ALLOWED = (5, 6)
DEFAULT_MAX_RETRIES = int(os.environ.get("DEFAULT_MAX_RETRIES", "3"))
DEFAULT_RETRY_DELAY = int(os.environ.get("DEFAULT_RETRY_DELAY", "60"))  
SINGLE_INSTANCE = os.environ.get("SINGLE_INSTANCE", "true").lower() == "true"  

# -----------------------
# Logging (structured-ish)
# -----------------------
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger("task-scheduler")

# -----------------------
# Metrics
# -----------------------
MET_SUCCESS = Counter("task_executions_success_total", "Successful task executions")
MET_FAILURE = Counter("task_executions_failure_total", "Failed task executions")
MET_RETRIES = Counter("task_execution_retries_total", "Task execution retries")

# -----------------------
# DB helpers
# -----------------------
def get_sqlite_conn(db_path=DB_PATH, timeout=30):
    conn = sqlite3.connect(db_path, timeout=timeout, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    if SINGLE_INSTANCE and (DB_PATH.endswith(".db") or DB_PATH == "tasks.db"):
        conn = get_sqlite_conn()
        with conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    schedule TEXT NOT NULL,         -- cron expression (5 or 6 fields)
                    last_executed TEXT,
                    max_retries INTEGER DEFAULT ?,
                    retry_count INTEGER DEFAULT 0,
                    retry_delay_seconds INTEGER DEFAULT ?,
                    status TEXT DEFAULT 'enabled',  -- enabled | disabled | dead_letter
                    locked_at TEXT DEFAULT NULL     -- ISO timestamp when a worker acquired lock
                )
            """, (DEFAULT_MAX_RETRIES, DEFAULT_RETRY_DELAY))
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dead_letters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER,
                    failed_at TEXT,
                    last_error TEXT
                )
            """)
        conn.close()
    else:
        conn = get_sqlite_conn() if DB_PATH.endswith(".db") else None
        if conn:
            with conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS tasks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        description TEXT,
                        schedule TEXT NOT NULL,
                        last_executed TEXT,
                        max_retries INTEGER DEFAULT ?,
                        retry_count INTEGER DEFAULT 0,
                        retry_delay_seconds INTEGER DEFAULT ?,
                        status TEXT DEFAULT 'enabled',
                        locked_at TEXT DEFAULT NULL
                    )
                """, (DEFAULT_MAX_RETRIES, DEFAULT_RETRY_DELAY))
            conn.close()

# -----------------------
# App + Scheduler init
# -----------------------
app = Flask(__name__)

def create_scheduler():
    if USE_JOBSTORE:
        jobstores = {'default': SQLAlchemyJobStore(url=DB_PATH)}
        sched = BackgroundScheduler(jobstores=jobstores, timezone=timezone.utc)
        logger.info("Using SQLAlchemyJobStore at %s", DB_PATH)
    else:
        sched = BackgroundScheduler(timezone=timezone.utc)
        logger.info("Using in-memory APScheduler jobstore (no persistence)")
    sched.start()
    return sched

scheduler = create_scheduler()

# -----------------------
# Core logic: scheduling + execution
# -----------------------
def is_valid_cron(expr: str) -> bool:
    if not expr or not isinstance(expr, str):
        return False
    parts = expr.split()
    return len(parts) in CRON_FIELD_ALLOWED

def schedule_task_in_scheduler(task_id: int, cron_expr: str):
    job_id = f"task-{task_id}"
    try:
        trigger = CronTrigger.from_crontab(cron_expr, timezone=timezone.utc)
    except Exception as e:
        logger.exception("Invalid cron expression for task %s: %s", task_id, cron_expr)
        raise

    scheduler.add_job(
        func=execute_task,
        trigger=trigger,
        args=[task_id],
        id=job_id,
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=60
    )
    logger.info("Scheduled job %s -> cron='%s'", job_id, cron_expr)

def unschedule_task_in_scheduler(task_id: int):
    job_id = f"task-{task_id}"
    job = scheduler.get_job(job_id)
    if job:
        scheduler.remove_job(job_id)
        logger.info("Removed job %s from scheduler", job_id)
    else:
        logger.debug("Job %s not present in scheduler", job_id)

def acquire_lock(conn: sqlite3.Connection, task_id: int, lock_timeout_seconds=300) -> bool:
    """
    Optimistic locking: set locked_at if not set or expired.
    Returns True if lock acquired.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    cur = conn.cursor()
    cur.execute("SELECT locked_at FROM tasks WHERE id=?", (task_id,))
    row = cur.fetchone()
    if not row:
        return False
    locked_at = row["locked_at"]
    if locked_at:
        try:
            locked_time = datetime.fromisoformat(locked_at)
        except Exception:
            locked_time = None
        if locked_time and (datetime.now(timezone.utc) - locked_time).total_seconds() < lock_timeout_seconds:
            return False
    cur.execute("UPDATE tasks SET locked_at=? WHERE id=?", (now_iso, task_id))
    conn.commit()
    return True

def release_lock(conn: sqlite3.Connection, task_id: int):
    conn.execute("UPDATE tasks SET locked_at=NULL WHERE id=?", (task_id,))
    conn.commit()

def mark_dead_letter(conn: sqlite3.Connection, task_id: int, error_text: str):
    now_iso = datetime.now(timezone.utc).isoformat()
    conn.execute("INSERT INTO dead_letters (task_id, failed_at, last_error) VALUES (?,?,?)", (task_id, now_iso, error_text))
    conn.execute("UPDATE tasks SET status='dead_letter' WHERE id=?", (task_id,))
    conn.commit()

def execute_task(task_id: int):
    """
    Core runner executed by APScheduler. Implements:
      - idempotency via last_executed vs schedule (caller must ensure cron triggers)
      - optimistic locking via locked_at
      - retry/backoff logic recorded in DB
      - dead-lettering after max_retries
    """
    logger.info("Runner invoked for task %s", task_id)
    conn = get_sqlite_conn()
    try:
        with conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM tasks WHERE id=?", (task_id,))
            row = cur.fetchone()
            if not row:
                logger.warning("Task %s not found in DB", task_id)
                return

            if row["status"] != "enabled":
                logger.info("Task %s is not enabled (status=%s); skipping", task_id, row["status"])
                return
            locked = acquire_lock(conn, task_id)
            if not locked:
                logger.info("Task %s is currently locked by another worker; skipping", task_id)
                return
            last_exec = row["last_executed"]
            if last_exec:
                try:
                    last_exec_dt = datetime.fromisoformat(last_exec)
                    if (datetime.now(timezone.utc) - last_exec_dt).total_seconds() < 1:
                        logger.info("Task %s executed too recently; skipping to avoid duplicate", task_id)
                        release_lock(conn, task_id)
                        return
                except Exception:
                    pass

            try:
                logger.info("Executing Task %s: %s", task_id, row["name"])
                if "fail" in (row["name"] or "").lower():
                    raise RuntimeError("simulated failure for demo")
                now_iso = datetime.now(timezone.utc).isoformat()
                conn.execute("UPDATE tasks SET last_executed=?, retry_count=0 WHERE id=?", (now_iso, task_id))
                conn.commit()
                MET_SUCCESS.inc()
                logger.info("Task %s executed successfully", task_id)
            except Exception as e:
                MET_FAILURE.inc()
                retry_count = row["retry_count"] or 0
                max_retries = row["max_retries"] or DEFAULT_MAX_RETRIES
                retry_delay = row["retry_delay_seconds"] or DEFAULT_RETRY_DELAY
                retry_count += 1
                conn.execute("UPDATE tasks SET retry_count=? WHERE id=?", (retry_count, task_id))
                conn.commit()
                MET_RETRIES.inc()
                logger.exception("Task %s failed on attempt %s: %s", task_id, retry_count, str(e))
                if retry_count >= max_retries:
                    mark_dead_letter(conn, task_id, str(e))
                    logger.error("Task %s moved to dead-letter after %s attempts", task_id, retry_count)
                else:
                    run_at = datetime.now(timezone.utc) + timedelta(seconds=retry_delay)
                    tmp_job_id = f"retry-{task_id}-{int(time.time())}"
                    scheduler.add_job(execute_task, trigger='date', run_date=run_at, args=[task_id], id=tmp_job_id, replace_existing=False)
                    logger.info("Scheduled retry job %s for task %s at %s", tmp_job_id, task_id, run_at.isoformat())
            finally:
                release_lock(conn, task_id)
    finally:
        conn.close()

# -----------------------
# Sync / Polling
# -----------------------
_last_known = {}

def refresh_scheduler_from_db():
    """
    Idempotent synchronization:
      - Reads tasks from DB
      - Ensures scheduler contains scheduled jobs for enabled tasks
      - Removes scheduler jobs for tasks no longer present or disabled
    """
    conn = get_sqlite_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, schedule, status FROM tasks")
        rows = cur.fetchall()
        db_task_ids = set()
        for r in rows:
            tid = r["id"]
            db_task_ids.add(tid)
            status = r["status"]
            cron_expr = r["schedule"]
            job_id = f"task-{tid}"
            if status != "enabled":
                unschedule_task_in_scheduler(tid)
                continue
            if not is_valid_cron(cron_expr):
                logger.warning("Invalid cron for task %s: %s", tid, cron_expr)
                unschedule_task_in_scheduler(tid)
                continue
            try:
                schedule_task_in_scheduler(tid, cron_expr)
            except Exception:
                logger.exception("Failed to schedule task %s during refresh", tid)
        for job in list(scheduler.get_jobs()):
            jid = job.id
            if jid.startswith("task-"):
                try:
                    tid = int(jid.split("-", 1)[1])
                    if tid not in db_task_ids:
                        logger.info("Removing scheduler job for non-existent DB task %s", tid)
                        scheduler.remove_job(jid)
                except Exception:
                    continue
    finally:
        conn.close()

def start_polling_loop(interval=POLL_INTERVAL_SECONDS):
    def _loop():
        while True:
            try:
                refresh_scheduler_from_db()
            except Exception:
                logger.exception("Polling sync loop error")
            time.sleep(interval)
    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    logger.info("Started DB polling loop (interval=%s seconds)", interval)

# -----------------------
# API endpoints
# -----------------------
@app.route("/health")
def health():
    return jsonify({"status": "ok", "utc_now": datetime.now(timezone.utc).isoformat()}), 200

@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

@app.route("/tasks", methods=["POST"])
def create_task():
    payload = request.json or {}
    name = payload.get("name")
    schedule = (payload.get("schedule") or "").strip()
    description = payload.get("description", "")
    max_retries = int(payload.get("max_retries", DEFAULT_MAX_RETRIES))
    retry_delay_seconds = int(payload.get("retry_delay_seconds", DEFAULT_RETRY_DELAY))
    if not name or not schedule:
        return jsonify({"error": "name and schedule required"}), 400
    if not is_valid_cron(schedule):
        return jsonify({"error": "invalid cron format"}), 400

    conn = get_sqlite_conn()
    try:
        with conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO tasks (name, description, schedule, max_retries, retry_delay_seconds, status)
                VALUES (?, ?, ?, ?, ?, 'enabled')
            """, (name, description, schedule, max_retries, retry_delay_seconds))
            task_id = cur.lastrowid
            conn.commit()
        schedule_task_in_scheduler(task_id, schedule)
        return jsonify({"id": task_id, "name": name, "schedule": schedule}), 201
    finally:
        conn.close()

@app.route("/tasks/<int:task_id>", methods=["PUT"])
def update_task(task_id):
    payload = request.json or {}
    name = payload.get("name")
    schedule = payload.get("schedule")
    description = payload.get("description")
    status = payload.get("status")  
    max_retries = payload.get("max_retries")
    retry_delay_seconds = payload.get("retry_delay_seconds")

    conn = get_sqlite_conn()
    try:
        with conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM tasks WHERE id=?", (task_id,))
            existing = cur.fetchone()
            if not existing:
                return jsonify({"error": "task not found"}), 404

            updates = []
            params = []
            if name is not None:
                updates.append("name=?"); params.append(name)
            if description is not None:
                updates.append("description=?"); params.append(description)
            if schedule is not None:
                if not is_valid_cron(schedule):
                    return jsonify({"error": "invalid cron format"}), 400
                updates.append("schedule=?"); params.append(schedule)
            if status is not None:
                if status not in ("enabled", "disabled"):
                    return jsonify({"error": "invalid status"}), 400
                updates.append("status=?"); params.append(status)
            if max_retries is not None:
                updates.append("max_retries=?"); params.append(int(max_retries))
            if retry_delay_seconds is not None:
                updates.append("retry_delay_seconds=?"); params.append(int(retry_delay_seconds))
            if updates:
                sql = "UPDATE tasks SET " + ", ".join(updates) + " WHERE id=?"
                params.append(task_id)
                cur.execute(sql, params)
                conn.commit()
        if schedule is not None or status is not None:
            cur = get_sqlite_conn().cursor()
            cur.execute("SELECT schedule, status FROM tasks WHERE id=?", (task_id,))
            row = cur.fetchone()
            cron_expr = row[0]
            current_status = row[1]
            if current_status != "enabled":
                unschedule_task_in_scheduler(task_id)
            else:
                schedule_task_in_scheduler(task_id, cron_expr)
        return jsonify({"id": task_id}), 200
    finally:
        conn.close()

@app.route("/tasks/<int:task_id>", methods=["DELETE"])
def delete_task(task_id):
    conn = get_sqlite_conn()
    try:
        with conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM tasks WHERE id=?", (task_id,))
            conn.commit()
        unschedule_task_in_scheduler(task_id)
        return jsonify({"deleted": task_id}), 200
    finally:
        conn.close()

@app.route("/tasks", methods=["GET"])
def list_tasks():
    conn = get_sqlite_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM tasks")
        rows = cur.fetchall()
        tasks = []
        for r in rows:
            tasks.append({k: r[k] for k in r.keys()})
        return jsonify(tasks)
    finally:
        conn.close()

# -----------------------
# Boot sequence
# -----------------------
if __name__ == "__main__":
    init_db()
    refresh_scheduler_from_db()
    start_polling_loop()
    try:
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler")
        scheduler.shutdown(wait=False)
