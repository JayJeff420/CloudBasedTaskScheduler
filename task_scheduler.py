# task_scheduler.py
import os
import tempfile
import json
import time
import sqlite3
from datetime import datetime, timezone

import pytest
from app import app, init_db, get_sqlite_conn, is_valid_cron, scheduler

@pytest.fixture(scope="module")
def client():
    tmp_db = "file::memory:?cache=shared"
    os.environ["SCHED_DB_PATH"] = tmp_db
    os.environ["SINGLE_INSTANCE"] = "true"
    init_db()
    with app.test_client() as c:
        yield c

def test_cron_validation():
    assert is_valid_cron("*/5 * * * *")
    assert is_valid_cron("0 0 * * *")
    assert not is_valid_cron("")
    assert not is_valid_cron("this is invalid")
    assert is_valid_cron("*/5 * * * * *")  

def test_create_update_delete_task(client):
    rv = client.post("/tasks", json={"name": "t1", "schedule": "*/1 * * * *", "description": "test"})
    assert rv.status_code == 201
    data = rv.get_json()
    tid = data["id"]
    rv = client.get("/tasks")
    assert rv.status_code == 200
    tasks = rv.get_json()
    assert any(t["id"] == tid for t in tasks)
    rv = client.put(f"/tasks/{tid}", json={"schedule": "*/2 * * * *"})
    assert rv.status_code == 200
    time.sleep(0.5)
    job = scheduler.get_job(f"task-{tid}")
    assert job is not None
    rv = client.delete(f"/tasks/{tid}")
    assert rv.status_code == 200
    time.sleep(0.2)
    job = scheduler.get_job(f"task-{tid}")
    assert job is None

def test_task_execution_and_retry(client):
    rv = client.post("/tasks", json={"name": "fail-test", "schedule": "*/1 * * * *", "description": "will fail", "max_retries": 2, "retry_delay_seconds": 1})
    assert rv.status_code == 201
    tid = rv.get_json()["id"]
    from app import execute_task, get_sqlite_conn
    execute_task(tid)
    conn = get_sqlite_conn()
    cur = conn.cursor()
    cur.execute("SELECT retry_count, status FROM tasks WHERE id=?", (tid,))
    row = cur.fetchone()
    assert row["retry_count"] == 1
    assert row["status"] == "enabled"
    execute_task(tid)  
    cur.execute("SELECT retry_count, status FROM tasks WHERE id=?", (tid,))
    row = cur.fetchone()
    assert row["status"] in ("dead_letter", "enabled")
    conn.close()
