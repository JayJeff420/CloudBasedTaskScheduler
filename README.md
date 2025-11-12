# Cloud Task Scheduler Service

A lightweight **cloud-based task scheduler** built using **Flask**, **APScheduler**, and **SQLite/Postgres**.  
It supports cron-based scheduling, retry and dead-letter handling, health and metrics endpoints, and automatic synchronization between a database and an in-memory scheduler.
This scheduler is production-grade for single-instance services and easily extensible for distributed environments.
It combines simplicity (Flask + SQLite) with cloud readiness (Postgres + Prometheus + JobStore).

Ideal for:
- Lightweight single-instance deployments (e.g., edge devices, small servers)
- Multi-instance or containerized environments using PostgreSQL for persistent job storage


##  Features

 **Cron-based Scheduling**
- Supports 5-field and 6-field cron expressions (`*/5 * * * *` or `*/5 * * * * *`)

 **Task Management API**
- Create, update, delete, and list scheduled tasks via REST API

 **Retry & Dead Letter Handling**
- Automatic retry with configurable retry count and delay
- Failed tasks beyond limit are moved to `dead_letters` table

 **Locking & Idempotency**
- Prevents concurrent execution of the same task (`locked_at`)
- Basic last-run-time check to avoid duplicate executions

 **Persistence Options**
- **SQLite (default):** single-instance mode with WAL enabled
- **Postgres + SQLAlchemyJobStore:** persistent and multi-instance ready

 **Health & Metrics**
- `/health` for uptime checks  
- `/metrics` for Prometheus-compatible metrics

 **Automatic DB Polling**
- Keeps APScheduler jobs synchronized with database every 30 seconds

 **Structured Logging**
- JSON-friendly logs with clear timestamps and severity levels

 **Unit Tests (pytest)**
- Covers cron parsing, task lifecycle, and retry logic

---

##  Architecture Overview

        ┌──────────────────────────────┐
        │          REST API            │
        │   (Flask + JSON endpoints)   │
        └──────────────┬───────────────┘
                       │
                ┌──────▼───────┐
                │   Database   │
                │ (SQLite/Postgres)
                └──────┬───────┘
                       │
                ┌──────▼──────────────┐
                │ APScheduler Engine  │
                │ - CronTrigger jobs  │
                │ - Retry scheduling  │
                └──────┬──────────────┘
                       │
                ┌──────▼──────────────┐
                │  Task Executor      │
                │ (Locking, retries)  │
                └─────────────────────┘
