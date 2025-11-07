#!/bin/bash
# ==================================================================================================
# 🌀 Apache Airflow — Docker Management & Best Practices Guide
# --------------------------------------------------------------------------------------------------
# This guide explains how to manage Apache Airflow using Docker, covering both Development and
# Production setups:
#
# - Airflow-Docker-Dev  → Local testing and debugging
# - Airflow-Docker-Prod → Stable, continuous operation
#
# Each command below includes:
#   ✅ What it does
#   📅 When to use it
#   ⚠️  When *not* to use it
#
# Author: Prasanna
# Environments: Airflow-Docker-Dev / Airflow-Docker-Prod
# ==================================================================================================

# ==================================================================================================
# 🧭 Step 1 — Navigate to Your Airflow Folder
# --------------------------------------------------------------------------------------------------
# ✅ What It Does:
#    Moves you into your Airflow Docker project directory.
# 📅 When to Use:
#    Always before running any docker-compose commands.
# ⚠️ When NOT to Use:
#    If you’re already in the correct directory.
# ==================================================================================================
cd "C:/Users/prasa/Root/Airflow-Docker-Dev"

# ==================================================================================================
# 🧩 Step 2 — Initialize the Airflow Metadata Database
# --------------------------------------------------------------------------------------------------
# ✅ When to Use:
#   - First-time setup (new environment)
#   - After deleting volumes (`docker compose down -v`)
#   - After changing DB location or engine (SQLite → MySQL/Postgres)
#
# 🧠 What It Does:
#   - Creates Airflow’s internal metadata tables (DAGs, users, jobs, etc.)
#   - `--rm` cleans up the temporary container after completion
#
# ⚠️ When NOT to Use:
#   - After Airflow is configured and running (resets metadata)
# ==================================================================================================
docker compose run --rm airflow-init

# ==================================================================================================
# 🟢 Step 3 — Start Airflow Containers
# --------------------------------------------------------------------------------------------------
# Option A — Background Mode (Recommended)
# ✅ When to Use:
#   - Day-to-day operation (especially in production)
#   - You want Airflow to run continuously in the background
#
# 🧠 What It Does:
#   - Starts all containers (webserver, scheduler, etc.) in detached mode
#   - Frees up your terminal
#
# ⚠️ When NOT to Use:
#   - When debugging DAG load or environment issues
# ==================================================================================================
docker compose up -d

# --------------------------------------------------------------------------------------------------
# Option B — Foreground Mode (Debugging)
# ✅ When to Use:
#   - During troubleshooting or first-time setup
#   - When you need logs in real time
#
# ⚠️ When NOT to Use:
#   - In production or long-running sessions (stops when terminal closes)
# --------------------------------------------------------------------------------------------------
# docker compose up

# ==================================================================================================
# 🛑 Step 4 — Stop Airflow Containers
# --------------------------------------------------------------------------------------------------
# ✅ When to Use:
#   - To stop Airflow cleanly and preserve all data/logs
# 🧠 What It Does:
#   - Stops and removes containers but keeps metadata DB, logs, DAGs
# ⚠️ When NOT to Use:
#   - When you expect a full reset (use `down -v` instead)
# ==================================================================================================
docker compose down

# ==================================================================================================
# 💣 Step 5 — Full Environment Reset (Deletes Database)
# --------------------------------------------------------------------------------------------------
# ✅ When to Use:
#   - To completely wipe and reinitialize Airflow (clean start)
# ⚠️ When NOT to Use:
#   - If you want to preserve metadata, DAG runs, or user accounts
# ==================================================================================================
# docker compose down -v

# ==================================================================================================
# 🔁 Step 6 — Restart or Rebuild Airflow
# --------------------------------------------------------------------------------------------------
# Restart Cleanly
# ✅ Use when restarting after small edits to environment variables or compose file.
# ==================================================================================================
# docker compose down
# docker compose up -d

# Rebuild Images (after Dockerfile or dependency changes)
# ✅ Use when adding new Python dependencies or modifying Dockerfile.
# ==================================================================================================
# docker compose build --no-cache
# docker compose up -d

# ==================================================================================================
# 👤 Step 7 — Create an Admin User
# --------------------------------------------------------------------------------------------------
# ✅ When to Use:
#   - After DB initialization (first-time setup or reset)
#   - When adding new admin users
#
# 🧠 What It Does:
#   - Creates user directly inside the running Airflow webserver container
#
# ⚠️ When NOT to Use:
#   - Before DB initialization (fails)
#   - When overwriting existing usernames
# ==================================================================================================
docker exec -it airflow-webserver-prod airflow users create \
  --username xxx \
  --firstname xxxx \
  --lastname xxx \
  --role Admin \
  --email prasanna@xxx.com \
  --password "your_secure_password"

# ==================================================================================================
# 🌐 Step 8 — Access the Airflow Web UI
# --------------------------------------------------------------------------------------------------
# Dev UI:  http://localhost:8080
# Prod UI: http://localhost:8081
# Login with the credentials created above.
# ==================================================================================================

# ==================================================================================================
# 🧹 Step 9 — Maintenance & Inspection
# --------------------------------------------------------------------------------------------------
# Task                     | Command                                              | Description
# --------------------------|------------------------------------------------------|-----------------------------
# 🧩 List containers        | docker ps                                            | Shows running containers
# 🔍 View logs              | docker logs -f airflow-webserver-prod               | Follow logs in real time
# 🐚 Open shell             | docker exec -it airflow-webserver-prod bash         | Access container terminal
# 💾 Backup DB              | copy .\db\airflow_prod.db .\db\airflow_prod_backup_2025_11_05.db | Save metadata DB
# 🔄 Restore DB             | copy .\db\airflow_prod_backup_2025_11_05.db .\db\airflow_prod.db | Replace DB from backup
# ==================================================================================================

# ==================================================================================================
# 🧩 Step 10 — Environment Configuration (Dev vs Prod)
# --------------------------------------------------------------------------------------------------
# Setting         | Dev                             | Prod                              | Notes
# ----------------|----------------------------------|-----------------------------------|----------------------------------------
# Executor        | SequentialExecutor               | LocalExecutor / CeleryExecutor    | Sequential = Simple; Local = Parallel
# Database        | /db/airflow_dev.db               | /db/airflow_prod.db               | Separate DB files
# Web UI Port     | 8080                             | 8081                              | Avoid conflicts
# Purpose         | Testing & iteration               | Stable scheduled runs             | Keep isolated
# ==================================================================================================

# ==================================================================================================
# 🧠 Step 11 — Common Issues & Fixes
# --------------------------------------------------------------------------------------------------
# Error                                 | Cause                        | Solution
# --------------------------------------|-------------------------------|-----------------------------------------
# Invalid username/password             | DB reinitialized              | Recreate user
# DAG not found                         | Wrong repo mount              | Verify volume mapping & sys.path
# Cannot use SQLite with LocalExecutor   | Wrong executor type           | Switch to MySQL/Postgres
# FileNotFoundError: google-ads.yaml     | Secret not mounted correctly  | Confirm /opt/airflow/secrets volume
# UI not loading                         | Port conflict                 | Change port 8080/8081
# ==================================================================================================

# ==================================================================================================
# 🧩 Step 12 — Typical Full Workflow
# --------------------------------------------------------------------------------------------------
# 1️⃣ Move to Airflow folder
cd "C:/Users/prasa/Root/Airflow-Docker-Prod"

# 2️⃣ Initialize Airflow DB (only once)
docker compose run --rm airflow-init

# 3️⃣ Start Airflow in background
docker compose up -d

# 4️⃣ Create admin user
docker exec -it airflow-webserver-prod airflow users create \
  --username xxx \
  --firstname xxx \
  --lastname xxx \
  --role Admin \
  --email prasanna@xxx.com \
  --password "your_secure_password"

# 5️⃣ Access web UI: http://localhost:8081
# 6️⃣ Stop containers when needed
docker compose down
# ==================================================================================================

# ==================================================================================================
# 🧩 Step 13 — Quick Reference Cheat Sheet
# --------------------------------------------------------------------------------------------------
# Purpose               | Command                                                  | Detached? | Notes
# ----------------------|----------------------------------------------------------|-----------|----------------------------
# Initialize DB         | docker compose run --rm airflow-init                     | ❌        | One-time setup/reset
# Start Airflow         | docker compose up -d                                     | ✅        | Background (Prod)
# Start Airflow (debug) | docker compose up                                        | ❌        | Foreground logs
# Stop Airflow          | docker compose down                                      | ❌        | Graceful stop
# Full Reset            | docker compose down -v                                   | ❌        | Destroys DB
# Restart Airflow       | docker compose down && docker compose up -d              | ✅        | Refresh containers
# Rebuild Image         | docker compose build --no-cache && docker compose up -d  | ✅        | After Dockerfile change
# Create Admin User     | docker exec -it airflow-webserver-prod airflow users create ... | ❌ | For UI login
# List Containers       | docker ps                                                | ❌        | See running services
# Follow Logs           | docker logs -f airflow-webserver-prod                    | ❌        | Debug issues
# ==================================================================================================

# ==================================================================================================
# 🧠 Best Practices
# --------------------------------------------------------------------------------------------------
# ✅ Always backup your airflow_*.db before rebuilds or resets.
# ✅ Use `-d` for stability, `up` for troubleshooting.
# ✅ Keep Dev and Prod isolated (DBs, secrets, ports).
# ✅ Add to .gitignore:
#     *.env
#     *.json
#     *.yaml
#     db/
#     logs/
#     secrets/
# ⚠️ Avoid `docker compose down -v` unless you truly want a blank Airflow instance.
# ==================================================================================================
