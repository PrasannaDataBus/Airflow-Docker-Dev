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
# 🧭 Navigate to Your Airflow Folder
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
# 🧩 Initialize the Airflow Metadata Database
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
# 🟢 Start Airflow Containers
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
docker compose up

# ==================================================================================================
# 🛑 Stop Airflow Containers
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
# 💣 Full Environment Reset (Deletes Database)
# --------------------------------------------------------------------------------------------------
# ✅ When to Use:
#   - To completely wipe and reinitialize Airflow (clean start)
# ⚠️ When NOT to Use:
#   - If you want to preserve metadata, DAG runs, or user accounts
# ==================================================================================================
docker compose down -v

# ==================================================================================================
# 🔁 Restart or Rebuild Airflow
# --------------------------------------------------------------------------------------------------
# Restart Cleanly
# ✅ Use when restarting after small edits to environment variables or compose file.
# ==================================================================================================
# docker compose down
# docker compose up -d

# Rebuild Images (after Dockerfile or dependency changes)
# ✅ Use when adding new Python dependencies or modifying Dockerfile.
# ==================================================================================================
docker compose build --no-cache
docker compose up -d

# ==================================================================================================
# 👤 Create an Admin User
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
# 🌐 Access the Airflow Web UI
# --------------------------------------------------------------------------------------------------
# Dev UI:  http://localhost:8080
# Prod UI: http://localhost:8081
# Login with the credentials created above.
# ==================================================================================================

# ==================================================================================================
# 🧹 Maintenance & Inspection
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
# 🧩 Environment Configuration (Dev vs Prod)
# --------------------------------------------------------------------------------------------------
# Setting         | Dev                             | Prod                              | Notes
# ----------------|----------------------------------|-----------------------------------|----------------------------------------
# Executor        | SequentialExecutor               | LocalExecutor / CeleryExecutor    | Sequential = Simple; Local = Parallel
# Database        | /db/airflow_dev.db               | /db/airflow_prod.db               | Separate DB files
# Web UI Port     | 8080                             | 8081                              | Avoid conflicts
# Purpose         | Testing & iteration               | Stable scheduled runs             | Keep isolated
# ==================================================================================================

# ==================================================================================================
# 🧠 Common Issues & Fixes
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
# 🧩 Typical Full Workflow
# --------------------------------------------------------------------------------------------------
# 1️⃣ Move to Airflow folder
cd "C:/Users/prasa/Root/Airflow-Docker-dev"

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
# 🟡 Apply Dependency Changes (New Libraries)
# --------------------------------------------------------------------------------------------------
# Option B — Rebuild Images
# ✅ When to Use:
#   - You added a new library to 'requirements.txt' (e.g., google-ads, facebook_business).
#   - You changed the 'Dockerfile'.
#
# 🧠 What It Does:
#   - Recompiles the Docker image with new Python libraries.
#   - Recreates containers using the new image.
# ==================================================================================================
docker compose up -d --build

# ==================================================================================================
# 🔴  The "Nuclear" Reset (Troubleshooting)
# --------------------------------------------------------------------------------------------------
# Option C — Wipe & Restart
# ✅ When to Use:
#   - You changed DB credentials (passwords/users) in .env.
#   - You switched Executors (e.g., SQLite to Postgres).
#   - You see "Database is locked" or strange schema errors.
#   - You want to clear all past DAG run history and start fresh.
#
# ⚠️ WARNING: This deletes all historical DAG run data in the database!
# ==================================================================================================
docker compose down --volumes --remove-orphans
docker compose up -d --build

# ==================================================================================================
# 🔵 Debugging "Infinite Restarts" or "Exited" Containers
# --------------------------------------------------------------------------------------------------
# If a container (like the scheduler) keeps restarting or exits immediately:
# 1. Check the logs to see the Python error:
#    > docker logs airflow-scheduler-dev
#    or
#    > docker logs airflow-webserver-dev
#
# 2. Common Causes & Fixes:
#    - "Network is unreachable" -> You have top-level code in a DAG. Move client init inside functions.
#    - "Database is locked" -> You are using SQLite with Parallelism. Ensure .env points to Postgres.
#    - "OOM / SIGKILL" -> Docker ran out of RAM. Increase Docker Desktop memory to 4GB+.
# ==================================================================================================

# ==================================================================================================
# 🧩 Quick Reference Cheat Sheet
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

# ==================================================================================================
# FERNET KEY Related
# --------------------------------------------------------------------------------------------------

# To Generate AIRFLOW_FERNET_KEY follow the below steps:

# The below will install python if python was not found as Docker image

docker run --rm python:3.10 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# The above will install cyrtopgraphy if not installed previously and generates AIRFLOW_FERNET_KEY

docker run --rm python:3.10 sh -c "pip install cryptography >/dev/null 2>&1 && python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'"

# Note: If your loval venv uses 3.9 but you have installed python 3.10 image in docker - I would suggest to keep both local and docker image using the same version
# Follow the below steps to remove the previously installed version 3.10 and install 3.9.

docker rmi python:3.10

# After removing 3.10 you can verify with:

docker images

# Now you will need to install Python 3.9 image - the below code will install python 3.9 if not found and it will generate AIRFLOW_FERNET_KEY

docker run --rm python:3.9 sh -c "pip install cryptography >/dev/null 2>&1 && python - << 'EOF'
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
EOF"

# If you are confident that python and cyrptography has already been installed then use the below code
# The below can also be used to re-generate AIRFLOW_FERNET_KEY

py -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# ==================================================================================================

# ==================================================================================================
# Stop and Destroy Volumes:
# --------------------------------------------------------------------------------------------------

docker compose down --volumes --remove-orphans

# Start Fresh:

docker compose up -d --build

# ==================================================================================================