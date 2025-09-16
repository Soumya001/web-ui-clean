#!/usr/bin/env bash
# one-shot prune of stale workers (safe to run repeatedly)
set -euo pipefail

DB="/home/monga/web-ui-multicoin/ckpool.sqlite"   # <- absolute path (adjust if your DB is elsewhere)
SQLITE="/usr/bin/sqlite3"

# timeout seconds — keep in sync with WORKER_TIMEOUT env or parser constant (default 300)
TIMEOUT=300

# mark stale workers inactive (this will trigger the AFTER UPDATE trigger to update user_stats)
"$SQLITE" "$DB" "UPDATE workers_seen SET active=0 WHERE active=1 AND (strftime('%s','now') - last_seen) > ${TIMEOUT};"
