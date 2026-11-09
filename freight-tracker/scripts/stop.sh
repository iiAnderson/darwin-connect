#!/usr/bin/env bash
# Stop freight-tracker (it saves state on SIGTERM). Note: the cron watchdog will restart it
# within 5 minutes unless you also remove the crontab lines.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PIDFILE="$DIR/logs/freight-tracker.pid"
if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  kill "$(cat "$PIDFILE")"
  echo "stopped $(cat "$PIDFILE")"
else
  echo "not running"
fi
