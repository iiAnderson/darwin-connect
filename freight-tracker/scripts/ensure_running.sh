#!/usr/bin/env bash
# Start freight-tracker if it isn't already running. Safe to call repeatedly (cron every 5 min + @reboot).
# No root needed: it runs as the current user, detached, logging to logs/freight-tracker.log.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PIDFILE="$DIR/logs/freight-tracker.pid"
mkdir -p "$DIR/logs"

if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  exit 0
fi

PY="$(cd "$DIR" && "${POETRY:-$HOME/.local/bin/poetry}" env info --executable)"
cd "$DIR"
# keep the last log around for post-mortems
[[ -f logs/freight-tracker.log ]] && mv logs/freight-tracker.log logs/freight-tracker.prev.log
setsid "$PY" -m freight_tracker run --config config.toml >>logs/freight-tracker.log 2>&1 </dev/null &
echo $! >"$PIDFILE"
echo "$(date -u +%FT%TZ) started pid $(cat "$PIDFILE")" >>logs/supervisor.log
