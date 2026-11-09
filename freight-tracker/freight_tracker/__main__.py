"""CLI.

  python -m freight_tracker run    --config config.toml
  python -m freight_tracker board  --cif FILE --tiploc ASHJN [--replay trust.jsonl] [--at ISO_TIME]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
from pathlib import Path

from .timetable import Timetable
from .tracker import Tracker, board_json


def cmd_run(args: argparse.Namespace) -> None:
    from .config import load_config
    from .service import Service

    Service(load_config(args.config)).run()


def cmd_board(args: argparse.Namespace) -> None:
    """Offline: build a board from a CIF file and (optionally) replayed TRUST messages."""
    tiploc = args.tiploc.upper()
    tt = Timetable({tiploc})
    tt.load_cif(Path(args.cif))
    operators = {}
    if args.operators and Path(args.operators).exists():
        operators = {k: v for k, v in json.loads(Path(args.operators).read_text()).items() if not k.startswith("_")}
    tracker = Tracker(tt, operators)
    at = dt.datetime.fromisoformat(args.at) if args.at else dt.datetime.now(dt.timezone.utc)
    if at.tzinfo is None:
        at = at.replace(tzinfo=dt.timezone.utc)
    if args.replay:
        for line in open(args.replay):
            rec = json.loads(line)
            ts = rec.get("kafka_ts") or int(rec.get("header", {}).get("msg_queue_timestamp", 0))
            if ts <= at.timestamp() * 1000:
                tracker.handle(rec, ts)
    board = board_json(tracker, tiploc, at, horizon_hours=args.horizon, max_rows=args.rows)
    if args.json:
        print(json.dumps(board, indent=1))
        return
    print(f"{board['loc']} ({tiploc}) at {board['generated']}")
    for s in board["svcs"]:
        t = s["t"] + ("½" if s["half"] else " ") + ("+1" if s["d"] == 1 else "")
        print(
            f"{t:7}{s['hc']:5}{s['op'] or '-':6}{s['from'][:22]:23}→ {s['to'][:22]:23}{s['type']:5}{s['st']:7}"
            f"{('exp ' + s['exp']) if s['exp'] else '':10}{s['last']}{(' reason ' + s['reason']) if s['reason'] else ''}"
        )


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="freight-tracker")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="run the live service")
    r.add_argument("--config", default="config.toml")
    r.set_defaults(func=cmd_run)

    b = sub.add_parser("board", help="print a board offline from a CIF file (+ optional TRUST replay)")
    b.add_argument("--cif", required=True)
    b.add_argument("--tiploc", required=True)
    b.add_argument("--replay", help="JSONL of TRUST messages (as captured)")
    b.add_argument("--at", help="board time, ISO 8601 (default now, UTC if no offset)")
    b.add_argument("--operators", default="operators.json")
    b.add_argument("--horizon", type=float, default=24)
    b.add_argument("--rows", type=int, default=9)
    b.add_argument("--json", action="store_true")
    b.set_defaults(func=cmd_board)

    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    args.func(args)


if __name__ == "__main__":
    main()
