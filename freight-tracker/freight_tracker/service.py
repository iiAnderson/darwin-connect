"""Long-running service: Kafka consumer + timetable reloader + HTTP/JSON output."""
from __future__ import annotations

import datetime as dt
import json
import logging
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional

from .config import Config
from .timetable import Timetable, newest_cif
from .tracker import Tracker, board_json

log = logging.getLogger(__name__)


def fetch_newest_from_s3(uri: str, dest_dir: Path) -> Optional[Path]:
    """Download the newest object under s3://bucket/prefix into dest_dir, if not already there."""
    import boto3  # optional dependency (poetry install -E s3)

    bucket, _, prefix = uri.removeprefix("s3://").partition("/")
    s3 = boto3.client("s3")
    newest = None
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith((".json.gz", ".json")) and (newest is None or obj["LastModified"] > newest["LastModified"]):
                newest = obj
    if newest is None:
        return None
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / Path(newest["Key"]).name
    stamp = newest["LastModified"].timestamp()
    if target.exists() and target.stat().st_mtime >= stamp:
        return target
    tmp = target.with_suffix(target.suffix + ".part")
    log.info("Downloading s3://%s/%s", bucket, newest["Key"])
    s3.download_file(bucket, newest["Key"], str(tmp))
    tmp.replace(target)
    import os

    os.utime(target, (stamp, stamp))
    # keep the directory tidy: only the newest two files
    files = sorted((p for p in dest_dir.iterdir() if p.name.endswith((".json.gz", ".json"))), key=lambda p: p.stat().st_mtime)
    for old in files[:-2]:
        old.unlink()
    return target


class Service:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.tt = Timetable(set(cfg.watch))
        self.tracker = Tracker(
            self.tt,
            cfg.operators,
            passed_linger_min=cfg.passed_linger_min,
            not_activated_grace_min=cfg.not_activated_grace_min,
            passed_after_estimate_min=cfg.passed_after_estimate_min,
        )
        self.stop_event = threading.Event()
        self.started = dt.datetime.now(dt.timezone.utc)
        self._cif_mtime = 0.0

    # ---- timetable ---------------------------------------------------------------------------
    def refresh_timetable(self) -> None:
        cif_dir = self.cfg.cif_path
        if self.cfg.cif_s3_uri:
            try:
                fetch_newest_from_s3(self.cfg.cif_s3_uri, cif_dir if cif_dir.suffix == "" else cif_dir.parent)
            except Exception:  # noqa: BLE001
                log.exception("S3 CIF fetch failed; keeping current timetable")
        path = newest_cif(cif_dir)
        if path is None:
            log.error("No CIF file found at %s", cif_dir)
            return
        mtime = path.stat().st_mtime
        if mtime > self._cif_mtime:
            self.tt.load_cif(path)
            self._cif_mtime = mtime

    def _timetable_loop(self) -> None:
        while not self.stop_event.wait(self.cfg.cif_check_minutes * 60):
            try:
                self.refresh_timetable()
            except Exception:  # noqa: BLE001
                log.exception("Timetable refresh failed")

    # ---- outputs -----------------------------------------------------------------------------
    def board(self, tiploc: str) -> dict:
        return board_json(self.tracker, tiploc, horizon_hours=self.cfg.horizon_hours, max_rows=self.cfg.max_rows)

    def health(self) -> dict:
        last = self.tracker.last_kafka_ms
        return {
            "ok": bool(last) and (time.time() * 1000 - last) < 10 * 60 * 1000,
            "started": self.started.isoformat(timespec="seconds"),
            "last_kafka_message": dt.datetime.fromtimestamp(last / 1000, dt.timezone.utc).isoformat(timespec="seconds")
            if last
            else None,
            "message_counts": self.tracker.counts,
            "tracked_trains": len(self.tracker.trains),
            "timetable_file": self.tt.source_file,
            "timetable_uids": len(self.tt.plans),
            "watch": self.cfg.watch,
        }

    def _writer_loop(self) -> None:
        while not self.stop_event.wait(self.cfg.write_seconds):
            try:
                self.tracker.save(self.cfg.state_file)
                if self.cfg.output_dir:
                    self.cfg.output_dir.mkdir(parents=True, exist_ok=True)
                    for tiploc in self.cfg.watch:
                        out = self.cfg.output_dir / f"{tiploc}.json"
                        tmp = out.with_suffix(".tmp")
                        tmp.write_text(json.dumps(self.board(tiploc), separators=(",", ":")))
                        tmp.replace(out)
            except Exception:  # noqa: BLE001
                log.exception("Writer failed")

    def _http(self) -> ThreadingHTTPServer:
        service = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                path = self.path.split("?")[0].rstrip("/")
                if path == "/health":
                    body, code = service.health(), 200
                elif path.startswith("/board/"):
                    tiploc = path.removeprefix("/board/").removesuffix(".json").upper()
                    if tiploc in service.cfg.watch:
                        body, code = service.board(tiploc), 200
                    else:
                        body, code = {"error": f"not watching {tiploc}", "watch": service.cfg.watch}, 404
                else:
                    body, code = {"endpoints": ["/health", *[f"/board/{t}.json" for t in service.cfg.watch]]}, 200
                data = json.dumps(body, separators=(",", ":")).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-store")
                # lets the departures board's web settings page "Test" the server from the browser
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, fmt, *args):
                log.debug("http %s - " + fmt, self.client_address[0], *args)

        return ThreadingHTTPServer((self.cfg.http_host, self.cfg.http_port), Handler)

    # ---- main --------------------------------------------------------------------------------
    def run(self) -> None:
        from .kafka_source import KafkaSource  # imported here so offline commands don't need librdkafka

        if not self.cfg.topics:
            raise SystemExit("No Kafka topics configured (see secrets.env.example)")
        signal.signal(signal.SIGTERM, lambda *_: self.stop_event.set())
        signal.signal(signal.SIGINT, lambda *_: self.stop_event.set())

        self.refresh_timetable()
        self.tracker.load(self.cfg.state_file)

        floor_ms = int((time.time() - self.cfg.max_rewind_minutes * 60) * 1000)
        start_ms = max(floor_ms, self.tracker.last_kafka_ms - 60_000) if self.tracker.last_kafka_ms else floor_ms
        source = KafkaSource(self.cfg.topics, start_ms)

        httpd = self._http()
        threads = [
            threading.Thread(target=httpd.serve_forever, name="http", daemon=True),
            threading.Thread(target=self._timetable_loop, name="timetable", daemon=True),
            threading.Thread(target=self._writer_loop, name="writer", daemon=True),
        ]
        for t in threads:
            t.start()
        log.info("Serving on http://%s:%d/  watching %s", self.cfg.http_host, self.cfg.http_port, self.cfg.watch)

        def on_message(topic: str, msg: dict, ts: int) -> None:
            if topic == "vstp":
                try:
                    plan = self.tt.apply_vstp(msg)
                    if plan:
                        log.info("VSTP %s %s %s", plan.uid, plan.headcode, plan.start)
                except (KeyError, ValueError):
                    log.warning("Unparseable VSTP message: %.300s", json.dumps(msg))
            else:
                self.tracker.handle(msg, ts)

        try:
            source.run(on_message, self.stop_event.is_set)
        finally:
            log.info("Shutting down")
            self.stop_event.set()
            httpd.shutdown()
            source.close()
            self.tracker.save(self.cfg.state_file)
