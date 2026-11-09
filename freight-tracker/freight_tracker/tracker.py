"""Live state for running trains (from TRUST) and board generation for a TIPLOC.

Expected times are estimated as: scheduled time at the watched TIPLOC + the
train's current early/late variation from its latest on-route TRUST report.
This deliberately avoids TRUST's epoch timestamps (whose timezone handling is
ambiguous in BST) by working purely from WTT times + variation minutes.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import threading
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from .timetable import Plan, Timetable

log = logging.getLogger(__name__)
LONDON = ZoneInfo("Europe/London")


@dataclass
class LiveTrain:
    train_id: str
    uid: str
    date: str  # service (origin) date, YYYY-MM-DD
    toc: str
    source: str  # C / V
    activated_ms: int
    cancelled: bool = False
    canx_reason: str = ""
    canx_type: str = ""
    canx_stanox: str = ""
    terminated: bool = False
    last_stanox: str = ""
    last_event: str = ""
    last_variation: Optional[int] = None  # signed minutes, + late / - early (on-route reports only)
    last_off_route: bool = False
    last_report_ms: int = 0
    reported_stanox: list[str] = field(default_factory=list)


@dataclass
class BoardRow:
    t: str  # scheduled HH:MM at the watched location
    half: bool  # scheduled time has a WTT half minute
    d: int  # day of the scheduled time relative to the board's date (0 today, 1 tomorrow, -1 yesterday)
    hc: str
    op: str  # short operator name (raw toc_id if unmapped, "" if never seen)
    toc: str  # TRUST numeric toc_id (from activation, else remembered from a previous run of this UID)
    frm: str
    to: str
    type: str  # PASS / CALL
    st: str  # NOT (not activated) / ACT (activated, not moving yet) / RUN / PASSED / CAN
    exp: str  # estimated HH:MM ("" if none)
    var: Optional[int]
    last: str  # "Parson Street 10:53" or ""
    cat: str
    reason: str
    uid: str
    sort_key: float

    def to_json(self) -> dict:
        d = asdict(self)
        d["from"] = d.pop("frm")
        d.pop("sort_key")
        return d


def _hhmm(minutes: float) -> str:
    m = int(minutes) % 1440
    return f"{m // 60:02d}:{m % 60:02d}"


def _variation(body: dict) -> Optional[int]:
    status = (body.get("variation_status") or "").upper()
    try:
        mins = int(body.get("timetable_variation") or 0)
    except ValueError:
        return None
    if status == "EARLY":
        return -mins
    if status == "LATE":
        return mins
    if status == "ON TIME":
        return 0
    return None  # OFF ROUTE / unknown


class Tracker:
    def __init__(self, timetable: Timetable, operators: dict[str, dict], *, passed_linger_min: int = 5,
                 not_activated_grace_min: int = 30, passed_after_estimate_min: int = 10):
        self.tt = timetable
        self.operators = operators
        self.passed_linger_min = passed_linger_min
        self.passed_after_estimate_min = passed_after_estimate_min
        self.not_activated_grace_min = not_activated_grace_min
        self.trains: dict[str, LiveTrain] = {}  # by TRUST train_id
        self.by_service: dict[tuple[str, str], str] = {}  # (uid, date) -> train_id
        self.uid_toc: dict[str, str] = {}  # operator seen on previous activations of a UID
        self._unmatched: set[str] = set()  # train_ids already tried and not ours
        self._hc_cache: tuple[int, set[str]] = (0, set())
        self.last_kafka_ms = 0
        self.counts: dict[str, int] = {}
        self._lock = threading.RLock()

    # ---- TRUST message handling ----------------------------------------------------------
    def handle(self, msg: dict, kafka_ms: int = 0) -> None:
        header, body = msg.get("header", {}), msg.get("body", {})
        mtype = header.get("msg_type")
        with self._lock:
            self.counts[mtype] = self.counts.get(mtype, 0) + 1
            if kafka_ms:
                self.last_kafka_ms = max(self.last_kafka_ms, kafka_ms)
            if mtype == "0001":
                self._activation(body)
                return
            train = self.trains.get(body.get("train_id", ""))
            if train is None and mtype == "0003":
                train = self._adopt_unactivated(body)
            if train is None:
                return
            if mtype == "0002":
                train.cancelled = True
                train.canx_reason = body.get("canx_reason_code") or ""
                train.canx_type = body.get("canx_type") or ""
                train.canx_stanox = body.get("loc_stanox") or ""
            elif mtype == "0003":
                self._movement(train, body)
            elif mtype == "0005":
                train.cancelled = False
                train.canx_reason = train.canx_type = train.canx_stanox = ""
            elif mtype == "0007":
                new_id = body.get("revised_train_id")
                if new_id:
                    self.trains.pop(train.train_id, None)
                    train.train_id = new_id
                    self.trains[new_id] = train
                    self.by_service[(train.uid, train.date)] = new_id

    def _activation(self, b: dict) -> None:
        uid = (b.get("train_uid") or "").strip()
        date = b.get("tp_origin_timestamp") or b.get("schedule_start_date") or ""
        train_id = b.get("train_id", "")
        if not uid or not train_id:
            return
        # Only keep trains whose timetable we hold (i.e. ones touching a watched TIPLOC).
        if uid not in self.tt.plans:
            return
        try:
            created = int(b.get("creation_timestamp") or 0)
        except ValueError:
            created = 0
        self.trains[train_id] = LiveTrain(
            train_id=train_id,
            uid=uid,
            date=date,
            toc=b.get("toc_id") or "",
            source=b.get("schedule_source") or "",
            activated_ms=created,
        )
        self.by_service[(uid, date)] = train_id
        if b.get("toc_id"):
            self.uid_toc[uid] = b["toc_id"]

    def _adopt_unactivated(self, b: dict) -> Optional[LiveTrain]:
        """Link a movement to a timetable when we never saw the activation (Kafka keeps as little
        as ~1.5 h on weekdays, but activations can come 10 h before departure).

        TRUST train_id = origin STANOX area (2) + headcode (4) + 2 chars + day of month (2). We
        match headcode + day of month + a scheduled stop at the reported STANOX whose WTT time is
        within 2 min of the report's planned time (tried as both UTC and UK local).
        """
        train_id = b.get("train_id") or ""
        if len(train_id) != 10 or train_id in self._unmatched:
            return None
        hc, dom = train_id[2:6], train_id[8:10]
        if hc not in self._watched_headcodes():
            return None
        try:
            planned = int(b.get("planned_timestamp") or 0) / 1000
        except ValueError:
            planned = 0
        stanox = b.get("loc_stanox") or ""
        if not planned or not stanox:
            return None
        when_utc = dt.datetime.fromtimestamp(planned, dt.timezone.utc)
        candidates_min = {when_utc.hour * 60 + when_utc.minute + when_utc.second / 60}
        local = when_utc.astimezone(LONDON)
        candidates_min.add(local.hour * 60 + local.minute + local.second / 60)

        for day in {(when_utc - dt.timedelta(days=1)).date(), when_utc.date(), local.date()}:
            if f"{day.day:02d}" != dom:
                continue
            for plan in self.tt.plans_on(day):
                if plan.headcode != hc or (plan.uid, day.isoformat()) in self.by_service:
                    continue
                for s in plan.stops:
                    if self.tt.stanox_of(s.tiploc) != stanox:
                        continue
                    sched = s.minutes % 1440
                    if any(min(abs(sched - c), 1440 - abs(sched - c)) <= 2 for c in candidates_min):
                        train = LiveTrain(
                            train_id=train_id,
                            uid=plan.uid,
                            date=day.isoformat(),
                            toc=b.get("toc_id") or "",
                            source="inferred",
                            activated_ms=0,
                        )
                        self.trains[train_id] = train
                        self.by_service[(plan.uid, day.isoformat())] = train_id
                        if train.toc:
                            self.uid_toc[plan.uid] = train.toc
                        log.info("Linked %s to %s %s without activation", train_id, plan.uid, day)
                        return train
        self._unmatched.add(train_id)
        return None

    def _watched_headcodes(self) -> set[str]:
        key = id(self.tt.plans)
        if self._hc_cache[0] != key:
            hcs = {p.headcode for plans in self.tt.plans.values() for p in plans if p.headcode}
            self._hc_cache = (key, hcs)
            self._unmatched.clear()
        return self._hc_cache[1]

    def _movement(self, train: LiveTrain, b: dict) -> None:
        stanox = b.get("loc_stanox") or ""
        train.last_stanox = stanox
        train.last_event = b.get("event_type") or ""
        try:
            train.last_report_ms = int(b.get("actual_timestamp") or 0)
        except ValueError:
            pass
        var = _variation(b)
        train.last_off_route = var is None
        if var is not None:
            train.last_variation = var
        if stanox and stanox not in train.reported_stanox:
            train.reported_stanox.append(stanox)
        if (b.get("train_terminated") or "").lower() == "true":
            train.terminated = True

    # ---- board -------------------------------------------------------------------------------
    def board(self, tiploc: str, now: Optional[dt.datetime] = None, horizon_hours: float = 24,
              max_rows: int = 9) -> list[BoardRow]:
        now = (now or dt.datetime.now(dt.timezone.utc)).astimezone(LONDON)
        today = now.date()
        rows: list[BoardRow] = []
        with self._lock:
            for day in (today - dt.timedelta(days=1), today, today + dt.timedelta(days=1)):
                for plan in self.tt.plans_at(tiploc, day):
                    row = self._row(plan, day, tiploc, now)
                    if row is None:
                        continue
                    sched_dt = self._at(day, plan.stops[plan.stop_index(tiploc)].minutes)
                    exp_dt = sched_dt + dt.timedelta(minutes=row.var or 0)
                    keep_passed = self.passed_after_estimate_min + self.passed_linger_min
                    if row.st == "PASSED" and exp_dt < now - dt.timedelta(minutes=keep_passed):
                        continue
                    if row.st in ("NOT", "CAN") and sched_dt < now - dt.timedelta(minutes=self.not_activated_grace_min):
                        continue
                    if row.st in ("ACT", "RUN") and exp_dt < now - dt.timedelta(hours=3):
                        continue  # stale: lost track of it
                    if sched_dt > now + dt.timedelta(hours=horizon_hours) and exp_dt > now + dt.timedelta(hours=horizon_hours):
                        continue
                    row.sort_key = exp_dt.timestamp()
                    rows.append(row)
        rows.sort(key=lambda r: r.sort_key)
        return rows[:max_rows]

    @staticmethod
    def _at(day: dt.date, minutes: float) -> dt.datetime:
        naive = dt.datetime.combine(day, dt.time()) + dt.timedelta(minutes=minutes)
        return naive.replace(tzinfo=LONDON)

    def _row(self, plan: Plan, day: dt.date, tiploc: str, now: dt.datetime) -> Optional[BoardRow]:
        idx = plan.stop_index(tiploc)
        stop = plan.stops[idx]
        train_id = self.by_service.get((plan.uid, day.isoformat()))
        train = self.trains.get(train_id) if train_id else None
        toc = train.toc if train else self.uid_toc.get(plan.uid, "")
        sched_day = (dt.datetime.combine(day, dt.time()) + dt.timedelta(minutes=stop.minutes)).date()

        st, exp, var, last, reason = "NOT", "", None, "", ""
        if train:
            st = "ACT"
            if train.cancelled and self._cancel_affects(plan, idx, train):
                st, reason = "CAN", train.canx_reason
            else:
                positions = [self._index_of_stanox(plan, s) for s in train.reported_stanox]
                furthest = max((p for p in positions if p is not None), default=None)
                at_target_departed = furthest == idx and (stop.is_pass or train.last_event == "DEPARTURE")
                if furthest is not None and (furthest > idx or at_target_departed):
                    st = "PASSED"
                elif train.reported_stanox:
                    st = "RUN"
                if train.last_variation is not None and st in ("RUN", "PASSED"):
                    var = train.last_variation
                    exp = _hhmm(stop.minutes + var)
                    # Most watched junctions never report; infer "passed" once the estimate is well gone.
                    if st == "RUN" and self._at(day, stop.minutes + var) < now - dt.timedelta(
                        minutes=self.passed_after_estimate_min
                    ):
                        st = "PASSED"
                if train.last_stanox:
                    last = self._last_seen(plan, train)

        return BoardRow(
            t=_hhmm(stop.minutes),
            half=(stop.minutes % 1) >= 0.5,
            d=(sched_day - now.date()).days,
            hc=plan.headcode,
            op=self.operators.get(toc, {}).get("short", toc),
            toc=toc,
            frm=self.tt.name_of_tiploc(plan.stops[0].tiploc) if plan.stops else "",
            to=self.tt.name_of_tiploc(plan.stops[-1].tiploc) if plan.stops else "",
            type="PASS" if stop.is_pass else "CALL",
            st=st,
            exp=exp,
            var=var,
            last=last,
            cat=plan.category,
            reason=reason,
            uid=plan.uid,
            sort_key=0,
        )

    def _index_of_stanox(self, plan: Plan, stanox: str) -> Optional[int]:
        for i, s in enumerate(plan.stops):
            if self.tt.stanox_of(s.tiploc) == stanox:
                return i
        return None

    def _cancel_affects(self, plan: Plan, idx: int, train: LiveTrain) -> bool:
        if train.canx_type.upper() == "AT ORIGIN" or not train.canx_stanox:
            return True
        canx_idx = self._index_of_stanox(plan, train.canx_stanox)
        return canx_idx is None or canx_idx <= idx

    def _last_seen(self, plan: Plan, train: LiveTrain) -> str:
        name = self.tt.name_of_stanox(train.last_stanox)
        i = self._index_of_stanox(plan, train.last_stanox)
        if i is not None and train.last_variation is not None and not train.last_off_route:
            return f"{name} {_hhmm(plan.stops[i].minutes + train.last_variation)}"
        if train.last_report_ms:  # off route: fall back to TRUST timestamp
            t = dt.datetime.fromtimestamp(train.last_report_ms / 1000, dt.timezone.utc).astimezone(LONDON)
            return f"{name} {t:%H:%M}"
        return name

    # ---- persistence ------------------------------------------------------------------------
    def save(self, path: Path) -> None:
        with self._lock:
            cutoff = (dt.date.today() - dt.timedelta(days=2)).isoformat()
            self.trains = {k: v for k, v in self.trains.items() if v.date >= cutoff}
            self.by_service = {k: v for k, v in self.by_service.items() if v in self.trains}
            data = {
                "last_kafka_ms": self.last_kafka_ms,
                "uid_toc": self.uid_toc,
                "trains": [asdict(t) for t in self.trains.values()],
            }
        tmp = Path(path).with_suffix(".tmp")
        tmp.write_text(json.dumps(data))
        tmp.replace(path)

    def load(self, path: Path) -> None:
        path = Path(path)
        if not path.exists():
            return
        data = json.loads(path.read_text())
        with self._lock:
            self.last_kafka_ms = data.get("last_kafka_ms", 0)
            self.uid_toc.update(data.get("uid_toc", {}))
            for t in data.get("trains", []):
                train = LiveTrain(**t)
                self.trains[train.train_id] = train
                self.by_service[(train.uid, train.date)] = train.train_id
        log.info("Restored %d trains from %s", len(self.trains), path)


def board_json(tracker: Tracker, tiploc: str, now: Optional[dt.datetime] = None, **kw) -> dict:
    now = now or dt.datetime.now(dt.timezone.utc)
    rows = tracker.board(tiploc, now, **kw)
    return {
        "loc": tracker.tt.name_of_tiploc(tiploc),
        "tiploc": tiploc,
        "generated": now.astimezone(LONDON).isoformat(timespec="seconds"),
        "svcs": [r.to_json() for r in rows],
    }
