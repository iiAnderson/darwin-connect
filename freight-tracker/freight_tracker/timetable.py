"""Timetable handling: CIF JSON (daily file) and VSTP schedules.

Only schedules that touch a watched TIPLOC are kept, plus every other record
(overlays, cancellations) sharing their train UID, so STP precedence can be
resolved correctly for any date.
"""
from __future__ import annotations

import datetime as dt
import gzip
import json
import logging
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator, Optional

log = logging.getLogger(__name__)

# Lower rank wins when several records for one UID are valid on the same date.
STP_RANK = {"C": 0, "N": 1, "O": 2, "P": 3}


@dataclass(frozen=True)
class Stop:
    tiploc: str
    minutes: float  # minutes after midnight of the service date (can exceed 1440)
    is_pass: bool


@dataclass
class Plan:
    uid: str
    stp: str
    start: dt.date
    end: dt.date
    days: str
    headcode: str
    status: str
    category: str
    power: str
    timing_load: str
    source: str  # "C" = CIF file, "V" = VSTP
    stops: list[Stop] = field(default_factory=list)

    def runs_on(self, day: dt.date) -> bool:
        return self.start <= day <= self.end and len(self.days) == 7 and self.days[day.weekday()] == "1"

    def stop_index(self, tiploc: str) -> Optional[int]:
        for i, s in enumerate(self.stops):
            if s.tiploc == tiploc:
                return i
        return None


@dataclass(frozen=True)
class Location:
    tiploc: str
    stanox: Optional[str]
    name: str


def parse_wtt_time(value: Optional[str]) -> Optional[float]:
    """CIF 'HHMM' with optional 'H' half-minute, or VSTP 'HHMMSS'. Returns minutes after midnight."""
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        if len(value) == 6 and value.isdigit():  # VSTP HHMMSS
            return int(value[:2]) * 60 + int(value[2:4]) + int(value[4:]) / 60
        minutes = int(value[:2]) * 60 + int(value[2:4])
        return minutes + (0.5 if value[4:5] == "H" else 0)
    except ValueError:
        return None


def nice_name(desc: Optional[str], fallback: str) -> str:
    if not desc:
        return fallback
    words = []
    for w in desc.split():
        bare = w.rstrip(".")
        if bare in ("JN", "JCN", "JNC"):
            words.append("Jn")
        elif bare.isalpha() and not any(c in "AEIOU" for c in bare):  # abbreviations: TC, GBRF, FHH
            words.append(bare)
        elif "." in bare and bare.replace(".", "").isalpha():  # T.C. -> TC
            words.append(bare.replace(".", ""))
        else:
            words.append(w.capitalize())
    return " ".join(words)


def _build_stops(raw_locs: Iterable[tuple[str, Optional[str], Optional[str], Optional[str]]]) -> list[Stop]:
    """raw_locs: (tiploc, arrival, departure, pass). Makes times monotonic across midnight."""
    stops: list[Stop] = []
    day_offset = 0.0
    prev = None
    for tiploc, arr, dep, pas in raw_locs:
        t = parse_wtt_time(pas)
        is_pass = t is not None
        if t is None:
            t = parse_wtt_time(dep)
        if t is None:
            t = parse_wtt_time(arr)
        if t is None:
            continue
        t += day_offset
        if prev is not None and t < prev - 360:  # went backwards by >6h: crossed midnight
            day_offset += 1440
            t += 1440
        prev = t
        stops.append(Stop(tiploc, t, is_pass))
    return stops


def plan_from_cif(s: dict) -> Plan:
    seg = s.get("schedule_segment") or {}
    locs = seg.get("schedule_location") or []
    stops = _build_stops(
        (l.get("tiploc_code"), l.get("arrival"), l.get("departure"), l.get("pass")) for l in locs if l.get("tiploc_code")
    )
    return Plan(
        uid=s["CIF_train_uid"].strip(),
        stp=s.get("CIF_stp_indicator") or "P",
        start=dt.date.fromisoformat(s["schedule_start_date"]),
        end=dt.date.fromisoformat(s.get("schedule_end_date") or s["schedule_start_date"]),
        days=s.get("schedule_days_runs") or "0000000",
        headcode=(seg.get("signalling_id") or "").strip(),
        status=s.get("train_status") or "",
        category=seg.get("CIF_train_category") or "",
        power=seg.get("CIF_power_type") or "",
        timing_load=seg.get("CIF_timing_load") or "",
        source="C",
        stops=stops,
    )


def plan_from_vstp(msg: dict) -> tuple[str, Plan]:
    """Parse a VSTP message. Returns (transaction_type, Plan).

    NOTE: written against the documented NROD VSTP JSON layout; not yet verified
    against live RDM VSTP messages. Accepts a list or object schedule_segment and
    both nested (location.tiploc.tiploc_id) and flat (tiploc_id) location forms.
    """
    body = msg.get("VSTPCIFMsgV1", msg)
    s = body["schedule"]
    seg = s.get("schedule_segment") or {}
    if isinstance(seg, list):
        seg = seg[0] if seg else {}
    raw = []
    for l in seg.get("schedule_location") or []:
        loc = l.get("location") or {}
        tip = loc.get("tiploc") or {}
        tiploc = tip.get("tiploc_id") if isinstance(tip, dict) else None
        tiploc = tiploc or l.get("tiploc_id") or l.get("tiploc_code")
        if tiploc:
            raw.append(
                (
                    tiploc.strip(),
                    l.get("scheduled_arrival_time"),
                    l.get("scheduled_departure_time"),
                    l.get("scheduled_pass_time"),
                )
            )
    plan = Plan(
        uid=(s.get("CIF_train_uid") or "").strip(),
        stp=(s.get("CIF_stp_indicator") or "N").strip() or "N",
        start=dt.date.fromisoformat(s["schedule_start_date"]),
        end=dt.date.fromisoformat(s.get("schedule_end_date") or s["schedule_start_date"]),
        days=s.get("schedule_days_runs") or "1111111",
        headcode=(seg.get("signalling_id") or "").strip(),
        status=s.get("train_status") or "",
        category=seg.get("CIF_train_category") or "",
        power=seg.get("CIF_power_type") or "",
        timing_load=seg.get("CIF_timing_load") or "",
        source="V",
        stops=_build_stops(raw),
    )
    return (s.get("transaction_type") or "Create"), plan


def _iter_lines(path: Path) -> Iterator[str]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as f:
        yield from f


class Timetable:
    """Schedules (CIF + VSTP) for trains passing watched TIPLOCs, plus location names."""

    def __init__(self, watched: set[str]):
        self.watched = set(watched)
        self.plans: dict[str, list[Plan]] = {}
        self.locations: dict[str, Location] = {}  # by tiploc
        self.by_stanox: dict[str, Location] = {}
        self.source_file: Optional[str] = None
        self.loaded_at: Optional[dt.datetime] = None
        self._lock = threading.RLock()

    # ---- loading -------------------------------------------------------------------------
    def load_cif(self, path: Path) -> None:
        """Two streaming passes over the (gzipped) CIF JSON file, then atomically swap in."""
        path = Path(path)
        log.info("Loading CIF %s (pass 1: tiplocs + matching UIDs)", path)
        uids: set[str] = set()
        locations: dict[str, Location] = {}
        for line in _iter_lines(path):
            if line.startswith('{"TiplocV1"'):
                t = json.loads(line)["TiplocV1"]
                locations[t["tiploc_code"]] = Location(
                    t["tiploc_code"], t.get("stanox"), nice_name(t.get("tps_description"), t["tiploc_code"])
                )
            elif line.startswith('{"JsonScheduleV1"') and any(f'"{w}"' in line for w in self.watched):
                s = json.loads(line)["JsonScheduleV1"]
                locs = (s.get("schedule_segment") or {}).get("schedule_location") or []
                if any(l.get("tiploc_code") in self.watched for l in locs):
                    uids.add(s["CIF_train_uid"].strip())

        log.info("Pass 2: collecting all records for %d UIDs", len(uids))
        plans: dict[str, list[Plan]] = {}
        for line in _iter_lines(path):
            if not line.startswith('{"JsonScheduleV1"'):
                continue
            s = json.loads(line)["JsonScheduleV1"]
            uid = s["CIF_train_uid"].strip()
            if uid not in uids:
                continue
            plan = plan_from_cif(s)
            if s.get("transaction_type", "Create") == "Delete":
                plans[uid] = [p for p in plans.get(uid, []) if not (p.start == plan.start and p.stp == plan.stp)]
            else:
                plans.setdefault(uid, []).append(plan)

        with self._lock:
            # keep VSTP plans received from Kafka across reloads
            for uid, existing in self.plans.items():
                vstp = [p for p in existing if p.source == "V"]
                if vstp:
                    plans.setdefault(uid, []).extend(vstp)
            self.plans = plans
            self.locations = locations
            self.by_stanox = {}
            for loc in locations.values():
                if loc.stanox and loc.stanox not in self.by_stanox:
                    self.by_stanox[loc.stanox] = loc
            self.source_file = str(path)
            self.loaded_at = dt.datetime.now(dt.timezone.utc)
        log.info("CIF loaded: %d UIDs, %d schedule records", len(plans), sum(len(v) for v in plans.values()))

    def apply_vstp(self, msg: dict) -> Optional[Plan]:
        txn, plan = plan_from_vstp(msg)
        if not plan.uid:
            return None
        with self._lock:
            existing = self.plans.get(plan.uid, [])
            same = lambda p: p.source == "V" and p.start == plan.start and p.stp == plan.stp  # noqa: E731
            if txn == "Delete":
                if existing:
                    self.plans[plan.uid] = [p for p in existing if not same(p)]
                return None
            touches = any(s.tiploc in self.watched for s in plan.stops)
            if not touches and plan.uid not in self.plans:
                return None
            self.plans[plan.uid] = [p for p in existing if not same(p)] + [plan]
            return plan

    # ---- queries ---------------------------------------------------------------------------
    def resolve(self, uid: str, day: dt.date) -> Optional[Plan]:
        """The effective schedule for a UID on a date (None if cancelled / not running)."""
        with self._lock:
            cands = [p for p in self.plans.get(uid, []) if p.runs_on(day)]
        if not cands:
            return None
        # VSTP beats CIF at equal STP rank
        best = min(cands, key=lambda p: (STP_RANK.get(p.stp, 9), 0 if p.source == "V" else 1))
        return None if best.stp == "C" else best

    def plans_on(self, day: dt.date) -> list[Plan]:
        """All effective (non-cancelled) schedules we hold for a date."""
        with self._lock:
            uids = list(self.plans)
        return [p for p in (self.resolve(uid, day) for uid in uids) if p]

    def plans_at(self, tiploc: str, day: dt.date) -> list[Plan]:
        with self._lock:
            uids = list(self.plans)
        out = []
        for uid in uids:
            p = self.resolve(uid, day)
            if p and p.stop_index(tiploc) is not None:
                out.append(p)
        return out

    def stanox_of(self, tiploc: str) -> Optional[str]:
        loc = self.locations.get(tiploc)
        return loc.stanox if loc else None

    def name_of_tiploc(self, tiploc: str) -> str:
        loc = self.locations.get(tiploc)
        return loc.name if loc else tiploc

    def name_of_stanox(self, stanox: str) -> str:
        loc = self.by_stanox.get(stanox)
        return loc.name if loc else stanox


def newest_cif(path: Path) -> Optional[Path]:
    """A file, or the newest *.json.gz / *.json in a directory."""
    path = Path(path)
    if path.is_file():
        return path
    if path.is_dir():
        files = [p for p in path.iterdir() if p.name.endswith((".json.gz", ".json"))]
        return max(files, key=lambda p: p.stat().st_mtime) if files else None
    return None
