"""VSTP handling. The message below follows the NROD VSTP JSON layout; it is synthetic
(no live RDM VSTP sample yet), so these tests pin the parser's behaviour rather than
prove the feed format."""
import datetime as dt

from conftest import CIF

from freight_tracker.timetable import Timetable


def vstp(uid="Z12345", txn="Create", tiploc="ASHJN"):
    return {
        "VSTPCIFMsgV1": {
            "schedule": {
                "transaction_type": txn,
                "CIF_train_uid": f" {uid}",
                "CIF_stp_indicator": "N",
                "schedule_start_date": "2026-11-10",
                "schedule_end_date": "2026-11-10",
                "schedule_days_runs": "0100000",
                "train_status": "2",
                "schedule_segment": [
                    {
                        "signalling_id": "6Z99",
                        "CIF_train_category": "B1",
                        "schedule_location": [
                            {"scheduled_departure_time": "130000", "location": {"tiploc": {"tiploc_id": "PTBRATG"}}},
                            {"scheduled_pass_time": "131530", "location": {"tiploc": {"tiploc_id": tiploc}}},
                            {"scheduled_arrival_time": "170000", "location": {"tiploc": {"tiploc_id": "MRGMTCG"}}},
                        ],
                    }
                ],
            }
        }
    }


def fresh():
    tt = Timetable({"ASHJN"})
    tt.load_cif(CIF)
    return tt


def test_vstp_create_adds_train():
    tt = fresh()
    plan = tt.apply_vstp(vstp())
    assert plan and plan.headcode == "6Z99" and plan.source == "V"
    at = [p for p in tt.plans_at("ASHJN", dt.date(2026, 11, 10)) if p.uid == "Z12345"]
    assert at and at[0].stops[1].minutes == 13 * 60 + 15.5 and at[0].stops[1].is_pass


def test_vstp_not_touching_watch_is_ignored():
    tt = fresh()
    assert tt.apply_vstp(vstp(tiploc="BRSTLTM")) is None
    assert "Z12345" not in tt.plans


def test_vstp_delete_removes():
    tt = fresh()
    tt.apply_vstp(vstp())
    tt.apply_vstp(vstp(txn="Delete"))
    assert not [p for p in tt.plans_at("ASHJN", dt.date(2026, 11, 10)) if p.uid == "Z12345"]


def test_vstp_survives_cif_reload():
    tt = fresh()
    tt.apply_vstp(vstp())
    tt.load_cif(CIF)
    assert any(p.uid == "Z12345" for p in tt.plans_at("ASHJN", dt.date(2026, 11, 10)))
