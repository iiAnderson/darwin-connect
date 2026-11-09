# freight-tracker — design

A departures-board-style feed of **non-passenger movements (freight, light engines, ECS)
at non-station timing points** such as Ashton Junction, Bristol (`ASHJN`), for the
ESP32 departures board in [departures-board](https://github.com/iiAnderson/departures-board).

For each train due at a watched location it answers: when it's booked, whether it's
running (activated), who runs it, and roughly when it'll actually be there.

## 1. Why these data sources

| Source | Freight? | Notes |
|---|---|---|
| National Rail Darwin (LDBWS/RDG boards, Push Port, our `darwin-storage`) | **No** | Checked 30 Oct 2026: zero class 4/6/7/8 headcodes nationally, no rows at `ASHJN`. Passenger + ECS only. |
| Realtime Trains API (data.rtt.io) free tier | No | "Simple mode": passenger calls only; `detailed=true` ignored. Paid tiers unverified for freight. |
| **Network Rail open data via Rail Data Marketplace** | **Yes** | TRUST movements on Kafka + CIF timetable file. Used here. |

Inputs:

- **CIF timetable (JSON, gzipped)** — RDM "SCHEDULE" product, delivered as a file (manually, or
  pushed to S3 by RDM). The full daily file is ~133 MB gz / ~714k schedules and also contains
  `TiplocV1` records (TIPLOC → STANOX, name), so CORPUS isn't needed.
- **TRUST train movements** — RDM Kafka topic `TRAIN_MVT_ALL_TOC` (Confluent Cloud, SASL/PLAIN).
  Messages are JSON arrays of `{header, body}`; `0001` activation, `0002` cancellation,
  `0003` movement, `0005` reinstatement, `0006` change of origin, `0007` change of identity,
  `0008` change of location. Retention is size-based: **~4.5 h on a Sunday evening but only
  ~1.5 h on a weekday afternoon** (measured 9 Nov 2026).
- **VSTP** (optional, recommended) — short-notice schedules on their own Kafka topic. Several
  `ASHJN` trains are STP/`Z`-headcode paths, and some freight only ever exists as VSTP.

## 2. What we verified (8–9 Nov 2026)

- `ASHJN` = STANOX **81823**. Weekdays have 4–6 booked trains (Margam ⇄ Portbury flows,
  4M00 Portbury → Northampton, a light engine from Bristol East Depot); none at weekends.
- Freight schedules carry `atoc_code = "ZZ"`: **operator only comes from TRUST `toc_id`**.
  Confirmed from data: `54` GB Railfreight, `09` Freightliner Heavy Haul. `05` is unconfirmed.
- **TRUST never reports at Ashton Jn.** 6Z56 departed Parson Street (81819) with
  `next_report_stanox = 81823`, then the next message was its arrival at Portbury.
  Parson Street (~1 mile away) is the last report inbound; Portbury (81827/81824) outbound.
- Activation arrives 1–10 h before departure (6C92 activated 00:36 for 07:41). A train
  that is never activated (4M00 on 9 Nov) simply didn't run.
- Freight runs very early or late (6C92 left Margam 41 min early, then was held back to
  right time by Bristol). Terminal reports can be very late: 6Z56's Portbury arrival
  reached Kafka 75 minutes after the event.
- Headcodes are reused within a day (two different 0B92s on 9 Nov). Match on
  `train_uid` + `tp_origin_timestamp` (the service date) from the activation, never on headcode.
  `schedule_start_date` in the activation is the *schedule record's* start, not the run date.

## 3. Architecture

```
RDM SCHEDULE (CIF .json.gz) ──file / S3──► Timetable (two streaming passes, every 30 min check)
                                              keeps: TIPLOC table + every record for UIDs
                                              that touch a watched TIPLOC (incl. overlays/cancels)
RDM Kafka TRAIN_MVT_ALL_TOC ─┐
RDM Kafka VSTP (optional) ───┴─► KafkaSource ──► Tracker (state per TRUST train_id)
                                                    │  state.json every 60 s (resume on restart)
                                                    ▼
                                    board(TIPLOC, now) ──► HTTP  GET /board/<TIPLOC>.json
                                                       └─► out/<TIPLOC>.json (optional)
                                                                      │
                                                     ESP32 board polls every ~60 s (LAN)
```

Runs as one Python process on the home server (192.168.68.137). Cost: £0.

### Timetable
- Pass 1 collects `TiplocV1` and UIDs of schedules containing a watched TIPLOC; pass 2 collects
  **all** records for those UIDs (so an `O` overlay that diverts a train away, or a `C`
  cancellation with no locations, is honoured). Swap-in is atomic under a lock.
- STP resolution per (UID, date): candidates whose date range and days-run match; lowest of
  `C < N < O < P` wins (VSTP beats CIF at equal rank); `C` ⇒ not running.
- WTT times (`HHMM[H]`, VSTP `HHMMSS`) become minutes after midnight of the service date,
  made monotonic across midnight.
- VSTP Create/Delete messages are merged in and survive CIF reloads.

### Tracker
Keyed by TRUST `train_id`, linked to (UID, service date) at activation. Stores: operator,
cancelled (+ reason/type/location), reinstated, reported STANOX list, last report and its
signed variation (`EARLY` → negative, `LATE` → positive, `OFF ROUTE` → ignored for estimates).
`0007` re-keys the train. It also remembers the last operator seen per UID, so tomorrow's
not-yet-activated trains still show an operator.

### Board rules (per watched TIPLOC)
For yesterday/today/tomorrow's effective schedules touching the TIPLOC:

| `st` | Meaning |
|---|---|
| `NOT` | Booked, not activated. Dropped 30 min after booked time (didn't run). |
| `ACT` | Activated, no movement report yet (waiting to depart). |
| `RUN` | Moving; `exp` = booked time at the TIPLOC + latest on-route variation. |
| `PASSED` | Reported at/after the TIPLOC, **or** estimate is >10 min in the past (inferred, since junctions rarely report). Removed 15 min after the estimate. |
| `CAN` | Cancelled at origin or at/before the TIPLOC. Dropped 30 min after booked time. |

Estimates deliberately use WTT time + variation minutes rather than TRUST epoch timestamps, so
there's no timezone ambiguity in BST. `last` ("Parson Street 10:53") is computed the same way;
off-route reports fall back to the TRUST timestamp.

### Output (consumed by the ESP32)
```json
{"loc":"Ashton Junction","tiploc":"ASHJN","generated":"2026-11-09T10:55:00+00:00",
 "svcs":[{"t":"10:57","half":false,"d":0,"hc":"6C92","op":"GBRf","toc":"54",
          "from":"Margam TC GBRF","to":"Portbury Auto Terminal","type":"CALL","st":"RUN",
          "exp":"10:58","var":1,"last":"Parson Street 10:53","cat":"B1","reason":"","uid":"H01980"}]}
```
Sorted by expected (else booked) time, max 9 rows, < 4 KB. `d` = day offset of the booked time
(1 = tomorrow). `half` = WTT half-minute.

## 4. Resilience
- Kafka offsets are never committed. On start the service seeks by timestamp to 60 s before the
  last processed message (from `state.json`), floored at `max_rewind_minutes`. Handlers are
  idempotent, so overlap is harmless.
- Because retention can be as short as ~1.5 h but activations arrive up to 10 h before departure,
  a fresh start or an outage loses activations. Movements from an unknown `train_id` are therefore
  **linked without the activation**: headcode (`train_id[2:6]`) + day of month (`train_id[8:10]`)
  + a scheduled stop at the reported STANOX within 2 min of the report's planned time (tried as
  UTC and UK local). Such trains are marked `source = "inferred"`. Until a train's first movement
  it shows `NOT` even if it was activated during the gap.
- A cron watchdog (`scripts/ensure_running.sh`, every 5 min + `@reboot`) restarts the process;
  no root/systemd needed.
- `/health` reports last Kafka message time, message counts, tracked trains and timetable file.

## 5. Not done yet / next steps
1. **ESP32 freight mode** in the departures-board fork: new `MODE_FREIGHT` client that GETs
   `/board/<TIPLOC>.json` (plain HTTP on the LAN) and maps rows onto the existing service struct.
2. **RDM → S3 schedule delivery** + `cif_s3_uri` (implemented, untested) so the CIF refreshes
   daily without manual downloads. Until then, drop a fresh file into `cif/`.
3. **VSTP topic** subscription (parser written to the NROD layout, verify against live messages).
4. Confirm operator IDs (`05`, `42`, `97`, …) and decode cancellation reason codes (e.g. `FL`).
5. Optional history table (`nonpax_normalised_v1` in `darwin-storage`) from persisted raw
   messages, and a public endpoint via CloudFront/Cloudflare Tunnel if the board leaves the LAN.
