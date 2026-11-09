# freight-tracker

Live **freight / non-passenger train board for any timing point** (TIPLOC), built from
Network Rail open data via the Rail Data Marketplace. Written to drive a "freight mode" on the
ESP32 departures board, starting with **Ashton Junction, Bristol (`ASHJN`)**, which has no
station and never appears in National Rail (Darwin) data.

For each train due at a watched location it serves:

- booked time at the location (pass or call),
- whether it's **activated** (running today) or cancelled (with reason code),
- **operator** (from TRUST; e.g. GBRf),
- headcode, origin → destination,
- an **expected time** estimated from its latest TRUST report (e.g. "Parson Street 10:53, 1 late").

See [DESIGN.md](DESIGN.md) for why the data sources were chosen, what was verified, and the rules.

## How it works (short)

- **Timetable**: a CIF JSON file (`*.json.gz`, RDM "SCHEDULE" product) is scanned for trains
  touching the watched TIPLOCs. Checked every 30 min; a newer file in `cif/` is picked up automatically.
- **Live**: consumes the RDM Kafka topic `TRAIN_MVT_ALL_TOC` (and optionally VSTP) and tracks each
  matching train's activation, reports and cancellations.
- **Output**: `GET /board/<TIPLOC>.json` over HTTP (port 8090), and `out/<TIPLOC>.json` files.

## Setup

Requires Python 3.11+ and Poetry.

```bash
cd freight-tracker
poetry config virtualenvs.in-project true --local
poetry install                      # add `-E s3` to fetch the CIF from S3
cp config.example.toml config.toml  # set `watch = ["ASHJN", ...]`
cp secrets.env.example secrets.env && chmod 600 secrets.env   # fill in RDM Kafka credentials
mkdir -p cif && cp ~/Downloads/CIF_ALL_FULL_DAILY_toc-full.json.gz cif/
```

Kafka credentials come from the RDM subscription page for `TRAIN_MVT_ALL_TOC`
(bootstrap server, username, password, consumer group). VSTP is optional (`VSTP_*`).

## Running

Foreground (for trying it out):

```bash
poetry run python -m freight_tracker run --config config.toml
curl -s localhost:8090/board/ASHJN.json | python3 -m json.tool
curl -s localhost:8090/health
```

As a background service on the home server (no root needed), with a cron watchdog:

```bash
chmod +x scripts/*.sh
scripts/ensure_running.sh          # starts it if not running
crontab -e                         # add:
# @reboot      /home/robbie/freight-tracker/scripts/ensure_running.sh
# */5 * * * *  /home/robbie/freight-tracker/scripts/ensure_running.sh
```

Logs: `logs/freight-tracker.log` (previous run: `freight-tracker.prev.log`), restarts in
`logs/supervisor.log`. Stop with `scripts/stop.sh` (cron restarts it within 5 min unless you
remove the crontab lines).

The deployed copy lives at `~/freight-tracker` on 192.168.68.137 and serves
`http://192.168.68.137:8090/board/ASHJN.json`.

## Offline board (no Kafka)

Print a board from a CIF file, optionally replaying captured TRUST messages up to a time:

```bash
poetry run python -m freight_tracker board --cif cif/CIF_ALL_FULL_DAILY_toc-full.json.gz --tiploc ASHJN
poetry run python -m freight_tracker board --cif tests/fixtures/cif_ashjn_sample.json.gz --tiploc ASHJN \
    --replay tests/fixtures/trust_ashjn_20261109.jsonl --at 2026-11-09T10:55
```

## Board JSON

```json
{"loc":"Ashton Junction","tiploc":"ASHJN","generated":"2026-11-09T10:55:00+00:00",
 "svcs":[{"t":"10:57","half":false,"d":0,"hc":"6C92","op":"GBRf","toc":"54",
          "from":"Margam TC GBRF","to":"Portbury Auto Terminal","type":"CALL","st":"RUN",
          "exp":"10:58","var":1,"last":"Parson Street 10:53","cat":"B1","reason":"","uid":"H01980"}]}
```

`st`: `NOT` not activated · `ACT` activated, not moving yet · `RUN` running (`exp` estimated) ·
`PASSED` gone by (reported or inferred) · `CAN` cancelled. `d`: day offset of the booked time
(1 = tomorrow). `half`: WTT half-minute (e.g. 10:43½).

## Adding locations

Find the TIPLOC (e.g. from the CIF `TiplocV1` records or realtimetrains.co.uk), add it to
`watch` in `config.toml` and restart. Timing points with no TRUST reporting of their own get
estimates from the nearest reporting points either side.

## Operators

`operators.json` maps TRUST `toc_id` → short/long name. Only IDs confirmed from live data are
filled in (54 GB Railfreight, 09 Freightliner Heavy Haul); unknown IDs show as the raw number.
Add entries as you confirm them.

## Tests

```bash
poetry run pytest
```

Tests replay real TRUST messages captured for the Ashton Jn trains on 9 Nov 2026 against a
real CIF extract (`tests/fixtures/`, Network Rail data under the Open Government Licence).

## Data licence

Contains Network Rail Infrastructure Limited information licensed under the Open Government
Licence, via the Rail Data Marketplace.
