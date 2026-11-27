# cc1101-afec-logs-parser docs

Host-side tooling for the AFEC thesis measurement campaign. It drives the two nodes over serial, captures their JSON-lines logs, and turns them into the metrics of the evaluation design (report chapter 2, `../docs/03-evaluation-design.md` in the parent repo).

| File | What it covers |
| --- | --- |
| [campaign.md](campaign.md) | `campaign.py`: running a phase, the plan file format, `meta.json`, failure handling, resume, the interferer hooks |
| [metrics.md](metrics.md) | `metrics.py`: every output table and column, metric definitions, how sender and receiver events are joined, known approximations |
| [parse-and-eda.md](parse-and-eda.md) | `parse.py` (logs → `events.csv`, block context, clock sync) and `eda.py` (single-node plots) |

## Pipeline

```
 plans/<phase>.json
        │
        ▼
 campaign.py ──serial──► sender (node 2) ─┐
        │      commands   receiver (node 1)├─► raw/sender.log, raw/receiver.log  (JSON lines, untouched)
        │                 [interferer]    ─┘    meta.json  (plan, schedule, commits, per-block results)
        ▼
 metrics.py ──uses──► parse.py (load_files: parse, mark context, --sync clock offsets)
        │
        ▼
 out/packets.csv, blocks.csv, cells.csv, attempts.csv, [tconv.csv], [bench.csv]

 parse.py --mode file ... → out/events.csv, by_event/*.csv   (generic event tables)
 eda.py events.csv        → eda_overview.png / eda_receiver.png  (one node, quick look)
```

The firmware side of the command layer (commands, firmware events such as `app_tx`, `app_rx`, `mark`, `block_end`, `bench`) is documented in `../cc1101-afec-sender/docs/firmware.md`. Library events (`send`, `tx`, `rx_data`, `lvl_step`, …) are in `../cc1101-afec/docs/afec_log_schema.md`.

## Setup

```sh
uv venv --python 3.13 .venv            # system python3 on the bench Mac is 3.9, too old (needs >= 3.10)
uv pip install -r requirements.txt     # pandas 3, numpy, matplotlib, pyserial
```

Behind the TLS-intercepting proxy, `uv` needs `--system-certs` (or `SSL_CERT_FILE=~/.config/certs/macos-bundle.pem`). Run every script with `.venv/bin/python`.

## Data layout

`campaign.py` writes to the parent repo's `data/` (`--data` to change it):

```
data/<run_id>/            e.g. 2026-11-30-Q, 2026-11-28-pilot-Q
  meta.json               plan, schedule + seed, git commits, node builds, per-block results, final stats
  raw/sender.log          everything the node printed, untouched (non-JSON boot lines included)
  raw/receiver.log
  raw/interferer.log      once the interferer exists
  out/                    metrics.py output
```

`data/` is gitignored in the parent repo **on purpose**: campaign data is not committed, and the author persists it outside git. Don't add it to git or suggest LFS.
