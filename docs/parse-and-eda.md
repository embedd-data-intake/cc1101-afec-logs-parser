# parse.py and eda.py

## parse.py — logs to tables

Turns JSON-lines logs (from files or a live serial port) into a pandas DataFrame and CSV files. It is generic: any line `{"t":…,"node":…,"lv":…,"ev":…, …}` becomes a row, so new firmware or library events need no parser change. `metrics.py` imports `load_files` and `events` from it.

```sh
.venv/bin/python parse.py --mode file sender.log receiver.log --sync --out out/ [--summary] [--parquet]
.venv/bin/python parse.py --mode serial --port /dev/cu.usbserial-5 --duration 300 --raw-log sender.log --out out/
```

```python
from parse import load_files, events
df = load_files(["sender.log", "receiver.log"], sync=True)
ok = events(df, "send_ok")            # one event type, empty columns dropped
```

**Outputs:**
- `events.csv`: every event, one wide row each (NaN where a field doesn't apply).
- `by_event/<ev>.csv`: one file per event type, with all-NaN columns dropped.
- `offsets.json` (with `--sync`).

**Columns added:**

| Column | Meaning |
| --- | --- |
| `source`, `line` | Input file name and line index (per file). Per-node order is `(source, line)`. |
| `t_s` | Node-local time in seconds |
| `mark_run`, `mark_block`, `mark_cfg` | Campaign logs only: the `run`, `block` and `cfg` of the last firmware `mark` earlier in the same file. Forward-filled per file, NaN before the first mark. |
| `mode_name`, `<lvl col>_name`, `from_name`, `to_name` | Readable names: mode `0..4` → `RAW(C1)` … `AFEC(C5)`; levels 0–5 → `Hamming` … `RS8`, 7 → `none` |
| `t_sync`, `t_sync_s` | With `--sync`: time on the reference node's clock (ms), and seconds since the first event |

**Line handling.** A line without `{` is skipped and counted as "non-JSON" (boot banner, `CC1101 ready`). A line with `{` that doesn't parse, or has no `t`/`ev`, counts as "broken", which is typical of a truncated serial line. Both counts are printed per file.

**Clock sync (`--sync`).** The nodes' `millis()` clocks are independent. For each pair of nodes, sender DATA `tx` events are matched with receiver DATA `rx` events of the same `seq`:
1. A rough offset is taken as the median over the first occurrence of each seq.
2. Then each `rx` is mapped to the nearest same-seq `tx` within ±2 s, which handles retries and seq wrap-around, and the median is taken again.

The two directions are averaged. The lowest node id is the reference. The offset includes airtime and processing, a few ms, which is fine for attempt attribution but not for sub-ms timing.

**Serial mode.** Reads with pyserial, tees the raw capture to `--raw-log`, and stops after `--duration` s, `--max-lines` lines or Ctrl-C. `--sync` doesn't work here (a single port). For campaign runs use `campaign.py`, which captures both nodes.

## eda.py — quick single-node look

A diagnostic for one node's `events.csv`. It predates the command layer and looks at library events only, not `mark`, `app_tx` or `app_rx`. Use it for debugging a link, not for campaign results.

```sh
.venv/bin/python eda.py out/events.csv [--node 2] [--role auto|sender|receiver] [--out figs/]
```

- **Role:** picked automatically (more `send` events → sender, more `rx_data`/`ack_sent` → receiver). The node with the most such events is used, unless `--node` is given.
- **Sender view:** prints delivery (ok / fail, first attempt vs after retries, RTT), outage episodes (runs of failed messages), TX levels and airtime (including airtime spent on un-ACKed attempts, and a fitted ms/byte), the duty budget, and ACK link quality (RSSI, LQI, fb). It writes `eda_overview.png`: TX level per attempt, ACK RSSI/LQI, attempts per message, airtime per frame. Messages are identified by counting `send` events, so seq wrap-around is harmless.
- **Receiver view:** prints frames heard by level (RSSI, LQI, `corr`), decode failures, cadence gaps and seq resets, and ACK turnaround (rx → ACK sent = a fixed part + ACK airtime). It writes `eda_receiver.png`.

## Known issues (recorded, not fixed)

- The docstrings still use the old script names: `afec_logs.py` in `parse.py` and `afec_eda.py` in `eda.py`.
- `eda.py` fixes the LQI axis to 40–60, so values outside that range are clipped from the plot.
- **Missing:** `eda.py` has no per-cell or per-block campaign plots yet (the rest of run plan P0.4). They should read `metrics.py` output, not `events.csv`.
