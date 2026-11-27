# CLAUDE.md

This is the host-side tooling for the AFEC thesis measurement campaign: the orchestrator that drives the nodes over serial, the log parser, the metrics, and quick EDA plots. It is a submodule of the thesis repo `afec`. Read `docs/README.md` first. For the firmware commands and events, see `../cc1101-afec-sender/docs/firmware.md`; for library events, `../cc1101-afec/docs/afec_log_schema.md`; for the metric definitions in the thesis, `../docs/03-evaluation-design.md`; for the campaign, `../docs/07-run-plan.md`.

## Layout

- `campaign.py`: runs one phase from `plans/<phase>.json` (schedule, `cfg`/`mark`/`run` per block, failure handling, `meta.json`, resume), then calls `metrics.py`. See `docs/campaign.md`.
- `metrics.py`: per-packet, per-block and per-cell metrics (Wilson CIs and block bootstrap), T_conv, and the bench table. See `docs/metrics.md`.
- `parse.py`: JSON lines → DataFrame or CSV, the `mark` block context and the clock sync. `metrics.py` imports `load_files` from it. See `docs/parse-and-eda.md`.
- `mqtt_logs.py`: reads the nodes' log lines from an MQTT broker (`afec/<run_id>/<tx|rx>/log`). Used by `campaign.py --mqtt` and `parse.py --mode mqtt`. See "Log transport" in `docs/campaign.md`.
- `eda.py`: per-cell campaign figures from `metrics.py` output (`--campaign`), and a single-node diagnostic from `events.csv`.
- `plans/`: phase plans. `pilot-Q`, `Q` and `M` (whose TX power and geometry are still placeholders), and the `S` template (interferer on `/dev/cu.usbserial-210`, firmware in `../cc1101-afec-interferer`; still needs P3's appropriate levels).

## Commands

```sh
.venv/bin/python campaign.py plans/pilot-Q.json --dry-run             # schedule only, safe
.venv/bin/python campaign.py plans/pilot-Q.json [--resume]            # drives the hardware: ask first
.venv/bin/python metrics.py ../data/<run>/raw --out ../data/<run>/out --kw 10
.venv/bin/python parse.py --mode file sender.log receiver.log --sync --out out/
.venv/bin/python eda.py --campaign ../data/<run>/out --out ../data/<run>/figs
.venv/bin/python eda.py out/events.csv --out figs/
```

- **Python:** use `.venv` (Python 3.13). The system `python3` is 3.9 and too old. To recreate it: `uv venv --python 3.13 .venv && uv pip install -r requirements.txt`. Behind the TLS proxy, `uv` needs `--system-certs`.
- **Ports on the bench Mac:** sender (node 2) is `/dev/cu.usbserial-5`, receiver (node 1) is `/dev/cu.usbserial-0001`. `campaign.py` checks them with `hello`. Opening a port resets the board.
- **Output:** campaign data goes to `../data/<run_id>/`, which is gitignored on purpose: the author persists campaign data outside git. Never commit it.

## Conventions

- **Event names and fields are a contract with the firmware and the library.** If a firmware event changes (`app_tx`, `app_rx`, `mark`, `block_end`, `cfg_ok`, `bench`), update `metrics.py` and `campaign.py` together, and the docs on both sides.
- **Metric definitions follow the thesis (§2.2 and §2.4).** If you change a definition, update `docs/metrics.md`, and check that it still matches `../docs/03-evaluation-design.md` and the report.
- **Never modify raw logs.** All derived data goes to `out/`. `meta.json` is written only by `campaign.py`.
- **Freeze before the main campaign:** `retries`, `ackto`, `dg` and the C4 policy in the plans come from the pilot and P2, and must be identical across conditions.
- **No test suite** (as with the rest of the project). Check changes on simulated logs (see "Verification" in `docs/metrics.md`) or on a short bench run (`--blocks 1 --packets 12`).
- **Committing:** this is its own repo. Commit here first, then commit the updated submodule pointer in the parent `afec` repo. Update `../docs/05-code-map.md` and `../docs/06-status-and-open-items.md` when the tooling changes in a way that matters.
