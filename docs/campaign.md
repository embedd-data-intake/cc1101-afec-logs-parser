# campaign.py — the orchestrator

Runs one campaign **phase** (one operating condition) from a JSON plan: all blocks of all configurations, in a seeded randomised order, on the sender and receiver connected over USB. Run plan item P0.3.

```sh
.venv/bin/python campaign.py plans/pilot-Q.json                    # run a phase
.venv/bin/python campaign.py plans/Q.json --dry-run                # print schedule + duration, touch nothing
.venv/bin/python campaign.py plans/Q.json --resume                 # continue an interrupted run
.venv/bin/python campaign.py plans/Q.json --blocks 1 --packets 12  # quick smoke run
```

| Option | Meaning |
| --- | --- |
| `--data DIR` | Data root. Default: the parent repo's `data/` |
| `--run-id ID` | Override the plan's `run_id` (needed to repeat a plan: an existing `meta.json` is never overwritten) |
| `--blocks N`, `--packets N`, `--configs C2,C5` | Override blocks per configuration, packets per block, or the configuration set |
| `--retries N` | Re-runs of a failed block (default 2). After that the run stops. |
| `--resume` | Continue `<data>/<run_id>`. The schedule must be identical (same plan and overrides). |
| `--no-metrics` | Skip the final `metrics.py` call |

## Plan files (`plans/`)

| File | Use |
| --- | --- |
| `pilot-Q.json` | P1 pilot in Q: C2, C4, C5 × 3 blocks × 60 packets, ~20 min |
| `Q.json` | Main Q phase: C1, C2, C4, C5 × 18 blocks × 110 packets, ~4.6 h |
| `M.json` | Main M phase. **TX power and geometry are placeholders**: fill in after the M calibration. |
| `S.json` | **Template.** Needs the interferer firmware (P0.2) and the appropriate levels from P3. |

Freeze `retries`, `ackto`, `dg` and the C4 policy (`rssi_*`, `hys`) from the pilot and P2 before any main phase, and keep them identical across Q, M, B and S.

### Format

| Key | Example | Meaning |
| --- | --- | --- |
| `run_id` | `"2026-11-30-Q"` | Directory name under `data/`, and the `mark run=` tag in the logs |
| `condition` | `"Q"` | Q, M, B or S. Passed to `metrics.py --condition`. |
| `seed` | `1` | Schedule seed (stored in `meta.json`) |
| `blocks_per_config` | `18` | Blocks per configuration |
| `packets` | `110` | Packets per block, warm-up included |
| `kw` | `10` | Warm-up dropped by `metrics.py` (S: 0) |
| `len`, `interval` | `32`, `2000` | Payload bytes; ms between packet starts |
| `common` | `{"init": 2, "retries": 3, "ackto": 400, "tx": 0, "dg": 2, "nackstep": 2}` | `cfg` keys sent to **both** nodes for every configuration |
| `configs` | `{"C2": {"mode": 2}, "C4": {"mode": 4, "rssi_up": "11,9,7,5,3", ...}}` | Per-configuration `cfg` keys, also sent to both nodes. `mode: 3` (C3) is refused. |
| `sender`, `receiver` | `{"tx": -30}` | Optional per-node overrides, e.g. the M TX power |
| `ports` | `{"sender": "/dev/cu.usbserial-5", "receiver": "/dev/cu.usbserial-0001"}` | Serial ports |
| `interferer` | see below | Optional |
| `metrics` | `["--steps", "0:Q,50:B,130:Q", "--appropriate", "Q=0-1,B=3-5"]` | Extra `metrics.py` arguments |
| `notes` | `{"geometry": "..."}` | Free-form: geometry, distance, TX power rationale, environment. Copied into `meta.json`. |

The `cfg` keys are the firmware's (`mode`, `init`, `retries`, `ackto`, `tx`, `dg`, `nackstep`, `rssi_min`, `rssi_max`, `rssi_up`, `hys`). The order of precedence is `common` < `configs[Cx]` < `sender`/`receiver`.

## What a run does

1. **Open both ports.** On the CP2102 boards this resets the ESP32s. Wait 2.5 s, send `hello` to each and check `role`. Swapped ports stop the run with a message.
2. **Schedule: permuted rounds.** For each of `blocks_per_config` rounds, the configuration list is shuffled with `random.Random(seed)`, so every round contains each configuration once. This spreads slow drift (ambient noise, temperature) evenly over the configurations.
3. **Per block:**
   1. Receiver first, then sender: `cfg …`, wait for `cfg_ok` and check that its `cfg` and every requested field match; then `mark run=<run_id> block=<n> cfg=<Cx>`. The firmware's `cfg` already re-initialises the radio and node and resets the link, so no separate `reset` is sent.
   2. Interferer `on` (if `during_block`).
   3. `run n=<packets> len=<len> interval=<interval>` on the sender, then wait for `block_end`. If `steps` are set, the interferer commands are sent when the sender's `app_tx` reaches each packet index.
   4. Interferer `off`.
4. **After every block** `meta.json` is rewritten atomically, and a progress line with an ETA is printed.
5. **At the end:** `stats` from both nodes into `meta.json`, then `metrics.py raw --out out --kw <kw> --condition <condition> [--exclude-blocks <failed>] [plan metrics args]`.

Block ids are assigned as the run goes (0, 1, 2, …) and are never reused, including across `--resume`. A block's `mark` therefore identifies it uniquely in both logs.

## Failure handling

A block **fails** when:
- a node logs `cmd_err`;
- `cfg_ok` doesn't match the request;
- a node reboots (a new `CC1101 ready` line);
- `run_start` or `cfg_ok` doesn't arrive in time;
- the block takes longer than `packets × max(interval, (1 + retries) × (ackto + 150 ms)) + 30 s`. The sender then gets `stop`.

A failed block is recorded (`status: failed`, `error`) and re-run under a fresh block id, up to `--retries` times. After a reboot the orchestrator waits 3 s first. If an entry still fails, the run stops.

`metrics.py` leaves out failed and interrupted blocks in two ways: through `--exclude-blocks`, and because it only counts blocks for which the sender logged `block_end aborted=0`.

**Ctrl-C or SIGTERM:** the sender gets `stop`, the block in progress is recorded as `interrupted` (so its id is never reused), `meta.json` gets `status: interrupted`, and the ports are closed. `--resume` reopens the ports, which reboots the nodes, appends to the raw logs and continues with the next schedule entry that isn't `ok`.

**Tested on the bench (2026-11-27):** a normal run, interrupt + resume. **Not yet tested:** reboot detection (press a board's reset during a block) and everything involving the interferer.

## meta.json

| Key | Content |
| --- | --- |
| `run_id`, `condition`, `created`, `ended`, `host`, `status` | `status` is `running`, `done` or `interrupted` |
| `plan`, `plan_file` | The full plan after CLI overrides |
| `schedule`, `schedule_rule` | Configuration per schedule index |
| `firmware` | `{commit, dirty}` for the lib, sender, receiver, parser and thesis repos at run start. **Commit everything before a real run**, so that `dirty` is false. |
| `radio` | Frequency, sync word, bitrate, whitening, HW CRC (fixed in `config.h`) |
| `sessions[]` | One per start or resume: time, the `hello` of each node (port, node id, build timestamp), firmware commits |
| `blocks[]` | `sched_idx`, `block`, `cfg`, `attempt`, `started`, `ended`, `status` (`ok`, `failed`, `interrupted`), and for `ok`: `ok`, `no_ack`, `duty_limit`, `dur_ms` from `block_end`; for failures, `error` |
| `cfg_ok` | The first `cfg_ok` of each node per configuration: the effective configuration as the nodes reported it |
| `final_stats` | `stats` of both nodes at the end (cumulative since boot) |

## Interferer hooks (untested; for P0.2)

```json
"interferer": {"port": "/dev/cu.usbserial-X",
               "setup": ["burst len=10 period=200 jitter=80"],
               "during_block": true,
               "steps": [[50, "on"], [130, "off"]]}
```

- `setup` lines are sent once after the handshake.
- `during_block` (B): `on` before `run` and `off` after it.
- `steps` (S): each command is sent when the sender has logged `app_tx` for packet `idx + 1 ≥ step`, so the switch lands between two packets. Polling adds up to ~0.2 s of latency, which is far below the 2 s interval.
- The interferer log goes to `raw/interferer.log`. Its port is not `hello`-checked and its git commit is not yet recorded in `meta.json`. Add both once the firmware exists.

## Timing

At a 2 s interval, a 110-packet block takes ~220 s, plus ~5 s for `cfg` and `mark`. The `--dry-run` estimate uses 10 s of switching overhead per block, so it errs on the long side.
