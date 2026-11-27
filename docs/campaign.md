# campaign.py — the orchestrator

Runs one campaign **phase** (one operating condition) from a JSON plan: all blocks of all configurations, in a seeded randomised order, on the sender and receiver connected over USB. Run plan item P0.3.

```sh
.venv/bin/python campaign.py plans/pilot-Q.json                    # run a phase
.venv/bin/python campaign.py plans/Q.json --dry-run                # print schedule + duration, touch nothing
.venv/bin/python campaign.py plans/Q.json --resume                 # continue an interrupted run
.venv/bin/python campaign.py plans/Q.json --blocks 1 --packets 12  # quick smoke run
.venv/bin/python campaign.py plans/Q.json --mqtt mqtt://192.168.1.10:1883   # logs over MQTT
```

| Option | Meaning |
| --- | --- |
| `--data DIR` | Data root. Default: the parent repo's `data/` |
| `--run-id ID` | Override the plan's `run_id` (needed to repeat a plan: an existing `meta.json` is never overwritten) |
| `--blocks N`, `--packets N`, `--configs C2,C5` | Override blocks per configuration, packets per block, or the configuration set |
| `--retries N` | Re-runs of a failed block (default 2). After that the run stops. |
| `--resume` | Continue `<data>/<run_id>`. The schedule must be identical (same plan and overrides). |
| `--no-metrics` | Skip the final `metrics.py` call |
| `--mqtt URI` | Read the logs from this MQTT broker instead of serial; overrides the plan's `logs` (see "Log transport") |
| `--mqtt-commands` | With `--mqtt`: send the commands over MQTT too (untethered nodes, no serial ports) |

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

## Log transport (serial or MQTT)

Added 2026-11-27 (PLAN M6). **Commands always go over serial.** Where the JSON events come back from depends on the firmware's build-time log sink (`AFEC_LOG_SINK`, see "Log sink" in `../cc1101-afec-sender/docs/firmware.md`) and on the plan:

| Plan `logs` | Firmware sink | Events read from |
| --- | --- | --- |
| absent or `{"transport": "serial"}` | `1` serial (`env:dev`) or `3` both | the serial port, as before |
| `{"transport": "mqtt", "uri": "mqtt://host:1883"}` or `--mqtt URI` | `2` mqtt (`env:dev-mqtt`) or `3` both | the broker, topics `afec/<run_id>/<tx\|rx>/log` |

**Broker:** `docker compose up -d` in the thesis repo root starts Mosquitto on port 1883 (`docker-compose.yml`, `mosquitto/mosquitto.conf`: anonymous, no persistence, connect/disconnect logging). The host uses `mqtt://localhost:1883`; the nodes' `secrets.h` needs the Mac's LAN address.

With MQTT (`mqtt_logs.py`, paho-mqtt):
- **The broker connection is made before the ports are opened** (opening a port resets the board), so the boot lines are captured. An unreachable broker stops the run before any board is touched.
- **The raw logs keep their layout.** `raw/<role>.log` gets every MQTT line in arrival order plus the plain-text serial lines (`CC1101 ready`, used for reboot detection). `metrics.py` reads it unchanged. JSON lines that also arrive on serial (sink 3) go to `raw/<role>-serial.txt`, which `metrics.py` doesn't read, so nothing is counted twice.
- **The handshake** waits up to 20 s for `hello` (the node has to join WiFi and the broker after the reset) and checks that the node's `sink` includes `mqtt`. A serial-only build stops the run with a message.
- **Dropped lines fail the block.** After `block_end`, if either node has logged `log_drop` (its 32 KB ring overflowed) since the block started, the block fails and is re-run. A `log_drop` reported after that check is still caught at the end: the final `stats` also collects `log_stats` from both nodes, and a non-zero `dropped` prints a warning. The interferer stays on serial.
- **meta.json** records `logs` in each session, the node's `sink` in `nodes`, `log_stats` (node counters `enq`, `dropped`, `pub`, …) and `mqtt` (`messages` received by the orchestrator, broker `disconnects`).
- Broker credentials, if needed: `AFEC_MQTT_USER` / `AFEC_MQTT_PASS` in the environment, never in the plan.

**Untethered nodes** (`--mqtt URI --mqtt-commands`, or plan `logs.commands: "mqtt"`): commands go over MQTT too, so the nodes need no USB cable (useful for the M geometry, H2, and for energy measurements without the USB-serial circuit).
- No serial port is opened for the sender and receiver, and plan `ports` may be left out. The interferer stays on serial.
- Each command is published (QoS 1) to `afec/cmd/<tx|rx>` as `<cmd> id=<n>`, with an increasing id. The firmware logs `cmd_rx` and ignores a redelivered id (`cmd_dup`).
- Nothing resets the boards, so the handshake sends `hello` at once.
- **Reboots** are detected from the firmware's `boot` event. Because a rebooted node only reports it after rejoining WiFi, the orchestrator sends `hello` to both nodes after every block and waits for the reply. The `boot` comes first, so a reboot during the block is always charged to that block.
- If WiFi or the broker drops, the block times out and is re-run (paho queues QoS 1 commands while disconnected).

**Tested without hardware (2026-11-27)** against a local broker (`amqtt`) and fake nodes on ptys: a normal run, a `log_drop` → failed block → re-run, sink 3 (duplicates kept out of the logs), `--resume`, a serial-only firmware with an MQTT plan, and broker down. The serial transport was re-checked the same way. Untethered (fake nodes on MQTT only): a normal run, a reboot (`boot`) → that block failed and re-run, nodes off, and plans with missing ports or `commands: mqtt` without `--mqtt`. **Not yet tested on the real nodes.**

## What a run does

1. **Open both ports.** On the CP2102 boards this resets the ESP32s. Wait 2.5 s, send `hello` to each and check `role`. Swapped ports stop the run with a message.
2. **Schedule: permuted rounds.** For each of `blocks_per_config` rounds, the configuration list is shuffled with `random.Random(seed)`, so every round contains each configuration once. This spreads slow drift (ambient noise, temperature) evenly over the configurations.
3. **Per block:**
   1. Receiver first, then sender: `cfg …`, wait for `cfg_ok` and check that its `cfg` and every requested field match; then `mark run=<run_id> block=<n> cfg=<Cx>`. The firmware's `cfg` already re-initialises the radio and node and resets the link, so no separate `reset` is sent.
   2. Interferer `on` (if `during_block`).
   3. `run n=<packets> len=<len> interval=<interval>` on the sender, then wait for `block_end`. If `steps` are set, the interferer commands are sent when the sender's `app_tx` reaches each packet index.
   4. Interferer `off`.
4. **After every block** `meta.json` is rewritten atomically, and a progress line with an ETA is printed.
5. **At the end:** `stats` from both nodes into `meta.json`, then `metrics.py raw --out out --kw <kw> --condition <condition> [--exclude-blocks <failed>] [plan metrics args]`. If that succeeds, `eda.py --campaign out --out figs` draws the per-cell figures.

Block ids are assigned as the run goes (0, 1, 2, …) and are never reused, including across `--resume`. A block's `mark` therefore identifies it uniquely in both logs.

## Failure handling

A block **fails** when:
- a node logs `cmd_err`;
- `cfg_ok` doesn't match the request;
- a node reboots (a new `CC1101 ready` line, or untethered a `boot` event);
- `run_start` or `cfg_ok` doesn't arrive in time;
- (MQTT) a node logs `log_drop` during the block;
- the block takes longer than `packets × max(interval, (1 + retries) × (ackto + 150 ms)) + 30 s`.

If a block fails after `run_start`, the sender gets `stop` and the orchestrator waits up to 5 s for its `block_end aborted=1`. Otherwise the sender would still be mid-run (e.g. after a receiver reboot) and the retry's `cfg` would get `cmd_err busy`. After a sender reboot, the `stop` just gets `cmd_err no run active`, which is ignored.

A failed block is recorded (`status: failed`, `error`) and re-run under a fresh block id, up to `--retries` times. After a reboot the orchestrator waits 3 s first. If an entry still fails, the run stops with `meta.json` `status: failed`.

`metrics.py` leaves out failed and interrupted blocks in two ways: through `--exclude-blocks`, and because it only counts blocks for which the sender logged `block_end aborted=0`.

**Ctrl-C or SIGTERM:** the sender gets `stop`, the block in progress is recorded as `interrupted` (so its id is never reused), `meta.json` gets `status: interrupted`, and the ports are closed. `--resume` reopens the ports, which reboots the nodes, appends to the raw logs and continues with the next schedule entry that isn't `ok`.

**Tested on the bench (2026-11-27):** a normal run, interrupt + resume, and reboot detection (T9, serial): a sender reset and a receiver reset mid-block, each giving a failed block, then a clean retry. The receiver case found the `busy` bug above (fixed the same day). To reset a board during a run without the button, pulse RTS (the board's EN) from a second handle on its port. **Not yet tested:** reboot detection untethered (MQTT `boot` + post-block `hello`) and everything involving the interferer.

## meta.json

| Key | Content |
| --- | --- |
| `run_id`, `condition`, `created`, `ended`, `host`, `status` | `status` is `running`, `done` or `interrupted` |
| `plan`, `plan_file` | The full plan after CLI overrides |
| `schedule`, `schedule_rule` | Configuration per schedule index |
| `firmware` | `{commit, dirty}` for the lib, sender, receiver, parser and thesis repos at run start. **Commit everything before a real run**, so that `dirty` is false. |
| `radio` | Frequency, sync word, bitrate, whitening, HW CRC (fixed in `config.h`) |
| `sessions[]` | One per start or resume: time, `logs` (transport, URI), the `hello` of each node (port, node id, build timestamp, `sink`), firmware commits; with MQTT also `log_stats` and `mqtt` (see "Log transport") |
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
