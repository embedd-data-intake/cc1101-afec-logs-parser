# metrics.py — campaign metrics

Computes the evaluation metrics (report §2.2, parent `docs/03-evaluation-design.md`) from the sender and receiver logs of a campaign phase. Run plan item P0.4. `campaign.py` calls it at the end of a run; it can also be run by hand.

```sh
.venv/bin/python metrics.py ../data/2026-11-30-Q/raw --out ../data/2026-11-30-Q/out --kw 10
.venv/bin/python metrics.py sender.log receiver.log --out out/ --kw 10 --condition Q --exclude-blocks 4,17
# S: keep the transient, T_conv per step
.venv/bin/python metrics.py ../data/2026-12-01-S/raw --out ... --kw 0 \
    --steps 0:Q,50:B,130:Q --appropriate Q=0-1,B=3-5 --k 5
```

| Option | Default | Meaning |
| --- | --- | --- |
| inputs | | Log files, or directories (every `*.log` in them) |
| `--out` | required | Output directory |
| `--kw` | 10 | Warm-up: packets with `idx < kw` are not counted (S: 0) |
| `--w` | 10 | Flapping window in packets |
| `--condition` | last `-` token of the `mark` run id | Condition label for every block |
| `--exclude-blocks` | | Block ids to leave out (`campaign.py` passes the failed ones) |
| `--steps`, `--appropriate`, `--k` | | T_conv for S blocks (see below) |
| `--tol-ms` | 30 | Clock tolerance when deciding which attempt delivered a packet |
| `--boot`, `--seed` | 2000, 1 | Block-bootstrap replicates and seed |

The logs are loaded with `parse.load_files(..., sync=True)`, so every event gets `t_sync` (the sender's clock mapped onto the receiver's) and the `mark_run` / `mark_block` / `mark_cfg` context.

## Which blocks count

A block is used only if the sender logged `block_end` with `aborted=0` for its `(mark run, block)` and it is not in `--exclude-blocks`. Left-out blocks are listed on stderr. Their packets stay in `packets.csv` with `valid = False`.

## Outputs

### packets.csv: one row per sent packet (sender `app_tx`)

| Column | Meaning |
| --- | --- |
| `run`, `block`, `cfg`, `idx`, `aseq`, `len` | Block context and the synthetic-payload identity |
| `res` | `send()` result: `OK`, `NO_ACK`, `DUTY_LIMIT`, … |
| `retries` | R_max in force (from the sender's last `cfg_ok`) |
| `A` | Attempts: DATA frames the sender put on air for this packet (0 if the duty limit blocked it) |
| `lvl_first`, `lvl_last` | ECC level of the first and the last attempt (7 = none) |
| `n_lvl0` … `n_lvl7` | Attempts at each level |
| `air_data_us` | Airtime of the sender's DATA frames |
| `air_ctrl_us` | Airtime of control frames tied to this packet: receiver ACKs and NACKs, plus any non-DATA frame of the sender |
| `seq` | The library's 8-bit frame seq of the packet |
| `rtt_ms`, `dur_ms` | RTT of the ACKed attempt; time spent in `send()` |
| `n_rx_ok`, `n_rx_bad` | Receiver `app_rx` with `ok=1` / `ok=0` tied to this packet |
| `t_app`, `t_rx` | Sender `app_tx` time; receiver `app_rx ok=1` time (synced clock) |
| `delivered` | An `app_rx ok=1` exists for this `(run, block, aseq)` |
| `att_deliv` | Attempt that delivered it (NaN if not delivered) |
| `valid` | The block counts (see above) |

### blocks.csv: one row per valid block, over counted packets (`idx ≥ kw`)

| Column | Definition |
| --- | --- |
| `n`, `n_ok` | Counted packets; delivered packets |
| `pdr1`, `pdr2`, `pdrR` | Share delivered by attempt 1, by attempt 2, at all (= within 1 + R_max) |
| `n_d1`, `n_d2` | The counts behind `pdr1` and `pdr2` |
| `A_sum`, `A_mean` | Σ attempts; Ā |
| `n_acc`, `n_bad`, `rer` | `app_rx` total, `app_rx ok=0`, RER = n_bad / n_acc |
| `rer_ub` | 3 / n_acc when n_bad = 0 (the rule of three), else NaN |
| `air_us`, `eta_air_us_per_B` | Data + control airtime; η_air = air_us / (n_ok · L), in µs per delivered payload byte |
| `occ_0` … `occ_7` | Occupancy π_ℓ: share of the sender's DATA frames at each level |
| `switches`, `sigma_per100`, `flaps` | Changes of `lvl_first` between consecutive packets; per 100 packets; switches that return to the previous level within `w` packets |
| `duty_limited`, `no_ack` | Packets with `res` = `DUTY_LIMIT` / `NO_ACK` |
| `rtt_ms_median`, `n_frames`, `retries`, `len`, `condition` | |

### cells.csv: one row per (condition, cfg)

- **Proportions** (`pdr1`, `pdr2`, `pdrR`, `rer`) are pooled over the cell's blocks with **Wilson 95 % CIs** (`_lo`, `_hi`). `rer_ub3` = 3 / n_acc when no bad payload was seen.
- **Ratio metrics** (`A_mean`, `eta_air_us_per_B`, `sigma_per100`) are ratios of sums over blocks, with **block-bootstrap 95 % percentile CIs**. Whole blocks are resampled with replacement, because packets in a block share the same bursts (§2.4).
- `occ_*` are pooled by frame count. `flaps` and `duty_limited` are summed.

### attempts.csv

P(A = k) per cell, over counted packets: `condition, cfg, A, count, p`.

### tconv.csv (only with `--steps` and `--appropriate`)

For C4 and C5 blocks. `--steps 0:Q,50:B,130:Q` gives the segments (start index, condition). `--appropriate Q=0-1,B=3-5` gives the level range that counts as appropriate in each condition. **This has to come from the P3 pinned-level sweep, which is blocked on decision 1.**

`t_conv` = the number of packets after the segment start until `lvl_first` stays inside the range for `k` packets in a row. It is NaN if that never happens within the segment.

### bench.csv (if the logs contain a `bench`)

`op, lvl, s, n, ok, min, mean, max` in CPU cycles, plus `cpu_mhz` and `min_us`, `mean_us`, `max_us`. See the sender's `docs/firmware.md` for the ops.

## How sender and receiver events are joined

- **Packet ↔ sender events.** In the sender log, every row up to and including the k-th `app_tx` belongs to packet k (the library's `send`, `tx`, `send_ok` … come right before the `app_tx`).
- **Delivery.** `app_rx ok=1` is matched by payload identity `(mark run, block, aseq)`. Duplicates never produce `app_rx`, so there is at most one per packet.
- **Delivering attempt.** The count of the packet's DATA `tx` events with `t_sync ≤ t_rx + tol`. `tx` is logged after the frame is on air and `app_rx` after it is decoded, and attempts are at least an ACK timeout (≥ 400 ms) apart, so tens of ms of clock-offset error don't matter.
- **Receiver ACKs and `app_rx` → packet.** Through the frame seq of the most recent `rx_data` from the sender (forward-fill), keyed by `(mark run, mark block, seq)`. The seq is 8-bit, so this relies on a block having fewer than 256 packets (S uses 200).
- **Receiver NACKs → packet.** Through the next `rx_data` (back-fill). A NACK is triggered by a corrupted frame, and the next good frame is normally the retransmission of the same packet.
- Control frames and `app_rx` that can't be tied to a packet of the same block are not counted. Their number is printed on stderr.

## Known approximations and limits

- **Airtime** is the library's own estimate, `(bytes + 9) · 8 / 4800 s`, not a measurement.
- **RER** counts `ok=0` payloads, which can't be tied to a packet by their (corrupted) header. They are attributed through the frame seq of the preceding `rx_data`.
- **A node reboot in the middle of a run** restarts its `millis()`. The clock sync takes a median over the whole run, so `att_deliv` in the blocks after a reboot may be off. The reboot block itself is excluded. If a run has reboots, consider splitting it.
- **T_conv** uses the level of the first attempt per packet, and needs the appropriate-level ranges from P3.
- **E_byte** (energy) is not computed. It needs the composed energy model (run plan: datasheet currents × logged times + bench cycle counts).

## Verification

- **Simulated logs (2026-11-27):** checked with a script that emits the firmware's events with known ground truth: losses, NACKs, lost ACKs, duplicates, corrupted payloads and a 5 s clock offset. `A` and `att_deliv` matched for all 120 packets, and all control airtime was attributed.
- **Real logs:** checked on the bench runs `data/2026-11-27-cmdtest*` and `2026-11-27-campaign-*` (Q only, no losses).
