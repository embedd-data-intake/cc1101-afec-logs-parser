#!/usr/bin/env python3
"""
metrics.py - campaign metrics (run plan P0.4) from sender + receiver logs that use the firmware
command layer (`mark`, `app_tx`, `app_rx`, `block_end`, `bench`).

    python metrics.py data/2026-11-30-Q/raw/sender.log data/2026-11-30-Q/raw/receiver.log \
        --out data/2026-11-30-Q/out/ --kw 10

    # S phase: keep the transient, compute T_conv per step
    python metrics.py data/2026-12-01-S/raw/*.log --out ... --kw 0 \
        --steps 0:Q,50:B,130:Q --appropriate Q=0-1,B=3-5 --k 5

A directory argument means every *.log in it.

Outputs (in --out)
------------------
    packets.csv        one row per sent packet (sender `app_tx`), with delivery from the receiver
    blocks.csv         per-block metrics over the counted packets (idx >= K_w)
    cells.csv          per (condition, cfg): pooled proportions with Wilson CIs, ratio metrics with
                       block-bootstrap CIs
    attempts.csv       P(A = k) per cell
    tconv.csv          T_conv per S block and step (only with --steps and --appropriate)
    bench.csv          `bench` results in cycles and us (only if the logs contain a bench)

Definitions (docs/03-evaluation-design.md, report section 2.2)
---------------------------------------------------------------
    delivered       the receiver logged app_rx ok=1 (CRC32 of the synthetic payload matches) for
                    this packet's (run, block, aseq). Duplicates never produce app_rx.
    A               DATA frames the sender put on air for the packet (0 if the duty limit blocked it)
    att_deliv       which attempt delivered it: sender DATA frames with t_sync <= app_rx t_sync + tol.
                    Needs the --sync clock alignment; attempts are >= ACK timeout apart, so a few
                    ms of offset error does not matter.
    PDR_1/_2/_R     share of counted packets delivered by attempt 1 / 2 / any attempt
    RER             N_bad / N_acc: app_rx ok=0 over all app_rx (frames that passed CRC-16 but carry a
                    corrupted payload). With N_bad = 0 the upper bound 3 / N_acc is reported.
    eta_air         (sender DATA airtime + all ACK/NACK airtime of both nodes) / (N_ok * L), us/B.
                    Receiver ACKs and app_rx are tied to the sender packet through the 8-bit frame
                    seq of the preceding rx_data (unique within a block of < 256 packets); NACKs
                    through the next rx_data. Control frames that cannot be tied to a counted packet
                    of the same block are left out.
    occupancy       share of the sender's DATA frames at each level
    sigma           level switches (level of the first attempt, packet to packet) per 100 packets
    flaps           switches that return to the previous level within w packets
    T_conv          packets after a step until lvl_first stays in the appropriate range for k
                    packets in a row
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from parse import load_files

LEVELS = [0, 1, 2, 3, 4, 5, 7]
ADAPTIVE = {"C4", "C5"}


# --------------------------------------------------------------------------- #
# Statistics helpers
# --------------------------------------------------------------------------- #
def wilson(k: float, n: float, z: float = 1.96) -> tuple[float, float]:
    if n <= 0:
        return (np.nan, np.nan)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def boot_ratio(num: np.ndarray, den: np.ndarray, reps: int, rng: np.random.Generator
               ) -> tuple[float, float]:
    """Percentile 95 % CI of sum(num)/sum(den), resampling whole blocks."""
    num, den = np.asarray(num, float), np.asarray(den, float)
    if len(num) < 2:
        return (np.nan, np.nan)
    idx = rng.integers(0, len(num), size=(reps, len(num)))
    d = den[idx].sum(axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        s = num[idx].sum(axis=1) / d
    s = s[np.isfinite(s)]
    if not len(s):
        return (np.nan, np.nan)
    return (float(np.percentile(s, 2.5)), float(np.percentile(s, 97.5)))


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
def expand_inputs(inputs: list[str]) -> list[Path]:
    paths: list[Path] = []
    for i in inputs:
        p = Path(i)
        paths += sorted(p.glob("*.log")) if p.is_dir() else [p]
    return paths


def roles(df: pd.DataFrame) -> tuple[int, int]:
    s = df.loc[df["ev"] == "app_tx", "node"].unique()
    r = df.loc[df["ev"] == "app_rx", "node"].unique()
    if len(s) != 1:
        sys.exit(f"expected exactly one sender node with app_tx events, found {list(s)}")
    if len(r) != 1:
        sys.exit(f"expected exactly one receiver node with app_rx events, found {list(r)}")
    return int(s[0]), int(r[0])


def condition_of(run: str, override: str | None) -> str:
    if override:
        return override
    return str(run).rsplit("-", 1)[-1] if isinstance(run, str) and "-" in run else str(run)


# --------------------------------------------------------------------------- #
# Packet table
# --------------------------------------------------------------------------- #
def packet_table(df: pd.DataFrame, tol_ms: float = 30.0) -> tuple[pd.DataFrame, dict]:
    s_node, r_node = roles(df)
    has_sync = "t_sync" in df.columns
    tcol = "t_sync" if has_sync else "t"
    if not has_sync:
        print("[metrics] warning: no t_sync (single log?) - att_deliv falls back to A", file=sys.stderr)

    # ---------------- sender: rows up to and including app_tx #k belong to packet k
    S = df[df["node"] == s_node].sort_values(["source", "line"]).reset_index(drop=True)
    is_app = S["ev"].eq("app_tx")
    S["pkt"] = is_app.cumsum() - is_app.astype(int)

    app = S[is_app].copy()
    pk = pd.DataFrame({
        "pkt": app["pkt"].astype(int).values,
        "run": app["mark_run"].values if "mark_run" in app else np.nan,
        "block": app["block"].astype(int).values,
        "mark_block": app["mark_block"].values if "mark_block" in app else np.nan,
        "cfg": app["mark_cfg"].values if "mark_cfg" in app else np.nan,
        "idx": app["idx"].astype(int).values,
        "aseq": app["aseq"].astype(np.int64).values,
        "len": app["len"].astype(int).values,
        "res": app["res"].astype(str).values,
        "dur_ms": app["dur_ms"].values,
        "t_app": app[tcol].values,
    }).set_index("pkt")

    # cfg from cfg_ok if no mark carried it
    if pk["cfg"].isna().any() and "cfg" in S.columns:
        cfg_ff = S["cfg"].where(S["ev"] == "cfg_ok").ffill()
        pk["cfg"] = pk["cfg"].fillna(pd.Series(cfg_ff[is_app].values, index=pk.index))
    retries_ff = S["retries"].where(S["ev"] == "cfg_ok").ffill() if "retries" in S.columns else None
    pk["retries"] = retries_ff[is_app].values if retries_ff is not None else np.nan

    data_tx = S[(S["ev"] == "tx") & (S["type"].astype(str) == "DATA") & (S["src"] == s_node)]
    g = data_tx.groupby("pkt")
    pk["A"] = g.size().reindex(pk.index).fillna(0).astype(int)
    pk["lvl_first"] = g["lvl"].first().reindex(pk.index)
    pk["lvl_last"] = g["lvl"].last().reindex(pk.index)
    pk["air_data_us"] = g["air_us"].sum().reindex(pk.index).fillna(0.0)
    pk["seq"] = S[S["ev"] == "send"].groupby("pkt")["seq"].first().reindex(pk.index)
    pk["rtt_ms"] = S[S["ev"] == "send_ok"].groupby("pkt")["rtt_ms"].first().reindex(pk.index)
    ctrl_s = S[(S["ev"] == "tx") & (S["type"].astype(str) != "DATA")]
    pk["air_ctrl_us"] = ctrl_s.groupby("pkt")["air_us"].sum().reindex(pk.index).fillna(0.0)
    for lv in LEVELS:
        pk[f"n_lvl{lv}"] = data_tx[data_tx["lvl"] == lv].groupby("pkt").size().reindex(pk.index).fillna(0).astype(int)

    # ---------------- receiver
    R = df[df["node"] == r_node].sort_values(["source", "line"]).reset_index(drop=True)
    rseq = R["seq"].where((R["ev"] == "rx_data") & (R["src"] == s_node))
    R["seq_ff"], R["seq_bf"] = rseq.ffill(), rseq.bfill()
    key = pk.dropna(subset=["seq"]).reset_index()
    key = key.set_index([key["run"].astype(str), key["mark_block"], key["seq"].astype(int)])["pkt"]
    key = key[~key.index.duplicated(keep="first")]

    def attribute(rows: pd.DataFrame, seqcol: str) -> pd.Series:
        ok = rows[seqcol].notna() & rows["mark_block"].notna()
        idx = pd.MultiIndex.from_arrays([rows.loc[ok, "mark_run"].astype(str), rows.loc[ok, "mark_block"],
                                         rows.loc[ok, seqcol].astype(int)])
        out = pd.Series(np.nan, index=rows.index)
        out[ok] = key.reindex(idx).values
        return out

    rx_app = R[R["ev"] == "app_rx"].copy()
    rx_app["pkt"] = attribute(rx_app, "seq_ff")
    pk["n_rx_ok"] = rx_app[rx_app["ok"] == 1].groupby("pkt").size().reindex(pk.index).fillna(0).astype(int)
    pk["n_rx_bad"] = rx_app[rx_app["ok"] != 1].groupby("pkt").size().reindex(pk.index).fillna(0).astype(int)

    ctrl = R[(R["ev"] == "tx") & R["type"].astype(str).isin(["ACK", "NACK"])].copy()
    ctrl["pkt"] = np.where(ctrl["type"].astype(str) == "ACK", attribute(ctrl, "seq_ff"), attribute(ctrl, "seq_bf"))
    pk["air_ctrl_us"] += ctrl.groupby("pkt")["air_us"].sum().reindex(pk.index).fillna(0.0)
    unattributed_ctrl = int(ctrl["pkt"].isna().sum())

    # delivery: app_rx ok=1 by payload identity (run, block, aseq)
    good = rx_app[rx_app["ok"] == 1]
    deliv = good.groupby([good["mark_run"].astype(str), good["block"].astype(int), good["aseq"].astype(np.int64)])[tcol].min()
    dk = pd.MultiIndex.from_arrays([pk["run"].astype(str), pk["block"], pk["aseq"]])
    pk["t_rx"] = deliv.reindex(dk).values
    pk["delivered"] = pk["t_rx"].notna()

    if has_sync:
        tt = data_tx[["pkt", tcol]].merge(pk[["t_rx"]], left_on="pkt", right_index=True)
        tt = tt[tt[tcol] <= tt["t_rx"] + tol_ms]
        pk["att_deliv"] = tt.groupby("pkt").size().reindex(pk.index)
        pk.loc[pk["delivered"], "att_deliv"] = pk.loc[pk["delivered"], "att_deliv"].fillna(1).clip(lower=1)
    else:
        pk["att_deliv"] = pk["A"].where(pk["delivered"])
    pk.loc[~pk["delivered"], "att_deliv"] = np.nan

    info = {"sender": s_node, "receiver": r_node,
            "app_rx_unattributed": int(rx_app["pkt"].isna().sum()),
            "ctrl_unattributed": unattributed_ctrl,
            "app_rx_bad_total": int((rx_app["ok"] != 1).sum())}
    return pk.reset_index(), info


def valid_blocks(df: pd.DataFrame, pk: pd.DataFrame, s_node: int, exclude: str) -> pd.Series:
    """A block counts only if the sender logged block_end aborted=0 for it and it is not excluded."""
    ex = {int(x) for x in exclude.split(",") if x.strip()}
    be = df[(df["ev"] == "block_end") & (df["node"] == s_node)]
    if "aborted" in be.columns:
        be = be[be["aborted"] == 0]
    clean = set(zip(be["mark_run"].astype(str), be["block"].astype(int))) if "mark_run" in be else set()
    ok = pd.Series([(str(r), int(b)) in clean and int(b) not in ex for r, b in zip(pk["run"], pk["block"])],
                   index=pk.index)
    bad = pk.loc[~ok, ["run", "block"]].drop_duplicates()
    if len(bad):
        print(f"[metrics] left out {len(bad)} block(s) (excluded, aborted or unfinished): "
              f"{sorted(bad['block'].astype(int).tolist())}", file=sys.stderr)
    return ok


# --------------------------------------------------------------------------- #
# Block and cell metrics
# --------------------------------------------------------------------------- #
def switches_and_flaps(levels: np.ndarray, w: int) -> tuple[int, int]:
    lv = levels[~np.isnan(levels)]
    sw = np.flatnonzero(lv[1:] != lv[:-1]) + 1            # packet index where the level changed
    flaps = 0
    for i in sw:
        prev = lv[i - 1]
        later = sw[(sw > i) & (sw <= i + w)]
        if any(lv[j] == prev for j in later):
            flaps += 1
    return len(sw), flaps


def block_table(pk: pd.DataFrame, kw: int, w: int, condition: str | None) -> pd.DataFrame:
    rows = []
    for (run, block), b in pk.groupby(["run", "block"], sort=False, dropna=False):
        b = b.sort_values("idx")
        c = b[b["idx"] >= kw]
        n = len(c)
        if not n:
            continue
        L = int(c["len"].iloc[0])
        n_ok = int(c["delivered"].sum())
        n_acc = int(c["n_rx_ok"].sum() + c["n_rx_bad"].sum())
        n_bad = int(c["n_rx_bad"].sum())
        air = float(c["air_data_us"].sum() + c["air_ctrl_us"].sum())
        sw, fl = switches_and_flaps(c["lvl_first"].to_numpy(float), w)
        ntx = c[[f"n_lvl{lv}" for lv in LEVELS]].sum()
        r = {
            "run": run, "condition": condition_of(run, condition), "block": block,
            "cfg": c["cfg"].iloc[0], "retries": c["retries"].iloc[0], "len": L,
            "n": n, "n_ok": n_ok,
            "n_d1": int((c["att_deliv"] <= 1).sum()), "n_d2": int((c["att_deliv"] <= 2).sum()),
            "pdr1": (c["att_deliv"] <= 1).sum() / n, "pdr2": (c["att_deliv"] <= 2).sum() / n, "pdrR": n_ok / n,
            "A_sum": int(c["A"].sum()), "A_mean": c["A"].mean(),
            "n_acc": n_acc, "n_bad": n_bad,
            "rer": n_bad / n_acc if n_acc else np.nan,
            "rer_ub": (3 / n_acc if n_acc else np.nan) if n_bad == 0 else np.nan,
            "air_us": air, "eta_air_us_per_B": air / (n_ok * L) if n_ok else np.nan,
            "duty_limited": int((c["res"] == "DUTY_LIMIT").sum()),
            "no_ack": int((c["res"] == "NO_ACK").sum()),
            "switches": sw, "flaps": fl, "sigma_per100": 100 * sw / (n - 1) if n > 1 else np.nan,
            "rtt_ms_median": c["rtt_ms"].median(),
            "n_frames": int(ntx.sum()),
        }
        for lv in LEVELS:
            r[f"occ_{lv}"] = ntx[f"n_lvl{lv}"] / ntx.sum() if ntx.sum() else np.nan
        rows.append(r)
    return pd.DataFrame(rows)


def cell_table(bl: pd.DataFrame, reps: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    for (cond, cfg), b in bl.groupby(["condition", "cfg"], sort=True):
        n, n_ok, n_acc, n_bad = b["n"].sum(), b["n_ok"].sum(), b["n_acc"].sum(), b["n_bad"].sum()
        L = b["len"].iloc[0]
        r = {"condition": cond, "cfg": cfg, "blocks": len(b), "n": int(n), "n_ok": int(n_ok)}
        for name, k in (("pdr1", b["n_d1"].sum()), ("pdr2", b["n_d2"].sum()), ("pdrR", n_ok)):
            lo, hi = wilson(k, n)
            r[name], r[name + "_lo"], r[name + "_hi"] = k / n if n else np.nan, lo, hi
        r["A_mean"] = b["A_sum"].sum() / n
        r["A_mean_lo"], r["A_mean_hi"] = boot_ratio(b["A_sum"], b["n"], reps, rng)
        r["n_acc"], r["n_bad"] = int(n_acc), int(n_bad)
        r["rer"] = n_bad / n_acc if n_acc else np.nan
        r["rer_lo"], r["rer_hi"] = wilson(n_bad, n_acc)
        r["rer_ub3"] = 3 / n_acc if (n_acc and n_bad == 0) else np.nan
        r["eta_air_us_per_B"] = b["air_us"].sum() / (n_ok * L) if n_ok else np.nan
        r["eta_air_lo"], r["eta_air_hi"] = boot_ratio(b["air_us"], b["n_ok"] * L, reps, rng)
        sw_den = (b["n"] - 1).clip(lower=0)
        r["sigma_per100"] = 100 * b["switches"].sum() / sw_den.sum() if sw_den.sum() else np.nan
        lo, hi = boot_ratio(b["switches"], sw_den, reps, rng)
        r["sigma_lo"], r["sigma_hi"] = 100 * lo, 100 * hi
        r["flaps"] = int(b["flaps"].sum())
        r["duty_limited"] = int(b["duty_limited"].sum())
        fr = b["n_frames"].sum()
        for lv in LEVELS:
            r[f"occ_{lv}"] = (b[f"occ_{lv}"] * b["n_frames"]).sum() / fr if fr else np.nan
        rows.append(r)
    return pd.DataFrame(rows)


def attempts_table(pk: pd.DataFrame, bl: pd.DataFrame, kw: int) -> pd.DataFrame:
    c = pk[pk["idx"] >= kw].merge(bl[["run", "block", "condition"]], on=["run", "block"])
    t = c.groupby(["condition", "cfg", "A"]).size().rename("count").reset_index()
    t["p"] = t["count"] / t.groupby(["condition", "cfg"])["count"].transform("sum")
    return t


# --------------------------------------------------------------------------- #
# T_conv (class S)
# --------------------------------------------------------------------------- #
def parse_steps(s: str) -> list[tuple[int, str]]:
    out = [(int(a), b) for a, b in (x.split(":") for x in s.split(","))]
    return sorted(out)


def parse_appropriate(s: str) -> dict[str, tuple[int, int]]:
    out = {}
    for item in s.split(","):
        cond, rng_ = item.split("=")
        lo, _, hi = rng_.partition("-")
        out[cond] = (int(lo), int(hi or lo))
    return out


def tconv_table(pk: pd.DataFrame, steps: list[tuple[int, str]], appr: dict[str, tuple[int, int]], k: int
                ) -> pd.DataFrame:
    rows = []
    for (run, block), b in pk.groupby(["run", "block"], sort=False):
        if b["cfg"].iloc[0] not in ADAPTIVE:
            continue
        lv = b.sort_values("idx").set_index("idx")["lvl_first"]
        bounds = [s for s, _ in steps] + [int(lv.index.max()) + 1]
        for (start, cond), end in zip(steps, bounds[1:]):
            if cond not in appr:
                continue
            lo, hi = appr[cond]
            seg = lv.loc[start:end - 1]
            inside = seg.between(lo, hi).to_numpy()
            tc = np.nan
            for m in range(0, len(inside) - k + 1):
                if inside[m:m + k].all():
                    tc = m
                    break
            rows.append({"run": run, "block": block, "cfg": b["cfg"].iloc[0], "step_idx": start,
                         "to": cond, "appropriate": f"{lo}-{hi}", "k": k, "t_conv": tc,
                         "seg_len": len(seg)})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Bench
# --------------------------------------------------------------------------- #
def bench_table(df: pd.DataFrame) -> pd.DataFrame:
    if "op" not in df.columns or not (df["ev"] == "bench").any():
        return pd.DataFrame()
    d = df.sort_values(["source", "line"]).copy()
    d["cpu_mhz"] = d["cpu_mhz"].where(d["ev"] == "bench_start").ffill() if "cpu_mhz" in d else np.nan
    b = d[d["ev"] == "bench"][["node", "op", "lvl", "s", "n", "ok", "min", "mean", "max", "cpu_mhz"]].copy()
    for c in ("min", "mean", "max"):
        b[f"{c}_us"] = b[c] / b["cpu_mhz"]
    return b.reset_index(drop=True)


# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description="Per-packet, per-block and per-cell AFEC campaign metrics.")
    ap.add_argument("inputs", nargs="+", help="sender and receiver log files (or a directory of *.log)")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--kw", type=int, default=10, help="warm-up packets dropped per block (S: 0)")
    ap.add_argument("--w", type=int, default=10, help="flapping window in packets")
    ap.add_argument("--condition", help="condition label for all blocks (default: last '-' token of mark run)")
    ap.add_argument("--steps", help="S blocks: step points, e.g. 0:Q,50:B,130:Q")
    ap.add_argument("--appropriate", help="S blocks: appropriate level range per condition, e.g. Q=0-1,B=3-5")
    ap.add_argument("--k", type=int, default=5, help="T_conv: packets in a row at the appropriate level")
    ap.add_argument("--tol-ms", type=float, default=30.0, help="clock tolerance for att_deliv")
    ap.add_argument("--boot", type=int, default=2000, help="block-bootstrap replicates")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--exclude-blocks", default="", help="block ids to leave out (campaign.py: failed blocks)")
    args = ap.parse_args()

    df = load_files(expand_inputs(args.inputs), sync=True)
    args.out.mkdir(parents=True, exist_ok=True)

    bench = bench_table(df)
    if not bench.empty:
        bench.to_csv(args.out / "bench.csv", index=False)
        print(f"[metrics] bench: {len(bench)} rows", file=sys.stderr)
    if not (df["ev"] == "app_tx").any():
        print("[metrics] no app_tx events - no packet metrics", file=sys.stderr)
        return

    pk, info = packet_table(df, args.tol_ms)
    pk["valid"] = valid_blocks(df, pk, info["sender"], args.exclude_blocks)
    pk.to_csv(args.out / "packets.csv", index=False)
    pk = pk[pk["valid"]]
    bl = block_table(pk, args.kw, args.w, args.condition)
    bl.to_csv(args.out / "blocks.csv", index=False)
    cells = cell_table(bl, args.boot, args.seed)
    cells.to_csv(args.out / "cells.csv", index=False)
    attempts_table(pk, bl, args.kw).to_csv(args.out / "attempts.csv", index=False)
    if args.steps and args.appropriate:
        tconv_table(pk, parse_steps(args.steps), parse_appropriate(args.appropriate), args.k
                    ).to_csv(args.out / "tconv.csv", index=False)

    print(f"[metrics] sender node {info['sender']}, receiver node {info['receiver']}: "
          f"{len(pk)} packets, {len(bl)} blocks, {len(cells)} cells", file=sys.stderr)
    if info["app_rx_unattributed"] or info["ctrl_unattributed"]:
        print(f"[metrics] not tied to a sender packet: {info['app_rx_unattributed']} app_rx, "
              f"{info['ctrl_unattributed']} ACK/NACK frames", file=sys.stderr)
    cols = ["condition", "cfg", "blocks", "n", "pdr1", "pdr2", "pdr2_lo", "pdr2_hi", "pdrR", "A_mean",
            "rer", "rer_ub3", "eta_air_us_per_B", "sigma_per100", "duty_limited"]
    with pd.option_context("display.width", 200, "display.precision", 4):
        print(cells[[c for c in cols if c in cells.columns]].to_string(index=False))


if __name__ == "__main__":
    main()
