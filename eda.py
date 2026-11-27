#!/usr/bin/env python3
"""
eda.py - plots for AFEC logs.

Single-node view, from an events.csv produced by parse.py:
  python eda.py events.csv                      # prints summary, writes eda_overview.png
  python eda.py events.csv --node 2 --out figs/
Messages are identified by counting `send` events (not by `seq`), so seq wrap-around is harmless.

Campaign view, from the out/ directory of metrics.py (one or more runs):
  python eda.py --campaign data/2026-11-30-Q/out [data/2026-11-30-M/out ...] --out figs/ [--kw 10]
  Writes cells_reliability.png, cells_efficiency.png, cells_occupancy.png, cells_attempts.png,
  blocks.png, levels_trace.png and, if tconv.csv exists, tconv.png.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

LVL = {0: "Hamming", 1: "BCH2", 2: "BCH5", 3: "RS2", 4: "RS4", 5: "RS8", 7: "none"}
GREEN, AMBER, RED, GREY = "#2e9e5b", "#e0a030", "#d64545", "#8a8f98"


# --------------------------------------------------------------------------- #
def load(path: Path, node: int | None, role: str = "auto") -> tuple[pd.DataFrame, int, str]:
    df = pd.read_csv(path, low_memory=False)
    n_send = df[df["ev"] == "send"].groupby("node").size()
    n_recv = df[df["ev"].isin(["rx_data", "ack_sent"])].groupby("node").size()
    if role == "auto":
        if n_send.empty and n_recv.empty:
            raise SystemExit("no `send` or `rx_data`/`ack_sent` events - nothing to analyse")
        role = "sender" if n_send.sum() >= n_recv.sum() else "receiver"
    if node is None:
        cnt = n_send if role == "sender" else n_recv
        if cnt.empty:
            raise SystemExit(f"no {role} events in this file")
        node = int(cnt.idxmax())
    df = df[df["node"] == node].sort_values("line").reset_index(drop=True)
    df["t_rel"] = df["t_s"] - df["t_s"].min()
    df["msg"] = (df["ev"] == "send").cumsum()          # 0 = events before the first send
    return df, node, role


def build_tables(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    d = df[df["msg"] > 0]
    outcome = (d[d["ev"].isin(["send_ok", "send_fail"])]
        .drop_duplicates("msg", keep="last").set_index("msg")["ev"]
        .map({"send_ok": "ok", "send_fail": "fail"}))
    rtt = d[d["ev"] == "send_ok"].set_index("msg")["rtt_ms"]

    msgs = d[d["ev"] == "send"][["msg", "seq", "t_rel", "t_s"]].set_index("msg")
    msgs["outcome"] = outcome.reindex(msgs.index).fillna("pending")
    msgs["rtt_ms"] = rtt.reindex(msgs.index)

    tx = d[(d["ev"] == "tx") & (d["type"] == "DATA")].copy()
    tx["attempt_n"] = tx.groupby("msg").cumcount() + 1
    last = tx.groupby("msg")["attempt_n"].transform("max")
    tx["outcome"] = tx["msg"].map(msgs["outcome"])
    tx["acked"] = (tx["outcome"] == "ok") & (tx["attempt_n"] == last)
    msgs["attempts"] = tx.groupby("msg").size().reindex(msgs.index).fillna(0).astype(int)
    msgs["first_lvl"] = tx[tx["attempt_n"] == 1].set_index("msg")["lvl"].reindex(msgs.index)
    return msgs.reset_index(), tx


def outages(msgs: pd.DataFrame) -> pd.DataFrame:
    """Runs of consecutive failed messages."""
    rows, run = [], []
    for _, m in msgs.iterrows():
        if m["outcome"] == "fail":
            run.append(m)
        elif run:
            rows.append(run); run = []
    if run:
        rows.append(run)
    out = []
    for r in rows:
        nxt = msgs[msgs["t_rel"] > r[-1]["t_rel"]]
        end = nxt["t_rel"].iloc[0] if len(nxt) else r[-1]["t_rel"]
        out.append({"start_s": r[0]["t_rel"], "end_s": end, "dur_s": end - r[0]["t_rel"],
                    "msgs_lost": len(r), "first_seq": int(r[0]["seq"]), "last_seq": int(r[-1]["seq"])})
    return pd.DataFrame(out)


# --------------------------------------------------------------------------- #
def summary(df: pd.DataFrame, msgs: pd.DataFrame, tx: pd.DataFrame, node: int) -> None:
    fin = msgs[msgs["outcome"] != "pending"]
    print(f"node {node}: {len(df)} events, {df['t_rel'].max():.1f} s window "f"(node clock {df['t_s'].min():.1f}-{df['t_s'].max():.1f} s)")
    print("begin event present:", bool((df["ev"] == "begin").any()))

    print("\n== Delivery ==")
    n_ok = (fin["outcome"] == "ok").sum()
    print(f"messages {len(fin)}: ok {n_ok} ({n_ok / len(fin):.1%}), fail {len(fin) - n_ok}")
    print("ok on 1st attempt:", int(((fin["outcome"] == "ok") & (fin["attempts"] == 1)).sum()), "| ok after retries:", int(((fin["outcome"] == "ok") & (fin["attempts"] > 1)).sum()))
    ok = fin[fin["outcome"] == "ok"]
    print(f"rtt_ms: {ok['rtt_ms'].min():.0f}-{ok['rtt_ms'].max():.0f} (median {ok['rtt_ms'].median():.0f})")
    oc = outages(msgs)
    print("\n== Outage episodes (consecutive failed messages) ==")
    print(oc.round(2).to_string(index=False) if len(oc) else "none")
    if len(oc) > 1:
        print("onset-to-onset gaps (s):", np.diff(oc["start_s"]).round(2).tolist())

    print("\n== TX levels / airtime ==")
    g = tx.groupby("lvl").agg(tx=("t", "size"), acked=("acked", "sum"), air_bytes=("air_bytes", "mean"), air_ms=("air_us", lambda s: s.mean() / 1000))
    g.index = [LVL.get(int(i), i) for i in g.index]
    print(g.round(1).to_string())
    tot = tx["air_us"].sum() / 1e6
    wasted = tx.loc[~tx["acked"], "air_us"].sum() / 1e6
    print(f"total airtime {tot:.2f} s, of which on un-acked attempts {wasted:.2f} s ({wasted / tot:.0%})")
    if tx["air_bytes"].nunique() > 1:
        slope, icpt = np.polyfit(tx["air_bytes"], tx["air_us"], 1)
        print(f"air_us ~= {icpt / 1000:.1f} ms + {slope / 1000:.3f} ms/byte  (~{8e3 / (slope / 1000) / 1000:.2f} kbps)")
    if tx["budget_us"].notna().any():
        print(f"duty budget: min {tx['budget_us'].min() / 1e6:.2f} s, max {tx['budget_us'].max() / 1e6:.2f} s "f"| tx_duty_limit events: {int((df['ev'] == 'tx_duty_limit').sum())}")

    ra = df[df["ev"] == "rx_ack"]
    if len(ra):
        print("\n== ACK link quality ==")
        print(ra[["rssi", "lqi", "fb", "corr", "lvl"]].describe().loc[["min", "50%", "max"]].round(1).to_string())
        print("ACK lvl values:", ra["lvl"].value_counts().to_dict(), "| corr>0:", int((ra["corr"] > 0).sum()))
        print("fb values:", ra["fb"].value_counts().sort_index().to_dict())

    st = df[df["ev"] == "send_timeout"]
    if len(st):
        print(f"\n`timeouts` counter is cumulative: first value in capture = {int(st['timeouts'].min())} "f"(=> {int(st['timeouts'].min()) - 1} timeouts happened before this capture), "f"{len(st)} seen here")

    other = df[~df["ev"].isin(["send", "send_attempt", "send_ok", "send_fail", "send_timeout", "send_retry", "tx", "rx_ack", "ack_match", "lvl_step", "lvl_timeout"])]
    if len(other):
        print("\n== Other events ==")
        print(other.dropna(axis=1, how="all").to_string(index=False))


# --------------------------------------------------------------------------- #
def plot(df: pd.DataFrame, msgs: pd.DataFrame, tx: pd.DataFrame, path: Path) -> None:
    oc = outages(msgs)
    fig, ax = plt.subplots(4, 1, figsize=(11, 10), sharex=True, gridspec_kw={"height_ratios": [2, 1.4, 1.2, 1.2]})
    for a in ax:
        for _, o in oc.iterrows():
            a.axvspan(o["start_s"], o["end_s"], color=RED, alpha=0.08, lw=0)
        a.grid(alpha=0.25)

    # 1) TX level per attempt
    a = ax[0]
    good, bad = tx[tx["acked"]], tx[~tx["acked"]]
    a.scatter(bad["t_rel"], bad["lvl"], marker="x", c=RED, s=28, label="attempt not ACKed")
    a.scatter(good["t_rel"], good["lvl"], marker="o", c=GREEN, s=34, label="attempt ACKed")
    a.set_yticks(list(LVL)[:6]); a.set_yticklabels([f"{k} {v}" for k, v in list(LVL.items())[:6]])
    a.set_ylabel("DATA tx level"); a.legend(loc="center right", fontsize=8, framealpha=0.9)
    a.set_title("AFEC sender (node %d): adaptation, link quality and delivery" % int(df["node"].iloc[0]))

    # 2) ACK link quality
    a = ax[1]
    ra = df[df["ev"] == "rx_ack"]
    a.plot(ra["t_rel"], ra["rssi"], "o-", ms=3, lw=1, color="#3b6fb6", label="RSSI (dBm)")
    a.set_ylabel("ACK RSSI (dBm)", color="#3b6fb6")
    b = a.twinx()
    b.plot(ra["t_rel"], ra["lqi"], "s-", ms=3, lw=1, color="#9a5bb5")
    b.set_ylabel("ACK LQI", color="#9a5bb5"); b.set_ylim(40, 60)

    # 3) attempts per message
    a = ax[2]
    col = msgs["outcome"].map({"ok": GREEN, "fail": RED, "pending": GREY}).where(
        ~((msgs["outcome"] == "ok") & (msgs["attempts"] > 1)), AMBER)
    a.bar(msgs["t_rel"], msgs["attempts"], width=0.9, color=col)
    a.set_ylabel("attempts / msg"); a.set_yticks(range(0, int(msgs["attempts"].max()) + 1))
    from matplotlib.patches import Patch
    a.legend(handles=[Patch(color=GREEN, label="ok, 1st try"), Patch(color=AMBER, label="ok after retry"), Patch(color=RED, label="failed (NO_ACK)")], loc="upper left", fontsize=8, framealpha=0.9)

    # 4) airtime per tx
    a = ax[3]
    a.bar(tx["t_rel"], tx["air_us"] / 1000, width=0.35, color=np.where(tx["acked"], GREEN, RED))
    a.set_ylabel("airtime / tx (ms)"); a.set_xlabel("time since first event (s, node clock)")

    fig.tight_layout()
    fig.savefig(path, dpi=140)
    print(f"\nfigure -> {path}")


#                               RECEIVER VIEW
# --------------------------------------------------------------------------- #
def recv_tables(df: pd.DataFrame) -> pd.DataFrame:
    """One row per DATA frame heard (`rx`), with gap / seq-reset / ACK turnaround columns."""
    rx = df[(df["ev"] == "rx") & (df["type"] == "DATA")].sort_values("t").copy()
    rx["dt"] = rx["t_rel"].diff()
    rx["dseq"] = rx["seq"].diff()
    med = rx["dt"].median()
    rx["gap"] = rx["dt"] > 3 * med                         # silence much longer than the normal cadence
    rx["seq_reset"] = rx["dseq"] < 0                        # sender counter went backwards
    rx["missing"] = (rx["dseq"] - 1).where(rx["dseq"] > 1, 0)

    ack = (df[(df["ev"] == "tx") & (df["type"] == "ACK")][["t", "lvl", "air_us", "air_bytes"]]
        .rename(columns={"t": "ack_t", "lvl": "ack_lvl", "air_us": "ack_air_us", "air_bytes": "ack_bytes"})
        .sort_values("ack_t"))
    rx = pd.merge_asof(rx, ack, left_on="t", right_on="ack_t", direction="forward", tolerance=1000)
    rx["turnaround_ms"] = rx["ack_t"] - rx["t"]
    return rx.reset_index(drop=True)


def summary_recv(df: pd.DataFrame, rx: pd.DataFrame, node: int) -> None:
    print(f"node {node} (receiver view): {len(df)} events, {df['t_rel'].max():.1f} s window "f"(node clock {df['t_s'].min():.1f}-{df['t_s'].max():.1f} s)")
    print("begin event present:", bool((df["ev"] == "begin").any()))

    rd = df[df["ev"] == "rx_data"]
    print("\n== Frames heard ==")
    print(f"DATA frames: {len(rx)} from src {sorted(int(s) for s in rx['src'].dropna().unique())}, "f"rx_data: {len(rd)} (dup={int(rd['dup'].sum())}, cast={rd['cast'].value_counts().to_dict()})")
    for ev in ("rx_bad_header", "rx_ecc_fail", "rx_crc_fail"):
        print(f"{ev}: {int((df['ev'] == ev).sum())}", end="   ")
    print(f"\nsymbols/bits corrected (sum of corr): {int(rx['corr'].sum())}, frames with corr>0: {int((rx['corr'] > 0).sum())}")

    print("\n== By DATA level ==")
    g = rx.groupby("lvl").agg(frames=("t", "size"), ecc_t=("ecc_t", "first"), corr=("corr", "sum"), rssi_med=("rssi", "median"), rssi_min=("rssi", "min"), lqi_med=("lqi", "median"))
    g.index = [LVL.get(int(i), i) for i in g.index]
    print(g.round(1).to_string())
    print(f"RSSI {rx['rssi'].min():.0f}..{rx['rssi'].max():.0f} dBm, LQI {rx['lqi'].min():.0f}..{rx['lqi'].max():.0f}, "f"fb values {rx['fb'].value_counts().sort_index().to_dict()}")

    print("\n== Cadence, gaps, seq ==")
    print(f"inter-arrival median {rx['dt'].median():.2f} s (max {rx['dt'].max():.2f} s)")
    gaps = rx[rx["gap"]]
    if len(gaps):
        gt = pd.DataFrame({"gap_start_s": gaps["t_rel"] - gaps["dt"], "gap_end_s": gaps["t_rel"], "dur_s": gaps["dt"], "seq_before": gaps["seq"] - gaps["dseq"], "seq_after": gaps["seq"], "seq_reset": gaps["seq_reset"]})
        print(gt.round(2).to_string(index=False))
    else:
        print("no gaps")
    rs = rx[rx["seq_reset"]]
    for _, r in rs.iterrows():
        print(f"seq reset at t={r['t_rel']:.1f}s: {int(r['seq'] - r['dseq'])} -> {int(r['seq'])}")
    print(f"seq numbers skipped inside runs (frames lost but visible): {int(rx['missing'].sum())}")

    print("\n== ACKs sent ==")
    print(f"ACK tx: {int(((df['ev'] == 'tx') & (df['type'] == 'ACK')).sum())}, ack_sent: {int((df['ev'] == 'ack_sent').sum())}, "f"ack_failed: {int((df['ev'] == 'ack_failed').sum())}, nack_sent: {int((df['ev'] == 'nack_sent').sum())}")
    a = rx.groupby("ack_lvl").agg(acks=("t", "size"), air_bytes=("ack_bytes", "mean"), air_ms=("ack_air_us", lambda s: s.mean() / 1000), turnaround_ms=("turnaround_ms", "mean"))
    a.index = [LVL.get(int(i), i) for i in a.index]
    print(a.round(1).to_string())
    if rx["ack_air_us"].notna().any():
        fixed = (rx["turnaround_ms"] - rx["ack_air_us"] / 1000).dropna()
        print(f"rx -> ACK end = fixed {fixed.median():.1f} ms + ACK airtime  (spread {fixed.min():.1f}-{fixed.max():.1f} ms)")
    tx = df[(df["ev"] == "tx")]
    if tx["budget_us"].notna().any():
        print(f"duty budget min {tx['budget_us'].min() / 1e6:.2f} s | tx_duty_limit: {int((df['ev'] == 'tx_duty_limit').sum())}")


def plot_recv(df: pd.DataFrame, rx: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(4, 1, figsize=(11, 10), sharex=True, gridspec_kw={"height_ratios": [2, 1.4, 1.4, 1.3]})
    gaps, resets = rx[rx["gap"]], rx[rx["seq_reset"]]
    for a in ax:
        a.grid(alpha=0.25)
        for _, g in gaps.iterrows():
            a.axvspan(g["t_rel"] - g["dt"], g["t_rel"], color=RED, alpha=0.08, lw=0)
        for x in resets["t_rel"]:
            a.axvline(x, color=GREY, ls="--", lw=1)

    a = ax[0]
    ok, fixed = rx[rx["corr"] == 0], rx[rx["corr"] > 0]
    a.scatter(ok["t_rel"], ok["lvl"], c=GREEN, s=34, label="decoded, corr=0")
    if len(fixed):
        a.scatter(fixed["t_rel"], fixed["lvl"], c=AMBER, s=60, label="decoded, corr>0")
    bad = df[df["ev"].isin(["rx_ecc_fail", "rx_crc_fail"])]
    if len(bad):
        a.scatter(bad["t_rel"], bad["lvl"], marker="x", c=RED, s=40, label="ecc/crc fail")
    a.set_yticks(list(LVL)[:6]); a.set_yticklabels([f"{k} {v}" for k, v in list(LVL.items())[:6]])
    a.set_ylabel("DATA level as received"); a.legend(loc="center right", fontsize=8, framealpha=0.9)
    a.set_title("AFEC receiver (node %d): what arrived, how clean, how fast it answered" % int(df["node"].iloc[0]))

    a = ax[1]
    a.plot(rx["t_rel"], rx["rssi"], "o-", ms=3, lw=1, color="#3b6fb6")
    a.set_ylabel("DATA RSSI (dBm)", color="#3b6fb6")
    b = a.twinx()
    b.plot(rx["t_rel"], rx["lqi"], "s-", ms=3, lw=1, color="#9a5bb5")
    b.set_ylabel("DATA LQI", color="#9a5bb5"); b.set_ylim(40, 60)

    a = ax[2]
    a.plot(rx["t_rel"], rx["seq"], "o-", ms=3, lw=1, color="#555")
    for _, r in resets.iterrows():
        a.annotate(f"seq {int(r['seq'] - r['dseq'])}→{int(r['seq'])}", (r["t_rel"], r["seq"]), xytext=(6, 12), textcoords="offset points", fontsize=8)
    a.set_ylabel("sender seq received")

    a = ax[3]
    cmap = {0: "#2e9e5b", 1: "#3b6fb6", 2: "#e0a030", 3: "#9a5bb5", 4: "#d64545", 5: "#555"}
    for lv, sub in rx.dropna(subset=["ack_lvl"]).groupby("ack_lvl"):
        a.scatter(sub["t_rel"], sub["turnaround_ms"], s=30, color=cmap.get(int(lv), GREY), label=f"ACK lvl {int(lv)} {LVL.get(int(lv), '')}")
    a.set_ylabel("rx → ACK sent (ms)"); a.set_xlabel("time since first event (s, node clock)")
    a.legend(fontsize=8, loc="center right", framealpha=0.9)

    fig.tight_layout()
    fig.savefig(path, dpi=140)
    print(f"\nfigure -> {path}")


#                               CAMPAIGN VIEW (metrics.py output)
# --------------------------------------------------------------------------- #
# Colour follows the configuration, never its rank. C5/C4/C2 take the first three categorical slots
# of the reference palette (validated all-pairs, light); C1 (no protection, the baseline) is neutral
# grey. Every configuration is also named on the x axis, so colour never carries identity alone.
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
CFG_COLOR = {"C5": "#2a78d6", "C4": "#eb6834", "C2": "#1baf7a", "C1": "#8a8f98"}
CFG_ORDER = ["C1", "C2", "C3", "C4", "C5"]
COND_ORDER = ["Q", "M", "B", "S"]
SEQ = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]   # one hue, light->dark
LEVELS = [0, 1, 2, 3, 4, 5, 7]


def _style() -> None:
    plt.rcParams.update({
        "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
        "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
        "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
        "axes.axisbelow": True, "axes.spines.top": False, "axes.spines.right": False,
        "font.size": 9, "axes.titlesize": 10, "axes.titleweight": "bold", "axes.titlelocation": "left",
        "legend.frameon": False, "xtick.major.size": 0, "ytick.major.size": 0,
    })


def _rank(cond: str, cfg: str) -> tuple:
    ci = COND_ORDER.index(cond) if cond in COND_ORDER else len(COND_ORDER)
    fi = CFG_ORDER.index(cfg) if cfg in CFG_ORDER else len(CFG_ORDER)
    return (ci, str(cond), fi, str(cfg))


def _layout(keys: list[tuple]) -> tuple[list[float], list[tuple[float, str]]]:
    """x per (condition, cfg), grouped by condition; returns xs and (centre, condition) labels."""
    xs, groups, x, prev = [], [], 0.0, None
    for cond, _ in keys:
        if prev is not None and cond != prev:
            x += 0.8
        xs.append(x)
        x += 1.0
        prev = cond
    for cond in dict.fromkeys(k[0] for k in keys):
        sel = [xx for (c, _), xx in zip(keys, xs) if c == cond]
        groups.append(((min(sel) + max(sel)) / 2, cond))
    return xs, groups


def _xaxis(ax, keys, xs, groups) -> None:
    ax.set_xticks(xs)
    ax.set_xticklabels([k[1] for k in keys])
    ax.grid(axis="x", visible=False)
    ax.set_xlim(min(xs) - 0.6, max(xs) + 0.6)
    tr = ax.get_xaxis_transform()
    for cx, cond in groups:
        ax.text(cx, -0.13, cond, transform=tr, ha="center", va="top", color=INK, fontweight="bold")


def _ci_panel(ax, cells, keys, xs, col, lo, hi, title, fmt, only: set | None = None) -> None:
    for (cond, cfg), x in zip(keys, xs):
        if only is not None and cfg not in only:
            continue
        r = cells[(cells["condition"] == cond) & (cells["cfg"] == cfg)].iloc[0]
        c = CFG_COLOR.get(cfg, GREY)
        if lo and pd.notna(r.get(lo)) and pd.notna(r.get(hi)):
            ax.vlines(x, r[lo], r[hi], color=c, lw=2, zorder=2)
        if pd.notna(r[col]):
            ax.scatter([x], [r[col]], s=48, color=c, edgecolor=SURFACE, linewidth=1.5, zorder=3)
            ax.annotate(fmt.format(r[col]), (x, r[col]), xytext=(7, 0), textcoords="offset points",
                        va="center", fontsize=7.5, color=INK2)
    ax.set_title(title)


def _legend(fig, cfgs) -> None:
    from matplotlib.lines import Line2D
    h = [Line2D([], [], marker="o", ls="", ms=7, color=CFG_COLOR.get(c, GREY), label=c) for c in cfgs]
    fig.legend(handles=h, loc="upper right", ncol=len(h), fontsize=8.5, handletextpad=0.2, columnspacing=1.0)


def _heat(ax, mat: np.ndarray, rows: list[str], cols: list[str], title: str) -> None:
    from matplotlib.colors import LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list("afec_seq", SEQ)
    m = np.ma.masked_where(~(mat > 0), mat)          # zero / NaN recede to a neutral ground
    ax.set_facecolor("#f0efec")
    ax.imshow(m, cmap=cmap, vmin=0, vmax=1, aspect="auto")
    ax.set_xticks(range(len(cols)), cols)
    ax.set_yticks(range(len(rows)), rows)
    ax.grid(False)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.set_xticks(np.arange(-0.5, len(cols)), minor=True)
    ax.set_yticks(np.arange(-0.5, len(rows)), minor=True)
    ax.grid(which="minor", color=SURFACE, linewidth=2)
    ax.tick_params(which="minor", length=0)
    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            v = mat[i, j]
            if np.isnan(v):
                continue
            txt = "0" if v == 0 else ("<1%" if v < 0.005 else f"{v:.0%}")
            ax.text(j, i, txt, ha="center", va="center", fontsize=8,
                    color="#ffffff" if v > 0.55 else (INK2 if v == 0 else INK))
    ax.set_title(title)


def _read(dirs: list[Path], name: str) -> pd.DataFrame:
    parts = [pd.read_csv(d / name) for d in dirs if (d / name).exists()]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def campaign_figures(dirs: list[Path], out: Path, kw: int, steps: str | None) -> None:
    _style()
    out.mkdir(parents=True, exist_ok=True)
    cells, blocks = _read(dirs, "cells.csv"), _read(dirs, "blocks.csv")
    packets, attempts, tconv = _read(dirs, "packets.csv"), _read(dirs, "attempts.csv"), _read(dirs, "tconv.csv")
    if cells.empty:
        raise SystemExit("no cells.csv in " + ", ".join(map(str, dirs)))
    keys = sorted({(r.condition, r.cfg) for r in cells.itertuples()}, key=lambda k: _rank(*k))
    xs, groups = _layout(keys)
    cfgs = sorted({k[1] for k in keys}, key=lambda c: _rank("", c))
    labels = [f"{c} · {f}" for c, f in keys]
    saved = []

    def save(fig, name):
        fig.savefig(out / name, dpi=160, bbox_inches="tight")
        plt.close(fig)
        saved.append(name)

    # 1) reliability: PDR_1, PDR<=2 (primary), PDR_R with Wilson 95 % CIs
    lo = max(0.0, float(np.nanmin(cells[["pdr1_lo", "pdr2_lo", "pdrR_lo"]].to_numpy())) - 0.02)
    fig, axs = plt.subplots(1, 3, figsize=(11, 3.6), sharey=True)
    for ax, (col, title) in zip(axs, [("pdr1", "PDR₁"), ("pdr2", "PDR≤2 (primary)"), ("pdrR", "PDR_R")]):
        _ci_panel(ax, cells, keys, xs, col, col + "_lo", col + "_hi", title, "{:.3f}")
        _xaxis(ax, keys, xs, groups)
        ax.set_ylim(lo, 1.005)
    axs[0].set_ylabel("delivered share (Wilson 95 % CI)")
    _legend(fig, cfgs)
    fig.suptitle("Reliability per cell", x=0.01, ha="left", fontweight="bold")
    fig.tight_layout(rect=(0, 0.02, 1, 0.93))
    save(fig, "cells_reliability.png")

    # 2) efficiency and adaptivity: one measure per panel, each on its own scale (never a dual axis)
    fig, axs = plt.subplots(1, 3, figsize=(11, 3.6))
    _ci_panel(axs[0], cells, keys, xs, "A_mean", "A_mean_lo", "A_mean_hi", "Mean attempts Ā", "{:.2f}")
    _ci_panel(axs[1], cells, keys, xs, "eta_air_us_per_B", "eta_air_lo", "eta_air_hi",
              "η_air (µs airtime per delivered byte)", "{:.0f}")
    _ci_panel(axs[2], cells, keys, xs, "sigma_per100", "sigma_lo", "sigma_hi", "Level switches per 100 packets (C4, C5)", "{:.1f}",
              only={"C4", "C5"})
    axs[0].set_ylabel("block-bootstrap 95 % CI")
    for ax in axs:
        _xaxis(ax, keys, xs, groups)
        top = ax.dataLim.y1 if np.isfinite(ax.dataLim.y1) and ax.dataLim.y1 > 0 else 1.0
        ax.set_ylim(0, top * 1.12)               # headroom for the top point and its label
    _legend(fig, cfgs)
    fig.suptitle("Efficiency and adaptivity per cell", x=0.01, ha="left", fontweight="bold")
    fig.tight_layout(rect=(0, 0.02, 1, 0.93))
    save(fig, "cells_efficiency.png")

    # 3) level occupancy (share of DATA frames per level)
    occ = np.array([[cells[(cells.condition == c) & (cells.cfg == f)].iloc[0].get(f"occ_{lv}", np.nan)
                     for lv in LEVELS] for c, f in keys], dtype=float)
    fig, ax = plt.subplots(figsize=(7, 0.45 * len(keys) + 1.2))
    _heat(ax, occ, labels, [f"{lv} {LVL[lv]}" for lv in LEVELS], "Level occupancy π_ℓ (share of DATA frames)")
    save(fig, "cells_occupancy.png")

    # 4) attempt distribution P(A = k)
    if not attempts.empty:
        ks = sorted(attempts["A"].astype(int).unique())
        pa = np.array([[attempts[(attempts.condition == c) & (attempts.cfg == f) & (attempts.A == k)]["p"].sum()
                        for k in ks] for c, f in keys], dtype=float)
        fig, ax = plt.subplots(figsize=(1.0 * len(ks) + 2.5, 0.45 * len(keys) + 1.2))
        _heat(ax, pa, labels, [f"A={k}" for k in ks], "Attempt distribution P(A = k)")
        save(fig, "cells_attempts.png")

    # 5) per-block spread and drift
    if not blocks.empty:
        fig, axs = plt.subplots(1, 2, figsize=(11, 3.6), gridspec_kw={"width_ratios": [1, 1.4]})
        rng = np.random.default_rng(0)
        for (cond, cfg), x in zip(keys, xs):
            b = blocks[(blocks.condition == cond) & (blocks.cfg == cfg)]
            c = CFG_COLOR.get(cfg, GREY)
            axs[0].scatter(x + rng.uniform(-0.18, 0.18, len(b)), b["pdr2"], s=40, color=c,
                           edgecolor=SURFACE, linewidth=1, zorder=3)
            if len(b):
                axs[0].hlines(b["pdr2"].median(), x - 0.3, x + 0.3, color=INK, lw=2, zorder=4)
        _xaxis(axs[0], keys, xs, groups)
        axs[0].set_title("PDR≤2 per block (bar = median)")
        axs[0].set_ylabel("PDR≤2")
        for (cond, cfg), b in blocks.sort_values("block").groupby(["condition", "cfg"]):
            axs[1].plot(b["block"], b["pdr2"], "-o", color=CFG_COLOR.get(cfg, GREY), lw=2, ms=6,
                        label=f"{cond} · {cfg}")
        axs[1].set_title("PDR≤2 in run order (drift check)")
        axs[1].set_xlabel("block id (run order)")
        from matplotlib.ticker import MaxNLocator
        axs[1].xaxis.set_major_locator(MaxNLocator(integer=True))
        axs[1].legend(fontsize=8, ncol=2)
        fig.tight_layout()
        save(fig, "blocks.png")

    # 6) level traces of the adaptive configurations
    if not packets.empty:
        pk = packets[packets["valid"]] if "valid" in packets else packets
        runs = blocks[["run", "block", "condition"]] if not blocks.empty else None
        if runs is not None:
            pk = pk.merge(runs, on=["run", "block"])
            adapt = [k for k in keys if k[1] in ("C4", "C5")]
            if adapt:
                fig, axs = plt.subplots(len(adapt), 1, figsize=(10, 2.2 * len(adapt) + 0.6), sharex=True, squeeze=False)
                for ax, (cond, cfg) in zip(axs[:, 0], adapt):
                    sub = pk[(pk.condition == cond) & (pk.cfg == cfg)]
                    c = CFG_COLOR[cfg]
                    for _, b in sub.groupby(["run", "block"]):
                        b = b.sort_values("idx")
                        ax.step(b["idx"], b["lvl_first"], where="post", color=c, lw=1, alpha=0.3)
                    med = sub.groupby("idx")["lvl_first"].quantile(0.5, interpolation="lower")   # stays on a level
                    ax.step(med.index, med.values, where="post", color=c, lw=2.2, label="median over blocks")
                    ax.axvspan(-0.5, kw - 0.5, color=GRID, alpha=0.6, lw=0)
                    if steps:
                        for st in steps.split(","):
                            i, cnd = st.split(":")
                            if int(i) > 0:
                                ax.axvline(int(i) - 0.5, color=INK2, ls="--", lw=1)
                            ax.text(int(i), 5.6, cnd, color=INK2, fontsize=8)
                    ax.set_yticks(range(6), [f"{lv} {LVL[lv]}" for lv in range(6)])
                    ax.set_ylim(-0.4, 6)
                    ax.set_title(f"{cond} · {cfg}: level of the first attempt per packet "
                                 f"({sub[['run', 'block']].drop_duplicates().shape[0]} blocks; shaded = warm-up)")
                axs[-1, 0].set_xlabel("packet index in block")
                axs[0, 0].legend(loc="upper right", fontsize=8)
                fig.tight_layout()
                save(fig, "levels_trace.png")

    # 7) T_conv (class S)
    if not tconv.empty:
        fig, ax = plt.subplots(figsize=(7, 3.4))
        tk = sorted({(r.cfg, int(r.step_idx), r.to) for r in tconv.itertuples()})
        rng = np.random.default_rng(1)
        for i, (cfg, st, to) in enumerate(tk):
            t = tconv[(tconv.cfg == cfg) & (tconv.step_idx == st)]["t_conv"]
            ok, miss = t.dropna(), int(t.isna().sum())
            ax.scatter(i + rng.uniform(-0.15, 0.15, len(ok)), ok, s=26, color=CFG_COLOR.get(cfg, GREY),
                       edgecolor=SURFACE, linewidth=1, zorder=3)
            if len(ok):
                ax.hlines(ok.median(), i - 0.3, i + 0.3, color=INK, lw=2, zorder=4)
            if miss:
                ax.text(i, ax.get_ylim()[1] if len(ok) else 1, f"{miss} never", ha="center", va="bottom",
                        fontsize=7.5, color=INK2)
        ax.set_xticks(range(len(tk)), [f"{c}\n@{s} → {to}" for c, s, to in tk])
        ax.grid(axis="x", visible=False)
        ax.set_ylabel("packets until settled")
        ax.set_title("T_conv after each step (bar = median)")
        fig.tight_layout()
        save(fig, "tconv.png")

    print(f"campaign figures -> {out}/: " + ", ".join(saved))


# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", type=Path, nargs="?", help="events.csv (single-node view)")
    ap.add_argument("--campaign", type=Path, nargs="+", metavar="OUT_DIR",
                    help="metrics.py output dir(s): per-cell campaign figures")
    ap.add_argument("--kw", type=int, default=10, help="campaign: warm-up shading in levels_trace")
    ap.add_argument("--steps", help="campaign: S step points for levels_trace, e.g. 0:Q,50:B,130:Q")
    ap.add_argument("--node", type=int)
    ap.add_argument("--role", choices=["auto", "sender", "receiver"], default="auto")
    ap.add_argument("--out", type=Path, default=Path("."))
    args = ap.parse_args()

    if args.campaign:
        campaign_figures(args.campaign, args.out, args.kw, args.steps)
        return
    if args.csv is None:
        ap.error("give an events.csv, or --campaign OUT_DIR")
    df, node, role = load(args.csv, args.node, args.role)
    args.out.mkdir(parents=True, exist_ok=True)
    if role == "sender":
        msgs, tx = build_tables(df)
        summary(df, msgs, tx, node)
        plot(df, msgs, tx, args.out / "eda_overview.png")
    else:
        rx = recv_tables(df)
        summary_recv(df, rx, node)
        plot_recv(df, rx, args.out / "eda_receiver.png")


if __name__ == "__main__":
    main()
