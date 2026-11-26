#!/usr/bin/env python3
"""
afec_logs.py - turn AFEC JSON-Lines logs (file or live serial) into pandas tables.

CLI examples
------------
    # from files (one or more nodes), with clock-offset estimation between nodes
    python afec_logs.py --mode file sender.log receiver.log --sync --out out/

    # live from serial, tee the raw capture, stop after 5 minutes
    python afec_logs.py --mode serial --port COM5 --baud 115200 --duration 300 \
        --raw-log sender.log --out out/

As a module (e.g. in a notebook)
--------------------------------
    from afec_logs import load_files, events
    df = load_files(["sender.log", "receiver.log"], sync=True)
    ok = events(df, "send_ok")          # only that event, empty columns dropped
    ok.plot(x="t_sync_s", y="rtt_ms")

Output tables
-------------
    events.csv            every event, one wide row each (columns unused by an event are NaN)
    by_event/<ev>.csv     one file per event type, all-NaN columns dropped
    offsets.json          estimated clock offsets per node (only with --sync)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import deque
from pathlib import Path
from typing import Iterable, Iterator

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------- #
# Schema constants (from afec_log_schema.md)
# --------------------------------------------------------------------------- #
MODE_NAMES = {0: "RAW(C1)", 1: "ARQ(C2)", 2: "HWFEC(C3)", 3: "RSSI(C4)", 4: "AFEC(C5)"}
LVL_NAMES = {0: "Hamming", 1: "BCH2", 2: "BCH5", 3: "RS2", 4: "RS4", 5: "RS8", 7: "none"}
LVL_COLS = ["lvl", "peer_lvl", "init_lvl", "req_lvl"]      # level columns on any event
CATEGORICAL = ["ev", "lv", "type", "cast", "reason", "tag", "action"]
BASE_COLS = ["t", "node", "lv", "ev"]


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #
def parse_line(line: str) -> dict | None | bool:
    """
    Returns a dict for a valid event, None for a line that is not JSON at all
    (boot banner, text log, ...), and False for a line that looks like JSON but is
    broken/truncated (typical for serial captures).
    """
    i = line.find("{")
    if i < 0:
        return None
    j = line.rfind("}")
    if j < i:
        return False
    try:
        obj = json.loads(line[i : j + 1])
    except json.JSONDecodeError:
        return False
    if not isinstance(obj, dict) or "ev" not in obj or "t" not in obj:
        return False
    return obj


class Collector:
    """Accumulates parsed rows and counts what was skipped."""

    def __init__(self, source: str):
        self.source = source
        self.rows: list[dict] = []
        self.ignored = 0     # non-JSON lines
        self.broken = 0      # JSON-looking but unparsable

    def feed(self, line: str) -> bool:
        obj = parse_line(line)
        if obj is None:
            self.ignored += 1
            return False
        if obj is False:
            self.broken += 1
            return False
        obj["source"] = self.source
        obj["line"] = len(self.rows) + self.ignored + self.broken
        self.rows.append(obj)
        return True


def iter_file(path: Path) -> Iterator[str]:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        yield from f


def iter_serial(port: str, baud: int, duration: float | None, max_lines: int | None,
                raw_log: Path | None) -> Iterator[str]:
    try:
        import serial  # pyserial
    except ImportError:
        sys.exit("pyserial is required for --mode serial:  pip install pyserial")

    raw = open(raw_log, "w", encoding="utf-8") if raw_log else None
    t0 = time.monotonic()
    n = 0
    try:
        with serial.Serial(port, baud, timeout=0.5) as ser:
            print(f"[serial] {port} @ {baud} - Ctrl-C to stop", file=sys.stderr)
            while True:
                if duration is not None and time.monotonic() - t0 >= duration:
                    break
                if max_lines is not None and n >= max_lines:
                    break
                data = ser.readline()
                if not data:
                    continue
                line = data.decode("utf-8", errors="replace")
                n += 1
                if raw:
                    raw.write(line if line.endswith("\n") else line + "\n")
                    raw.flush()
                yield line
    except KeyboardInterrupt:
        print("\n[serial] stopped by user", file=sys.stderr)
    finally:
        if raw:
            raw.close()


# --------------------------------------------------------------------------- #
# DataFrame construction
# --------------------------------------------------------------------------- #
def build_frame(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=BASE_COLS + ["source", "line"])
    df = pd.DataFrame(rows)

    # stable column order: base first, then the rest alphabetically
    rest = sorted(c for c in df.columns if c not in BASE_COLS + ["source", "line"])
    df = df[BASE_COLS + ["source", "line"] + rest]

    df["t_s"] = df["t"] / 1000.0                       # node-local time in seconds
    for c in CATEGORICAL:
        if c in df.columns:
            df[c] = df[c].astype("category")

    # human-readable enum names
    if "mode" in df.columns:
        df["mode_name"] = df["mode"].map(MODE_NAMES)
    for c in LVL_COLS:
        if c in df.columns:
            df[c + "_name"] = df[c].map(LVL_NAMES)
    is_lvl_ev = df["ev"].astype(str).str.startswith("lvl_") | (df["ev"] == "link_reset")
    for c in ("from", "to"):
        if c in df.columns:
            df[c + "_name"] = df[c].where(is_lvl_ev).map(LVL_NAMES)

    # keep per-node order (millis is monotonic per node; line order breaks ties)
    return df.sort_values(["node", "line"], kind="stable").reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Clock offset estimation (match tx on one node with rx on another)
# --------------------------------------------------------------------------- #
def _pair_offset(df: pd.DataFrame, a: int, b: int, tol_ms: float = 2000.0) -> float | None:
    """Estimated (t_b - t_a) in ms from DATA frames originated by node a and heard by node b."""
    tx = df[(df["ev"] == "tx") & (df["type"] == "DATA") & (df["node"] == a) & (df["src"] == a)]
    rx = df[(df["ev"] == "rx") & (df["type"] == "DATA") & (df["node"] == b) & (df["src"] == a)]
    tx = tx[["seq", "t"]].dropna()
    rx = rx[["seq", "t"]].dropna()
    if tx.empty or rx.empty:
        return None

    # pass 1: first tx vs first rx per seq -> rough offset
    first = pd.concat([tx.groupby("seq")["t"].min(), rx.groupby("seq")["t"].min()], axis=1, keys=["tx", "rx"]).dropna()
    if first.empty:
        return None
    rough = float(np.median(first["rx"] - first["tx"]))

    # pass 2: map every rx to the nearest same-seq tx in (rx - rough) time, then re-estimate.
    # Handles retries (same seq sent several times) and seq wrap-around.
    left = rx.assign(t_adj=rx["t"] - rough).sort_values("t_adj")
    right = tx.rename(columns={"t": "t_tx"}).assign(t_adj=tx["t"]).sort_values("t_adj")
    m = pd.merge_asof(left, right, on="t_adj", by="seq", direction="nearest", tolerance=tol_ms)
    m = m.dropna(subset=["t_tx"])
    if m.empty:
        return rough
    return float(np.median(m["t"] - m["t_tx"]))


def estimate_offsets(df: pd.DataFrame, ref: int | None = None) -> dict[int, float]:
    """
    Offset (ms) of each node's clock relative to the reference node: t_ref_frame = t - offset[node].
    Includes airtime/processing (a few ms to tens of ms) - fine for plots, not for sub-ms analysis.
    """
    nodes = sorted(int(n) for n in df["node"].unique())
    if not nodes:
        return {}
    ref = nodes[0] if ref is None else ref
    offsets = {ref: 0.0}
    if len(nodes) == 1:
        return offsets

    # pairwise edges: o[(a, b)] = t_b - t_a
    edges: dict[tuple[int, int], float] = {}
    for a in nodes:
        for b in nodes:
            if a == b:
                continue
            o = _pair_offset(df, a, b)
            if o is not None:
                edges.setdefault((a, b), o)
    # combine both directions when available
    graph: dict[int, dict[int, float]] = {n: {} for n in nodes}
    for (a, b), o in edges.items():
        rev = edges.get((b, a))
        val = o if rev is None else (o - rev) / 2.0
        graph[a][b] = val
        graph[b][a] = -val

    q = deque([ref])
    while q:
        cur = q.popleft()
        for nxt, o in graph[cur].items():
            if nxt not in offsets:
                offsets[nxt] = offsets[cur] + o
                q.append(nxt)

    for n in nodes:
        if n not in offsets:
            print(f"[sync] warning: no tx/rx matches linking node {n} to node {ref}; offset left at 0",
                  file=sys.stderr)
            offsets[n] = 0.0
    return offsets


def apply_offsets(df: pd.DataFrame, offsets: dict[int, float]) -> pd.DataFrame:
    df = df.copy()
    df["t_sync"] = df["t"] - df["node"].map(offsets).astype(float)
    t0 = df["t_sync"].min()
    df["t_sync_s"] = (df["t_sync"] - t0) / 1000.0      # seconds since first event on the merged timeline
    return df.sort_values(["t_sync", "node", "line"], kind="stable").reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Public helpers
# --------------------------------------------------------------------------- #
def _collect(lines: Iterable[str], source: str) -> Collector:
    c = Collector(source)
    for ln in lines:
        c.feed(ln)
    print(f"[{source}] {len(c.rows)} events, {c.broken} broken JSON lines, "
          f"{c.ignored} non-JSON lines skipped", file=sys.stderr)
    return c


def load_files(paths: Iterable[str | Path], sync: bool = False, ref_node: int | None = None
               ) -> pd.DataFrame:
    rows: list[dict] = []
    for p in paths:
        p = Path(p)
        rows += _collect(iter_file(p), p.name).rows
    return _finish(rows, sync, ref_node)


def load_serial(port: str, baud: int = 115200, duration: float | None = None,
                max_lines: int | None = None, raw_log: str | Path | None = None) -> pd.DataFrame:
    c = _collect(iter_serial(port, baud, duration, max_lines, Path(raw_log) if raw_log else None), port)
    return _finish(c.rows, sync=False, ref_node=None)


def _finish(rows: list[dict], sync: bool, ref_node: int | None) -> pd.DataFrame:
    df = build_frame(rows)
    if sync and not df.empty:
        offsets = estimate_offsets(df, ref_node)
        df = apply_offsets(df, offsets)
        df.attrs["offsets_ms"] = offsets
        print("[sync] offsets (ms, relative to reference): "
              + ", ".join(f"node {n}: {o:+.1f}" for n, o in offsets.items()), file=sys.stderr)
    return df


def events(df: pd.DataFrame, ev: str | list[str]) -> pd.DataFrame:
    """Rows of one (or several) event types with all-empty columns dropped."""
    names = [ev] if isinstance(ev, str) else list(ev)
    sub = df[df["ev"].isin(names)]
    return sub.dropna(axis=1, how="all").reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Export / summary
# --------------------------------------------------------------------------- #
def export(df: pd.DataFrame, out: Path, parquet: bool = False) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / "by_event").mkdir(exist_ok=True)
    df.to_csv(out / "events.csv", index=False)
    if parquet:
        try:
            df.to_parquet(out / "events.parquet", index=False)
        except ImportError:
            print("[export] pyarrow not installed - skipping parquet", file=sys.stderr)
    for ev, sub in df.groupby("ev", observed=True):
        sub.dropna(axis=1, how="all").to_csv(out / "by_event" / f"{ev}.csv", index=False)
    offs = df.attrs.get("offsets_ms")
    if offs:
        (out / "offsets.json").write_text(json.dumps({str(k): v for k, v in offs.items()}, indent=2))
    print(f"[export] wrote {len(df)} events to {out}/", file=sys.stderr)


def print_summary(df: pd.DataFrame) -> None:
    if df.empty:
        print("No events parsed.")
        return
    span = (df["t"].groupby(df["node"]).max() - df["t"].groupby(df["node"]).min()) / 1000
    print("\nNodes / duration (s):")
    print(span.round(1).to_string())
    print("\nEvents per type:")
    print(df.groupby(["ev", "lv"], observed=True).size().rename("count").to_string())


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description="Parse AFEC JSONL logs into pandas tables / CSV.")
    ap.add_argument("--mode", choices=["file", "serial"], required=True)
    ap.add_argument("inputs", nargs="*", help="log file(s) (file mode)")
    ap.add_argument("--port", help="serial port, e.g. COM5 or /dev/ttyUSB0 (serial mode)")
    ap.add_argument("--baud", type=int, default=115200)
    ap.add_argument("--duration", type=float, help="serial: stop after N seconds")
    ap.add_argument("--max-lines", type=int, help="serial: stop after N lines")
    ap.add_argument("--raw-log", type=Path, help="serial: also save the raw capture to this file")
    ap.add_argument("--sync", action="store_true",
                    help="estimate clock offsets between nodes and add t_sync / t_sync_s columns")
    ap.add_argument("--ref-node", type=int, help="node id used as time reference for --sync")
    ap.add_argument("--out", type=Path, default=Path("afec_out"), help="output directory")
    ap.add_argument("--parquet", action="store_true", help="also write events.parquet")
    ap.add_argument("--summary", action="store_true", help="print an event-count summary")
    args = ap.parse_args()

    if args.mode == "file":
        if not args.inputs:
            ap.error("file mode needs at least one input file")
        df = load_files(args.inputs, sync=args.sync, ref_node=args.ref_node)
    else:
        if not args.port:
            ap.error("serial mode needs --port")
        if args.sync:
            print("[note] --sync needs logs from >=2 nodes; capture each node with --raw-log "
                  "and re-run in file mode.", file=sys.stderr)
        df = load_serial(args.port, args.baud, args.duration, args.max_lines, args.raw_log)

    export(df, args.out, parquet=args.parquet)
    if args.summary:
        print_summary(df)


if __name__ == "__main__":
    main()
