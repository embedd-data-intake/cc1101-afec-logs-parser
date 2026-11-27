#!/usr/bin/env python3
"""
campaign.py - measurement-campaign orchestrator (run plan P0.3).

Drives the sender and receiver (and, for B and S, the interferer) over their serial command layer,
runs the blocks of one phase in a seeded randomised order, captures the raw logs, writes
meta.json, and finally runs metrics.py.

    python campaign.py plans/pilot-Q.json                    # run a phase
    python campaign.py plans/Q.json --dry-run                # print the schedule only
    python campaign.py plans/Q.json --resume                 # continue the latest unfinished Q run
    python campaign.py plans/Q.json --resume 2026-12-03-0915-Q   # continue that run
    python campaign.py plans/Q.json --blocks 1 --packets 12  # quick smoke run
    python campaign.py plans/Q.json --mqtt mqtt://192.168.1.10:1883   # logs over MQTT (see below)

Output: <data>/<run_id>/raw/{sender,receiver[,interferer]}.log, meta.json, out/ (metrics.py), figs/ (eda.py).
<data> defaults to ../data (the thesis repo's data/ when this runs inside the submodule).
<run_id> is "<YYYY-MM-DD-HHMM>-<title>": the local start time of the run plus the plan's run_id (the title).
It is also the `mark run=` tag and the MQTT topic segment, and meta.json's run_id.

Plan file (JSON)
----------------
    run_id            "Q"                     title; the run directory is <YYYY-MM-DD-HHMM>-<title>
                                              (a leading YYYY-MM-DD- in the title is dropped)
    condition         "Q"                     Q | M | B | S, passed to metrics.py
    seed              1                       schedule seed (stored in meta.json)
    blocks_per_config 18
    packets           110                     per block, including the K_w warm-up
    kw                10                      warm-up dropped by metrics.py (S: 0)
    len, interval     32, 2000                payload bytes, ms between packet starts
    common            {"init": 2, "retries": 3, "ackto": 400, "tx": 0, "dg": 2}
                      cfg keys sent to BOTH nodes for every configuration
    sender, receiver  {"tx": -30}             optional per-node cfg overrides (e.g. M TX power)
    configs           {"C2": {"mode": 2}, "C4": {"mode": 4, "rssi_up": "11,9,7,5,3"}, ...}
                      per-configuration cfg keys (also sent to both nodes)
    ports             {"sender": "/dev/cu.usbserial-5", "receiver": "/dev/cu.usbserial-0001"}
    interferer        optional: {"port": "...", "setup": ["burst len=10 period=200 jitter=80"],
                                 "during_block": true,            # "on" before run, "off" after
                                 "steps": [[50, "on"], [130, "off"]]}   # or: at packet idx
                      Firmware: ../cc1101-afec-interferer (serial only). `setup` is re-sent before every
                      block (so a reboot can't leave it unconfigured) and must answer burst_ok; on/off
                      must answer intf_on/intf_off; an interferer reboot or burst_err fails the block.
                      Per block, meta.json gets the interferer's intf_off counters (bursts, air_ms, duty).
    metrics           optional extra metrics.py args, e.g. ["--steps", "0:Q,50:B,130:Q",
                                                          "--appropriate", "Q=0-1,B=3-5"]
    notes             free-form: geometry, distance, TX power rationale, environment
    logs              optional: {"transport": "mqtt", "uri": "mqtt://192.168.1.10:1883", "commands": "mqtt"}
                      (default serial; --mqtt URI and --mqtt-commands set it from the command line)

Log transport
-------------
Commands always go over serial. With "serial" (firmware AFEC_LOG_SINK=1) the JSON events come back on
the same port. With "mqtt" (firmware AFEC_LOG_SINK=2 or 3, env:dev-mqtt) they come from the broker,
topics afec/<run_id>/<tx|rx>/log (mqtt_logs.py); raw/<role>.log then holds the MQTT lines plus the
plain-text serial lines (boot messages), so metrics.py reads it unchanged. JSON lines that still
arrive on serial (sink 3) go to raw/<role>-serial.txt, which metrics.py ignores. A block fails if a
node reports `log_drop` (its MQTT ring overflowed). Broker credentials: AFEC_MQTT_USER / AFEC_MQTT_PASS.

With "commands": "mqtt" (--mqtt-commands) the nodes are untethered: no serial port is opened (plan
`ports` may be left out), commands go to afec/cmd/<tx|rx> as "<cmd> id=<n>" (the firmware ignores a
QoS 1 redelivery of an id it has seen), and reboots are detected from the firmware's `boot` event.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Callable

from mqtt_logs import ROLE_NAMES, MqttLogs

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
CFG_FIELDS = {          # cfg key -> cfg_ok field, for verifying what the node applied
    "init": "init_lvl", "retries": "retries", "ackto": "ack_to_ms", "tx": "tx_dbm", "dg": "dg",
    "nackstep": "nack_step", "rssi_min": "rssi_min", "rssi_max": "rssi_max", "rssi_up": "rssi_up", "hys": "hys",
}


class BlockFailed(Exception):
    pass


# --------------------------------------------------------------------------- #
# Serial node
# --------------------------------------------------------------------------- #
class Node:
    """One node. Commands go over its serial port, or (untethered, `publish` given) to the broker as
    "<cmd> id=<n>" on afec/cmd/<tx|rx>. Log lines come from the serial port (serial transport) or from
    the broker through feed() (MQTT transport); either way they are teed to the raw log and parsed."""

    def __init__(self, role: str, port: str | None, log_path: Path, append: bool, mqtt: bool = False,
                 publish: Callable[[str], None] | None = None):
        self.role, self.port, self.mqtt, self.publish = role, port, mqtt, publish
        self.side = (open(log_path.with_name(f"{role}-serial.txt"), "a" if append else "w", encoding="utf-8")
                     if mqtt and publish is None else None)   # serial JSON lines (sink 3): kept, not parsed
        self.events: list[dict] = []
        self.boots = 0                       # reboots seen: "CC1101 ready" lines, or `boot` events if untethered
        self.cmd_id = int(time.time() * 1000) & 0x7FFFFFFF   # MQTT command ids: increasing, unique per run
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.log = open(log_path, "a" if append else "w", encoding="utf-8")
        self.ser = None
        self.thread = None
        if publish is not None:              # untethered: no serial port at all
            return
        import serial  # pyserial
        s = serial.Serial()
        s.port, s.baudrate, s.timeout = port, 115200, 0.2
        s.dtr = s.rts = False                 # don't hold the ESP32 in reset / bootloader
        s.open()
        self.ser = s
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self) -> None:
        while not self.stop.is_set():
            try:
                raw = self.ser.readline()
            except Exception as e:            # port vanished (USB unplugged)
                print(f"[{self.role}] serial error: {e}", file=sys.stderr)
                break
            if not raw:
                continue
            line = raw.decode("utf-8", "replace")
            if self.side and line.startswith("{"):
                self.side.write(line if line.endswith("\n") else line + "\n")
                self.side.flush()
                continue
            if line.startswith("CC1101 ready"):
                with self.lock:
                    self.boots += 1
            self.feed(line)

    def feed(self, line: str) -> None:
        """One log line (from serial or MQTT): tee it to the raw log and keep it if it is an event."""
        ev = None
        if line.startswith("{"):
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                pass
        with self.lock:
            self.log.write(line if line.endswith("\n") else line + "\n")
            self.log.flush()
            if ev is not None:
                self.events.append(ev)
                if self.ser is None and ev.get("ev") == "boot":
                    self.boots += 1

    def cursor(self) -> int:
        with self.lock:
            return len(self.events)

    def send(self, cmd: str) -> int:
        """Send one command line; returns the event cursor to wait from."""
        c = self.cursor()
        if self.publish is not None:
            self.cmd_id += 1
            self.publish(f"{cmd} id={self.cmd_id}")
        else:
            self.ser.write((cmd + "\n").encode())
            self.ser.flush()
        return c

    def wait(self, ev: str, cursor: int, timeout: float, boots: int | None = None, **match) -> dict:
        """First event `ev` (with matching fields) after `cursor`. Raises on cmd_err, reboot or timeout."""
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            with self.lock:
                new = self.events[cursor:]
                rebooted = boots is not None and self.boots > boots
            for e in new:
                if e.get("ev") == "cmd_err":
                    raise BlockFailed(f"{self.role}: cmd_err {e.get('err')} ({e.get('detail')})")
                if e.get("ev") == ev and all(e.get(k) == v for k, v in match.items()):
                    return e
            if rebooted:
                raise BlockFailed(f"{self.role}: node rebooted")
            time.sleep(0.05)
        raise BlockFailed(f"{self.role}: timeout waiting for {ev}")

    def close(self) -> None:
        self.stop.set()
        if self.ser is not None:
            self.thread.join(timeout=1)
            self.ser.close()
        with self.lock:
            self.log.close()
        if self.side:
            self.side.close()

    def since(self, cursor: int, ev: str) -> list[dict]:
        with self.lock:
            return [e for e in self.events[cursor:] if e.get("ev") == ev]


# --------------------------------------------------------------------------- #
# Plan and schedule
# --------------------------------------------------------------------------- #
def load_plan(path: Path, a: argparse.Namespace) -> dict:
    p = json.loads(path.read_text())
    for k in ("run_id", "condition", "configs"):     # ports: checked in log_transport (optional untethered)
        if k not in p:
            sys.exit(f"plan is missing '{k}'")
    p.setdefault("seed", 1)
    p.setdefault("blocks_per_config", 18)
    p.setdefault("packets", 110)
    p.setdefault("kw", 10)
    p.setdefault("len", 32)
    p.setdefault("interval", 2000)
    p.setdefault("common", {})
    if a.run_id:
        p["run_id"] = a.run_id
    if a.blocks is not None:
        p["blocks_per_config"] = a.blocks
    if a.packets is not None:
        p["packets"] = a.packets
    if a.configs:
        p["configs"] = {c: p["configs"][c] for c in a.configs.split(",")}
    for name, c in p["configs"].items():
        if "mode" not in c:
            sys.exit(f"config {name} has no mode")
        if int(c["mode"]) == 3:
            sys.exit("C3 is excluded (no fixed-length FEC driver patch)")
    return p


def schedule(plan: dict) -> list[str]:
    """Permuted rounds: every round runs each configuration once, in a seeded random order."""
    rng = random.Random(plan["seed"])
    names = sorted(plan["configs"])
    out: list[str] = []
    for _ in range(plan["blocks_per_config"]):
        r = names[:]
        rng.shuffle(r)
        out += r
    return out


def cfg_args(plan: dict, cfg: str, role: str) -> dict:
    d = {**plan["common"], **plan["configs"][cfg], **plan.get(role, {})}
    return {k: d[k] for k in ["mode"] + list(CFG_FIELDS) if k in d}


def cfg_line(args: dict) -> str:
    return "cfg " + " ".join(f"{k}={v}" for k, v in args.items())


def check_cfg(ok: dict, args: dict, cfg: str, role: str) -> None:
    if ok.get("cfg") != cfg:
        raise BlockFailed(f"{role}: cfg_ok says {ok.get('cfg')}, expected {cfg}")
    for k, f in CFG_FIELDS.items():
        if k not in args:
            continue
        want = args[k]                     # cfg_ok echoes the requested values (tx before driver rounding)
        if str(ok.get(f)) != str(want):
            raise BlockFailed(f"{role}: {f}={ok.get(f)}, expected {want}")


# --------------------------------------------------------------------------- #
# Provenance
# --------------------------------------------------------------------------- #
def git_info(path: Path) -> dict:
    def g(*a: str) -> str:
        return subprocess.run(["git", "-C", str(path), *a], capture_output=True, text=True).stdout.strip()
    return {"commit": g("rev-parse", "--short", "HEAD"), "dirty": bool(g("status", "--porcelain"))}


def provenance() -> dict:
    subs = {"lib": "cc1101-afec", "sender": "cc1101-afec-sender", "receiver": "cc1101-afec-receive",
            "interferer": "cc1101-afec-interferer", "parser": "cc1101-afec-logs-parser", "thesis": "."}
    return {k: git_info(REPO / v) for k, v in subs.items() if (REPO / v).exists()}


def write_meta(path: Path, meta: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(meta, indent=2))
    os.replace(tmp, path)


def now() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


# --------------------------------------------------------------------------- #
# Running
# --------------------------------------------------------------------------- #
def abort_run(s: Node, block: int) -> None:
    """Stop a sender run that may still be active and wait until it has ended. If it already ended
    or the sender rebooted, `stop` gets cmd_err `no run active`, which is fine."""
    c = s.send("stop")
    try:
        s.wait("block_end", c, 5, block=block)
    except BlockFailed:
        pass


def run_block(nodes: dict[str, Node], plan: dict, cfg: str, block: int, intf: dict | None) -> dict:
    s, r = nodes["sender"], nodes["receiver"]
    boots = {k: n.boots for k, n in nodes.items()}
    first = {k: n.cursor() for k, n in nodes.items()}
    applied = {}
    for role in ("receiver", "sender"):              # receiver first: it must be ready when data flows
        n, args = nodes[role], cfg_args(plan, cfg, role)
        c = n.send(cfg_line(args))
        ok = n.wait("cfg_ok", c, 8, boots[role])
        check_cfg(ok, args, cfg, role)
        applied[role] = ok
        c = n.send(f"mark run={plan['run_id']} block={block} cfg={cfg}")
        n.wait("mark", c, 3, boots[role], block=block)

    i = nodes.get("interferer")
    if i:
        if i.boots > boots["interferer"]:
            raise BlockFailed("interferer: node rebooted")
        for cmd in (intf or {}).get("setup", []):     # off here (every block ends with off)
            c = i.send(cmd)
            i.wait("burst_ok", c, 3, boots["interferer"])
    if i and intf and intf.get("during_block"):
        c = i.send("on")
        i.wait("intf_on", c, 3, boots["interferer"])
    steps = sorted((intf or {}).get("steps", []))
    intf_res: dict = {}
    retries = int(applied["sender"].get("retries", 3))
    ackto = int(applied["sender"].get("ack_to_ms", 400))
    worst_ms = max(plan["interval"], (1 + retries) * (ackto + 150))
    timeout = plan["packets"] * worst_ms / 1000 + 30

    c = s.send(f"run n={plan['packets']} len={plan['len']} interval={plan['interval']}")
    start = s.wait("run_start", c, 5, boots["sender"])
    t0, step_i, seen = time.monotonic(), 0, c
    try:
        while True:
            if i and step_i < len(steps):               # S: switch the interferer at packet indices
                with s.lock:
                    new = s.events[seen:]
                    seen += len(new)
                for e in new:
                    if e.get("ev") == "app_tx":
                        while step_i < len(steps) and e.get("idx", -1) + 1 >= steps[step_i][0]:
                            i.send(steps[step_i][1])
                            step_i += 1
            try:
                end = s.wait("block_end", c, 0.2, boots["sender"], block=block)
                break
            except BlockFailed as e:
                if "timeout" not in str(e):
                    raise
            if r.boots > boots["receiver"]:
                raise BlockFailed("receiver: node rebooted")
            if time.monotonic() - t0 > timeout:
                raise BlockFailed(f"sender: block did not finish within {timeout:.0f} s")
    except BlockFailed:
        abort_run(s, block)      # the sender may still be mid-run: a retry's cfg would get `busy`
        raise
    finally:
        if i and intf and (intf.get("during_block") or steps):
            c = i.send("off")
            try:
                intf_res = i.wait("intf_off", c, 3, boots["interferer"])
            except BlockFailed:
                pass                     # reported below (reboot / missing reply)
    if i:
        if i.boots > boots["interferer"]:
            raise BlockFailed("interferer: node rebooted")
        if errs := i.since(first["interferer"], "burst_err") + i.since(first["interferer"], "cmd_err"):
            raise BlockFailed(f"interferer: {errs[0].get('ev')} {errs[0].get('err')}")
        if intf and (intf.get("during_block") or steps):
            if not intf_res:
                raise BlockFailed("interferer: no intf_off reply")
            if steps and len(i.since(first["interferer"], "intf_on")) == 0:
                raise BlockFailed("interferer: step 'on' never acknowledged")
    if end.get("aborted"):
        raise BlockFailed("sender: block aborted")
    for role in ("sender", "receiver"):
        n = nodes[role]
        if n.ser is None:     # untethered: a reboot only shows once the node is back on WiFi. Its `boot`
            c = n.send("hello")                       # precedes any later reply, so after this reply
            n.wait("hello", c, 30, boots[role])       # a reboot during the block has been counted
    for role, n in nodes.items():                     # the receiver is never waited on during the run
        if drops := n.since(first[role], "log_drop"):
            raise BlockFailed(f"{role}: {sum(int(d.get('dropped', 0)) for d in drops)} log lines dropped (MQTT)")
    return {"run_start": start, "block_end": end, "cfg_ok": applied,
            "interferer": {k: intf_res[k] for k in ("bursts", "skipped", "errors", "air_ms", "span_ms",
                                                    "duty_permille", "seed") if k in intf_res}}


def log_transport(plan: dict) -> dict:
    lg = plan.get("logs") or {}
    t, cmds = lg.get("transport", "serial"), lg.get("commands", "serial")
    if t not in ("serial", "mqtt"):
        sys.exit(f"plan logs.transport must be serial or mqtt, not {t!r}")
    if cmds not in ("serial", "mqtt"):
        sys.exit(f"plan logs.commands must be serial or mqtt, not {cmds!r}")
    if t == "mqtt" and not lg.get("uri"):
        sys.exit("plan logs.transport is mqtt but logs.uri is missing (or pass --mqtt URI)")
    if cmds == "mqtt" and t != "mqtt":
        sys.exit("plan logs.commands is mqtt but logs.transport is not (use --mqtt URI --mqtt-commands)")
    if cmds == "serial":
        for role in ("sender", "receiver"):
            if not (plan.get("ports") or {}).get(role):
                sys.exit(f"plan ports.{role} is missing (needed unless commands go over MQTT)")
    return {"transport": t, "commands": cmds, **({"uri": lg["uri"]} if t == "mqtt" else {})}


def open_nodes(plan: dict, raw: Path, append: bool) -> tuple[dict[str, Node], MqttLogs | None]:
    lt = log_transport(plan)
    mqtt, untethered = lt["transport"] == "mqtt", lt["commands"] == "mqtt"
    nodes: dict[str, Node] = {}
    broker = None
    if mqtt:     # subscribe before opening the ports (which resets the boards), so boot lines are kept
        def route(run_id: str, role: str, line: str) -> None:
            n = nodes.get(ROLE_NAMES.get(role, ""))
            if n is not None and n.mqtt:
                n.feed(line)
        broker = MqttLogs(plan["logs"]["uri"], route, client_id=f"afec-campaign-{os.getpid()}")
    for role in ("sender", "receiver"):
        pub = None
        if untethered:              # commands to afec/cmd/<tx|rx>; no serial port is opened
            topic = f"afec/cmd/{ 'tx' if role == 'sender' else 'rx' }"
            pub = (lambda t: lambda line: broker.publish(t, line))(topic)
        nodes[role] = Node(role, None if untethered else plan["ports"][role], raw / f"{role}.log",
                           append, mqtt, pub)
    intf = plan.get("interferer")
    if intf and intf.get("port"):            # the interferer has no MQTT sink: always serial
        nodes["interferer"] = Node("interferer", intf["port"], raw / "interferer.log", append)
    return nodes, broker


def handshake(nodes: dict[str, Node], mqtt: bool) -> dict:
    """Opening a CP2102 port resets the ESP32: wait for the boot, then check roles with hello.
    With MQTT the reply only comes once the node has joined WiFi and the broker, hence the longer wait.
    Untethered nodes (commands over MQTT) are not reset: hello is sent at once."""
    if any(n.ser is not None for n in nodes.values()):
        time.sleep(2.5)
    info = {}
    for role in ("sender", "receiver"):
        n = nodes[role]
        c = n.send("hello")
        try:
            h = n.wait("hello", c, 20 if mqtt else 5)
        except BlockFailed:
            via = "the MQTT broker" if mqtt else n.port
            if n.ser is None:
                sys.exit(f"{role}: no hello over MQTT (afec/cmd/{'tx' if role == 'sender' else 'rx'}). Is the "
                         f"node powered, on WiFi, and built with AFEC_LOG_SINK=2/3 and AFEC_MQTT_COMMANDS=1?")
            sys.exit(f"{role}: no hello on {via}. Is the node built with the matching AFEC_LOG_SINK "
                     f"({'2/3, env:dev-mqtt' if mqtt else '1/3, env:dev'}), and (MQTT) are WiFi and broker reachable?")
        if h.get("role") != role:
            sys.exit(f"{n.port} answered as {h.get('role')!r}, expected {role}: swap the ports in the plan")
        sink = h.get("sink", "serial")       # firmware before the log sink had no field: serial
        if mqtt and "mqtt" not in sink:
            sys.exit(f"{role}: firmware log sink is {sink!r}, the plan wants mqtt")
        info[role] = {"port": n.port or "mqtt", "node": h.get("node"), "build": h.get("build"), "sink": sink}
    if (n := nodes.get("interferer")) is not None:
        c = n.send("hello")
        try:
            h = n.wait("hello", c, 5)
        except BlockFailed:
            sys.exit(f"interferer: no hello on {n.port}. Is it flashed with cc1101-afec-interferer?")
        if h.get("role") != "interferer":
            sys.exit(f"{n.port} answered as {h.get('role')!r}, expected interferer: check interferer.port")
        info["interferer"] = {"port": n.port, "node": h.get("node"), "build": h.get("build"), "sink": "serial"}
    return info


def run_metrics(run_dir: Path, plan: dict, failed: list[int]) -> None:
    cmd = [sys.executable, str(HERE / "metrics.py"), str(run_dir / "raw"), "--out", str(run_dir / "out"),
           "--kw", str(plan["kw"]), "--condition", plan["condition"], *plan.get("metrics", [])]
    if failed:
        cmd += ["--exclude-blocks", ",".join(map(str, failed))]
    print("[campaign] " + " ".join(cmd[1:]), file=sys.stderr)
    if subprocess.run(cmd).returncode != 0:
        return
    figs = [sys.executable, str(HERE / "eda.py"), "--campaign", str(run_dir / "out"),
            "--out", str(run_dir / "figs"), "--kw", str(plan["kw"])]
    steps = plan.get("metrics", [])
    if "--steps" in steps:
        figs += ["--steps", steps[steps.index("--steps") + 1]]
    subprocess.run(figs)


STAMP = re.compile(r"^\d{4}-\d{2}-\d{2}-")          # leading date in a plan title
RUN_DIR = re.compile(r"^\d{4}-\d{2}-\d{2}-\d{4}-")    # <YYYY-MM-DD-HHMM>- prefix of a run directory


def run_title(plan: dict) -> str:
    return STAMP.sub("", RUN_DIR.sub("", plan["run_id"])) or plan["run_id"]


def resolve_run_dir(data: Path, plan: dict, resume: bool | str | None) -> Path:
    """New run: <data>/<YYYY-MM-DD-HHMM>-<title>, local start time. --resume DIR: that directory.
    --resume alone: the latest run of this title whose meta.json status is not done."""
    title = run_title(plan)
    if isinstance(resume, str):
        d = data / resume
        if not (d / "meta.json").exists():
            sys.exit(f"--resume {resume}: no {d / 'meta.json'}")
        return d
    if resume:
        runs = sorted(d for d in data.glob(f"*-{title}") if RUN_DIR.match(d.name)
                      and d.name[len("YYYY-MM-DD-HHMM-"):] == title and (d / "meta.json").exists())
        open_runs = [d for d in runs if json.loads((d / "meta.json").read_text()).get("status") != "done"]
        if not open_runs:
            sys.exit(f"--resume: no unfinished '<YYYY-MM-DD-HHMM>-{title}' run in {data}")
        return open_runs[-1]
    return data / f"{dt.datetime.now():%Y-%m-%d-%H%M}-{title}"


def main() -> None:
    ap = argparse.ArgumentParser(description="Run one campaign phase from a JSON plan.")
    ap.add_argument("plan", type=Path)
    ap.add_argument("--data", type=Path, default=REPO / "data", help="data root (default: thesis repo data/)")
    ap.add_argument("--run-id", help="override the plan's run_id (the title; the start time is still prefixed)")
    ap.add_argument("--blocks", type=int, help="override blocks_per_config")
    ap.add_argument("--packets", type=int, help="override packets per block")
    ap.add_argument("--configs", help="only these configurations, e.g. C2,C5")
    ap.add_argument("--retries", type=int, default=2, help="re-runs of a failed block")
    ap.add_argument("--dry-run", action="store_true", help="print the schedule and timing, touch nothing")
    ap.add_argument("--resume", nargs="?", const=True, metavar="RUN_DIR",
                    help="continue an interrupted run: the given <data>/RUN_DIR, or by default the latest "
                         "<YYYY-MM-DD-HHMM>-<title> run of this plan that is not done")
    ap.add_argument("--no-metrics", action="store_true")
    ap.add_argument("--mqtt", metavar="URI", help="read the logs from this MQTT broker (overrides plan logs)")
    ap.add_argument("--mqtt-commands", action="store_true",
                    help="also send the commands over MQTT (untethered nodes, no serial ports); needs --mqtt")
    a = ap.parse_args()

    plan = load_plan(a.plan, a)
    if a.mqtt:
        plan["logs"] = {**(plan.get("logs") or {}), "transport": "mqtt", "uri": a.mqtt}
    if a.mqtt_commands:
        plan["logs"] = {**(plan.get("logs") or {}), "commands": "mqtt"}
    logs = log_transport(plan)
    sched = schedule(plan)
    est_h = len(sched) * (plan["packets"] * plan["interval"] / 1000 + 10) / 3600
    print(f"[campaign] {plan['run_id']}: {len(sched)} blocks x {plan['packets']} packets, "
          f"~{est_h:.1f} h of radio time, logs via {logs['transport']}, commands via {logs['commands']} "
          f"{logs.get('uri', '')}", file=sys.stderr)
    if a.dry_run:
        for k, cfg in enumerate(sched):
            print(k, cfg, cfg_line(cfg_args(plan, cfg, "sender")), "|", cfg_line(cfg_args(plan, cfg, "receiver")))
        return

    run_dir = resolve_run_dir(a.data, plan, a.resume)
    plan["run_id"] = run_dir.name                    # mark run= tag, MQTT topic, meta.json run_id
    print(f"[campaign] run directory {run_dir}", file=sys.stderr)
    raw = run_dir / "raw"
    meta_path = run_dir / "meta.json"
    if a.resume:
        meta = json.loads(meta_path.read_text())
        if meta["schedule"] != sched:
            sys.exit("schedule differs from the one in meta.json (plan or overrides changed)")
    else:
        if meta_path.exists():
            sys.exit(f"{run_dir} already has a meta.json: use --resume {run_dir.name} or wait a minute")
        raw.mkdir(parents=True, exist_ok=True)
        meta = {"run_id": plan["run_id"], "condition": plan["condition"], "created": now(),
                "host": socket.gethostname(), "plan_file": str(a.plan), "plan": plan,
                "schedule": sched, "schedule_rule": "permuted rounds, random.Random(seed).shuffle per round",
                "firmware": provenance(), "radio": {"freq_mhz": 433.92, "sync_word": "0x6B1D",
                                                   "bitrate_bps": 4800, "whitening": True, "hw_crc": False},
                "sessions": [], "blocks": [], "status": "running"}
    done = {b["sched_idx"] for b in meta["blocks"] if b["status"] == "ok"}
    next_block = max([b["block"] for b in meta["blocks"]], default=-1) + 1

    def on_term(*_):                                   # kill / SIGTERM: stop cleanly like Ctrl-C
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, on_term)
    nodes, broker = open_nodes(plan, raw, append=a.resume)
    mqtt = broker is not None
    current: dict | None = None                        # block in progress (not yet in meta)
    session: dict = {}
    try:
        session = {"started": now(), "logs": logs, "firmware": provenance()}
        meta["sessions"].append(session)
        session["nodes"] = handshake(nodes, mqtt)
        write_meta(meta_path, meta)
        t_start = time.monotonic()
        todo = [k for k in range(len(sched)) if k not in done]
        for n_done, k in enumerate(todo):
            cfg = sched[k]
            for attempt in range(1 + a.retries):
                block, next_block = next_block, next_block + 1
                rec = {"sched_idx": k, "block": block, "cfg": cfg, "attempt": attempt, "started": now()}
                current = rec
                try:
                    res = run_block(nodes, plan, cfg, block, plan.get("interferer"))
                    e = res["block_end"]
                    rec.update(status="ok", ok=e["ok"], no_ack=e["no_ack"], duty_limit=e["duty_limit"],
                               dur_ms=e["dur_ms"])
                    if res["interferer"]:
                        rec["interferer"] = res["interferer"]
                    if not meta.get("cfg_ok", {}).get(cfg):
                        meta.setdefault("cfg_ok", {})[cfg] = res["cfg_ok"]
                except BlockFailed as ex:
                    rec.update(status="failed", error=str(ex))
                    print(f"[campaign] block {block} ({cfg}) FAILED: {ex}", file=sys.stderr)
                    if "rebooted" in str(ex):
                        time.sleep(3)                      # let it finish booting
                rec["ended"] = now()
                meta["blocks"].append(rec)
                current = None
                write_meta(meta_path, meta)
                if rec["status"] == "ok":
                    break
            else:
                meta["status"] = "failed"
                raise SystemExit(f"[campaign] schedule entry {k} ({cfg}) failed {1 + a.retries} times - stopping")
            el = time.monotonic() - t_start
            eta = el / (n_done + 1) * (len(todo) - n_done - 1) / 60
            print(f"[campaign] {n_done + 1}/{len(todo)} block {rec['block']} {cfg}: "
                  f"ok {rec['ok']}/{plan['packets']} no_ack {rec['no_ack']} duty {rec['duty_limit']} "
                  f"| ETA {eta:.0f} min", file=sys.stderr)
        meta["status"] = "done"
    except KeyboardInterrupt:
        nodes["sender"].send("stop")
        if nodes.get("interferer"):
            nodes["interferer"].send("off")
        if current is not None:                        # record it, so its block id is never reused
            current.update(status="interrupted", ended=now())
            meta["blocks"].append(current)
        meta["status"] = "interrupted"
        print("\n[campaign] interrupted - continue with --resume", file=sys.stderr)
    finally:
        for role in ("sender", "receiver"):
            try:
                c = nodes[role].send("stats")
                meta.setdefault("final_stats", {})[role] = nodes[role].wait("stats", c, 3)
                if mqtt:                                    # node-side counters of the MQTT sink
                    ls = nodes[role].wait("log_stats", c, 3)
                    session.setdefault("log_stats", {})[role] = ls
                    if ls.get("dropped"):
                        print(f"[campaign] WARNING {role}: {ls['dropped']} log lines dropped this boot",
                              file=sys.stderr)
            except BlockFailed as ex:
                print(f"[campaign] final stats: {ex}", file=sys.stderr)
        if (n := nodes.get("interferer")) is not None:
            try:
                n.send("off")
                c = n.send("stats")
                meta.setdefault("final_stats", {})["interferer"] = n.wait("stats", c, 3)
            except BlockFailed as ex:
                print(f"[campaign] final stats: {ex}", file=sys.stderr)
        if mqtt:
            session["mqtt"] = {"messages": broker.messages, "disconnects": broker.disconnects}
        meta["ended"] = now()
        write_meta(meta_path, meta)
        time.sleep(1.0 if mqtt else 0.3)                   # let the last QoS 1 messages land
        if broker:
            broker.close()
        for n in nodes.values():
            n.close()

    if not a.no_metrics:
        failed = [b["block"] for b in meta["blocks"] if b["status"] != "ok"]
        run_metrics(run_dir, plan, failed)


if __name__ == "__main__":
    main()
