"""
mqtt_logs.py - read the nodes' log lines from an MQTT broker (firmware built with AFEC_LOG_SINK=2|3).

The firmware publishes one JSON line per message, QoS 1, on

    afec/<run_id>/<role>/log        role: tx (sender), rx (receiver)

<run_id> is the `run=` of the node's last `mark` ("boot" before the first one). See "Log sink" in
cc1101-afec-sender/docs/firmware.md. Used by campaign.py (MQTT transport) and parse.py --mode mqtt.

Credentials, if the broker needs them, come from the environment: AFEC_MQTT_USER, AFEC_MQTT_PASS.
"""
from __future__ import annotations

import os
import sys
import threading
from typing import Callable
from urllib.parse import urlparse

ROLE_NAMES = {"tx": "sender", "rx": "receiver", "intf": "interferer"}


def topic_filter(run_id: str | None = None, role: str | None = None) -> str:
    return f"afec/{run_id or '+'}/{role or '+'}/log"


def split_topic(topic: str) -> tuple[str, str] | None:
    """afec/<run_id>/<role>/log -> (run_id, role), or None for any other topic."""
    p = topic.split("/")
    if len(p) != 4 or p[0] != "afec" or p[3] != "log":
        return None
    return p[1], p[2]


class MqttLogs:
    """One broker connection. `on_line(run_id, role, line)` runs in paho's network thread for every
    log message; `line` has no trailing newline. Subscribes (QoS 1) on every (re)connect."""

    def __init__(self, uri: str, on_line: Callable[[str, str, str], None],
                 topic: str = topic_filter(), client_id: str | None = None, timeout: float = 10.0):
        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            sys.exit("paho-mqtt is required for MQTT logs:  pip install -r requirements.txt")
        u = urlparse(uri if "://" in uri else "mqtt://" + uri)
        if u.scheme not in ("mqtt", "tcp"):
            sys.exit(f"unsupported broker URI {uri!r}: use mqtt://host[:port]")
        self.uri, self.topic, self.on_line = uri, topic, on_line
        self.connected = threading.Event()
        self.messages = 0
        self.disconnects = 0
        self.closing = False
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                             client_id=client_id or f"afec-reader-{os.getpid()}")
        user = os.environ.get("AFEC_MQTT_USER") or u.username
        if user:
            self.c.username_pw_set(user, os.environ.get("AFEC_MQTT_PASS") or u.password)
        self.c.on_connect = self._on_connect
        self.c.on_disconnect = self._on_disconnect
        self.c.on_message = self._on_message
        try:
            self.c.connect(u.hostname or "localhost", u.port or 1883, keepalive=30)
        except OSError as e:
            sys.exit(f"cannot reach MQTT broker {uri}: {e}")
        self.c.loop_start()
        if not self.connected.wait(timeout):
            self.close()
            sys.exit(f"MQTT broker {uri}: no CONNACK within {timeout:.0f} s (credentials?)")

    def _on_connect(self, c, userdata, flags, reason, props) -> None:
        if reason.is_failure:
            print(f"[mqtt] connect refused: {reason}", file=sys.stderr)
            return
        c.subscribe(self.topic, qos=1)
        self.connected.set()

    def _on_disconnect(self, c, userdata, flags, reason, props) -> None:
        self.connected.clear()
        if self.closing:
            return
        self.disconnects += 1
        print(f"[mqtt] disconnected ({reason}); paho reconnects", file=sys.stderr)

    def _on_message(self, c, userdata, m) -> None:
        rt = split_topic(m.topic)
        if rt is None:
            return
        self.messages += 1
        self.on_line(rt[0], rt[1], m.payload.decode("utf-8", "replace").rstrip("\r\n"))

    def publish(self, topic: str, payload: str) -> None:
        """QoS 1 publish (commands to afec/cmd/<tx|rx>). While disconnected paho queues the message
        and sends it after reconnecting (rc NO_CONN); any other error raises."""
        import paho.mqtt.client as mqtt
        info = self.c.publish(topic, payload, qos=1)
        if info.rc not in (mqtt.MQTT_ERR_SUCCESS, mqtt.MQTT_ERR_NO_CONN):
            raise RuntimeError(f"MQTT publish to {topic} failed (rc={info.rc})")

    def close(self) -> None:
        self.closing = True
        self.c.disconnect()
        self.c.loop_stop()
