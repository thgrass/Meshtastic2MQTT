#!/usr/bin/env python3
"""
Meshtastic BLE -> MQTT bridge for Raspberry Pi.

Bridges a Meshtastic node (e.g. SenseCAP Solar Node, nRF52840) over
Bluetooth LE to a local MQTT broker (mosquitto), so Home Assistant or
any LAN client can read mesh traffic and send messages back to the mesh.

Topic layout:
  meshtastic/bridge/status                       -> "online"/"offline" (retained, LWT)
  meshtastic/bridge/heartbeat                    -> ISO timestamp, every 60s
  meshtastic/bridge/get/<field>                  -> request local node info
  meshtastic/bridge/resp/<field>                 -> retained answer to a get
  meshtastic/node/<nodeid>/telemetry/<type>/<k>  -> numeric sensor value (retained)
  meshtastic/node/<nodeid>/telemetry/<type>/json -> full telemetry JSON
  meshtastic/node/<nodeid>/position              -> JSON {lat, lon, alt}
  meshtastic/node/<nodeid>/text                  -> plain text message
  meshtastic/node/<nodeid>/json                  -> raw decoded packet JSON
  meshtastic/chat                                -> every text message as JSON:
       {"fromId", "fromName", "to": "all"|"you", "channel", "text"}
  meshtastic/chat/dm                             -> only DMs addressed to this node
  (Store & Forward replays are unwrapped: history messages that an
  S&F router re-sends wrapped in STORE_FORWARD_APP appear as normal
  entries on chat/chat/dm, attributed to their original senders)

Send text to the mesh (LAN -> mesh):
  meshtastic/bridge/to/all/text                  -> broadcast on default channel
  meshtastic/bridge/to/<hex-nodeid>/text         -> direct message to one node

Forward Home Assistant sensors to the mesh (LAN -> mesh):
  every topic in FORWARD_TOPICS is watched and broadcast as
  EnvironmentTelemetry; one packet per FORWARD_MIN_INTERVAL (15 min)
  carries all current values, and only on changes of at least
  FORWARD_DEADBAND — the mesh sees this node like a sensor node
  (fields: temperature, relative_humidity, barometric_pressure).
  Mapping forms: "topic": "field" for bare-number payloads, or
  "topic": {"json_key": "field", ...} for JSON payloads
  (zigbee2mqtt). Empty FORWARD_TOPICS disables the feature.
  The injected values are ALSO published locally under
  meshtastic/node/<thisnode>/telemetry/environmentMetrics/...
  (receipt: the node cannot hear its own broadcasts, so the
  received-packet stream never shows them).

Get fields: batteryLevel, voltage, channelUtilization, airUtilTx,
uptimeSeconds, firmwareVersion, longName, shortName,
nodes (full node list incl. positions as JSON on .../resp/nodes),
topology (edge list from NeighborInfo packets on .../resp/topology),
get/node/<id>/<field> -> query one remote node from the node DB
  (fields: longName, shortName, hwModel, batteryLevel, voltage,
   channelUtilization, airUtilTx, uptimeSeconds, snr, lastHeard,
   lat, lon, alt, or "all" for the full DB entry as JSON;
   answer retained on .../resp/node/<id>/<field>),
channel (share URL for this node's channel on .../resp/channel —
  this is the exact content a Meshtastic QR code encodes),
channelchallenge (one-time nonce for set/channel, on
  .../resp/channelchallenge, valid 10 minutes),
traceroute: get/traceroute/<hexid>[/<hops 1-7>] -> force a remote
  node to answer with its route through the mesh; verifies two-way
  delivery end to end; result retained on .../resp/traceroute
  (pending -> ok with hops+SNR, or failed/timeout)

Set topic meshtastic/bridge/set/channel:
  Import a channel URL (from a QR code / channel link) into the
  node. REPLACES channels and LoRa settings; the node reboots.
  Payload: JSON {"url": "<full channel URL>", "confirm": "<nonce>"}
  — get the nonce first via get/channelchallenge. Result (retained)
  on .../resp/channel/result. Refuses imports that would change the
  LoRa region (would silently split the mesh).

Requirements on the Pi:
  sudo apt install python3-venv bluetooth bluez mosquitto
  python3 -m venv ~/bridge-venv
  ~/bridge-venv/bin/pip install meshtastic paho-mqtt

Find your node's BLE address:
  ~/bridge-venv/bin/meshtastic --ble-scan

Then edit BLE_TARGET below (BLE MAC address) and, if needed,
MQTT_HOST / MQTT_USER / MQTT_PASS.

Run:  ~/bridge-venv/bin/python3 Meshtastic2MQTT.py
Or install as a service: see meshtastic2mqtt.service

Transport (CLI switches override the TRANSPORT / BLE_TARGET /
SERIAL_PORT config defaults; each switch takes an optional target):
  Meshtastic2MQTT.py                         -> config default (BLE)
  Meshtastic2MQTT.py --ble                   -> Bluetooth, default MAC
  Meshtastic2MQTT.py --ble DE:AD:BE:EF:DE:ED -> Bluetooth, this MAC
  Meshtastic2MQTT.py --serial                -> USB serial, auto-detect
  Meshtastic2MQTT.py --serial /dev/ttyACM0   -> USB serial, this port
"""

import argparse
import base64
import json
import secrets
import signal
import ssl
import sys
import threading
import time
import logging
import datetime

import paho.mqtt.client as mqtt
from pubsub import pub
from meshtastic import BROADCAST_ADDR
from meshtastic.ble_interface import BLEInterface
try:
    from meshtastic.serial_interface import SerialInterface
except ImportError:  # pyserial missing: --serial not available
    SerialInterface = None
from meshtastic.protobuf import portnums_pb2, telemetry_pb2

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
# Transport defaults — CLI switches (--ble/--serial, each with an
# optional MAC or device path) override these at runtime.
TRANSPORT = "ble"                # "ble" | "serial"
BLE_TARGET = "DE:AD:BE:EF:DE:ED" # BLE MAC address from `meshtastic --ble-scan`
SERIAL_PORT = None               # e.g. "/dev/ttyACM0"; None = auto-detect
MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_USER = None                 # or "user" if mosquitto requires auth
MQTT_PASS = None                 # or "pass"
MQTT_TLS = False                 # True if broker requires TLS
TOPIC_PREFIX = "meshtastic/bridge"
NODE_TOPIC_PREFIX = "meshtastic/node"
RECONNECT_DELAY = 5             # seconds between reconnect attempts
HEARTBEAT_INTERVAL = 60         # seconds

# ------- HA sensor / zigbee2mqtt -> mesh telemetry forwarding -------
# The bridge watches every topic in FORWARD_TOPICS and broadcasts the
# values as genuine EnvironmentTelemetry — to the mesh, this node
# then looks like a sensor node. ONE packet per interval carries ALL
# current values, exactly like a real BME280 node sends
# temperature+humidity+pressure together. Etiquette rails: at most
# one broadcast per FORWARD_MIN_INTERVAL seconds, and only when at
# least one value moved >= FORWARD_DEADBAND.
# Two mapping forms per topic:
#   bare-number payload:  "topic": "temperature"
#   JSON payload (zigbee2mqtt!):
#     "zigbee2mqtt/<device>": {"temperature": "temperature",
#                              "humidity": "relative_humidity"}
#   -> JSON keys of the device message mapped to mesh fields; pick
#      any subset, other keys (battery, linkquality, ...) are
#      ignored. Find your topics via
#      `mosquitto_sub -t 'zigbee2mqtt/#' -v` or the Z2M frontend.
# Mesh metric fields (snake_case): temperature, relative_humidity,
#   barometric_pressure. Empty dict disables all forwarding.
FORWARD_TOPICS = {
    # zigbee2mqtt example — pick your device and the keys you need:
    # "zigbee2mqtt/<your-device>": {
    #     "temperature": "temperature",
    #     "humidity": "relative_humidity",
    # },
}
FORWARD_MIN_INTERVAL = 900      # seconds between broadcasts (15 min)
FORWARD_DEADBAND = 0.3          # skip if no value changed this much
FORWARD_STATE = {"values": {}, "sent": {}, "sent_at": 0.0}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("mesh-bridge")

# --------------------------------------------------------------------------
# MQTT client with Last Will (so HA can detect bridge outages)
# --------------------------------------------------------------------------
mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="meshtastic-ble-bridge")
if MQTT_USER:
    mqttc.username_pw_set(MQTT_USER, MQTT_PASS)
if MQTT_TLS:
    mqttc.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)

mqttc.will_set(f"{TOPIC_PREFIX}/status", "offline", qos=1, retain=True)


def node_id_hex(packet):
    """Get a stable hex node id like '9e20d02c' from a packet."""
    frm = packet.get("fromId") or packet.get("from")
    if isinstance(frm, int):
        return f"{frm:08x}"
    if isinstance(frm, str):
        return frm.replace("!", "").replace("0x", "").lower()
    return "unknown"


def publish(topic, payload, retain=False):
    try:
        mqttc.publish(topic, payload, qos=1, retain=retain)
    except Exception as e:
        log.error("MQTT publish failed: %s", e)


def _json_dump(obj):
    """json.dumps that can't fail: protobuf leftovers such as raw
    bytes become hex strings instead of raising TypeError — an
    exception here would silently kill ALL publishing for that
    packet (json + telemetry + chat)."""
    return json.dumps(
        obj,
        default=lambda o: (
            o.hex() if isinstance(o, (bytes, bytearray)) else str(o)
        ),
    )


# --------------------------------------------------------------------------
# Meshtastic packet -> MQTT topics
# --------------------------------------------------------------------------
def _get_firmware(interface):
    """Firmware version lives on the interface's myInfo object, not in
    the node DB. Handle both dict and object shapes across lib versions."""
    try:
        myinfo = interface.myInfo
    except Exception:
        myinfo = None
    if isinstance(myinfo, dict):
        return myinfo.get("firmwareVersion")
    if myinfo is not None:
        return (
            getattr(myinfo, "firmware_version", None)
            or getattr(myinfo, "firmwareVersion", None)
        )
    return None


# NeighborInfo packets (needs the neighborinfo module enabled on the
# nodes) feed a topology map: source node id -> list of edges
MESH_TOPOLOGY = {}

# Pending confirmation nonce for meshtastic/bridge/set/channel.
# Channel imports are powerful: they replace channels AND LoRa
# settings (node reboots) — so they need a two-step confirmation.
CHANNEL_CHALLENGE = {"nonce": None, "expires": 0.0}
CHANNEL_CHALLENGE_TTL = 600  # seconds


def _channelset_region(url):
    """Best-effort: pull the LoRa region out of a channel URL
    (https://meshtastic.org/e/#<base64url ChannelSet>). Returns None
    if the payload can't be parsed — callers treat that as 'unknown'
    and don't block on it."""
    try:
        from meshtastic.protobuf import apponly_pb2
        frag = url.split("#", 1)[-1].split("?", 1)[0].strip()
        pad = "=" * (-len(frag) % 4)
        blob = base64.urlsafe_b64decode(frag + pad)
        cs = apponly_pb2.ChannelSet()
        cs.ParseFromString(blob)
        for attr in ("lora_config", "loRaConfig", "loraConfig"):
            lora = getattr(cs, attr, None)
            if lora is not None and lora.HasField("region"):
                return int(lora.region)
    except Exception:
        pass
    return None


def on_packet(packet, interface):
    """Main callback for every mesh packet received from the node."""
    try:
        nodeid = node_id_hex(packet)
        decoded = packet.get("decoded", {})
        ptype = decoded.get("portnum", decoded.get("type"))

        # One journal line per received packet: makes "is anything
        # arriving at all?" answerable from journalctl alone.
        log.info("RX packet type=%s from=%s", ptype, nodeid)

        # Only publish the raw JSON when there is actual content —
        # routing ACKs etc. carry an empty decoded dict and would
        # otherwise flood the broker with '{}' messages.
        if decoded:
            publish(f"{NODE_TOPIC_PREFIX}/{nodeid}/json", _json_dump(decoded))

        # Telemetry (environment, power, device metrics, air quality...)
        telemetry = decoded.get("telemetry")
        if telemetry:
            for ttype, values in telemetry.items():
                if not isinstance(values, dict):
                    continue
                for key, val in values.items():
                    if isinstance(val, (int, float)):
                        publish(
                            f"{NODE_TOPIC_PREFIX}/{nodeid}/telemetry/{ttype}/{key}",
                            val,
                            retain=True,
                        )
                publish(
                    f"{NODE_TOPIC_PREFIX}/{nodeid}/telemetry/{ttype}/json",
                    _json_dump(values),
                    retain=True,
                )

        # Device metrics (battery/voltage) arrive in nodeinfo packets too
        metrics = decoded.get("deviceMetrics")
        if metrics:
            for key in ("batteryLevel", "voltage", "channelUtilization", "airUtilTx"):
                val = metrics.get(key)
                if isinstance(val, (int, float)):
                    publish(
                        f"{NODE_TOPIC_PREFIX}/{nodeid}/telemetry/device/{key}",
                        val,
                        retain=True,
                    )

        # Position (skip if we don't have both coordinates)
        pos = decoded.get("position")
        if pos:
            payload = {}
            if "latitudeI" in pos and "longitudeI" in pos:
                payload["lat"] = pos["latitudeI"] / 1e7
                payload["lon"] = pos["longitudeI"] / 1e7
                payload["alt"] = pos.get("altitude", 0)
            elif pos.get("latitude") is not None and pos.get("longitude") is not None:
                payload["lat"] = pos["latitude"]
                payload["lon"] = pos["longitude"]
                payload["alt"] = pos.get("altitude", 0)
            if payload:
                publish(f"{NODE_TOPIC_PREFIX}/{nodeid}/position", json.dumps(payload))

        # NeighborInfo packets -> mesh topology edges (for get/topology).
        # Requires the neighborinfo module enabled on the broadcasting
        # nodes: meshtastic --set neighbor_info.enabled true
        neighborinfo = decoded.get("neighborInfo")
        if neighborinfo:
            edges = []
            for nb in neighborinfo.get("neighbors") or []:
                peer = nb.get("nodeId")
                if isinstance(peer, int):
                    edges.append({"to": f"{peer:08x}", "snr": nb.get("snr")})
            if edges:
                MESH_TOPOLOGY[nodeid] = edges

        # Text messages + unified chat stream (broadcast vs DM)
        # Store & Forward replays: over-the-mesh replays arrive
        # wrapped in a STORE_FORWARD_APP packet; the embedded bytes
        # are the original text. The S&F server already rewrites the
        # packet envelope to the ORIGINAL sender, so the chat code
        # below attributes the message correctly. Replays are always
        # addressed to the requester, so _sf_broadcast preserves
        # whether the ORIGINAL message was a broadcast or a DM.
        if ptype == "STORE_FORWARD_APP":
            sf = decoded.get("storeforward") or {}
            if sf.get("rr") in ("ROUTER_TEXT_BROADCAST", "ROUTER_TEXT_DIRECT"):
                embedded = sf.get("text")
                if isinstance(embedded, (bytes, bytearray)) and embedded:
                    decoded["text"] = embedded.decode("utf-8", "replace")
                    decoded["_sf_broadcast"] = (
                        sf.get("rr") == "ROUTER_TEXT_BROADCAST"
                    )
                    log.info(
                        "S&F replay unwrapped (original sender %s)", nodeid
                    )

        text = decoded.get("text")
        if text:
            publish(f"{NODE_TOPIC_PREFIX}/{nodeid}/text", text)

            try:
                my_id = interface.getMyNodeInfo().get("user", {}).get("id") or ""
            except Exception:
                my_id = ""
            to_id = str(packet.get("toId") or "")
            is_dm = (
                bool(my_id)
                and to_id.replace("!", "").lower() == my_id.replace("!", "").lower()
            )
            # S&F replays are always addressed to the requesting node;
            # the wrapper says whether the ORIGINAL was a broadcast.
            if decoded.pop("_sf_broadcast", False):
                is_dm = False
            # Human-readable sender name from the node DB, if known
            from_name = nodeid
            try:
                nodes = interface.nodes or {}
                entry = nodes.get(int(nodeid, 16)) or nodes.get(nodeid) or {}
                from_name = (
                    (entry.get("user") or {}).get("longName")
                    or (entry.get("user") or {}).get("shortName")
                    or nodeid
                )
            except Exception:
                pass
            chat = {
                "fromId": nodeid,
                "fromName": from_name,
                "to": "you" if is_dm else "all",
                "channel": packet.get("channel"),
                "text": text,
            }
            publish("meshtastic/chat", json.dumps(chat))
            if is_dm:
                publish("meshtastic/chat/dm", json.dumps(chat))

    except Exception:
        log.exception("Error handling packet")


# --------------------------------------------------------------------------
# MQTT -> mesh (requests + outbound text messages)
# ---------------------------------------------------------------------------
def _to_camel(field):
    """snake_case -> camelCase, so receipt topics match the keys the
    bridge uses for received telemetry (relative_humidity ->
    relativeHumidity) — automatically, for ANY field mapped in
    FORWARD_TOPICS, without a hardcoded field list."""
    first, *rest = field.split("_")
    return first + "".join(part.capitalize() for part in rest)


def _forward_telemetry(userdata, mapping, topic, payload_str):
    """Broadcast HA / zigbee2mqtt sensor values as EnvironmentTelemetry.

    Two mapping forms per topic: a bare field name (payload must be
    a bare number) or a dict {json_key: mesh_field} (payload is
    JSON — the zigbee2mqtt device message). Throttled
    (FORWARD_MIN_INTERVAL) + deadband (FORWARD_DEADBAND) so the mesh
    sees this node exactly like a real sensor node: one small packet
    every 15 minutes at most, none when nothing changed. All current
    values ride together in that one packet."""
    state = FORWARD_STATE
    # 1) decode the payload into {mesh_field: value} updates
    if isinstance(mapping, str):
        try:
            updates = {mapping: float(payload_str)}
        except (ValueError, TypeError):
            log.warning(
                "Forward: non-numeric payload on %s: %r", topic, payload_str
            )
            return
    else:
        try:
            data = json.loads(payload_str)
        except (ValueError, TypeError):
            log.warning(
                "Forward: non-JSON payload on %s: %r", topic, payload_str
            )
            return
        if not isinstance(data, dict):
            log.warning(
                "Forward: unexpected JSON on %s: %r", topic, payload_str
            )
            return
        updates = {}
        for json_key, mesh_field in mapping.items():
            if isinstance(data.get(json_key), (int, float)):
                updates[mesh_field] = float(data[json_key])
        if not updates:
            return  # e.g. availability message: nothing useful inside
    # 2) cache + throttle/deadband + one combined broadcast
    state["values"].update(updates)
    values = state["values"]
    changed = any(
        f not in state["sent"]
        or abs(v - state["sent"][f]) >= FORWARD_DEADBAND
        for f, v in values.items()
    )
    due = (time.time() - state["sent_at"]) >= FORWARD_MIN_INTERVAL
    if not (changed and due):
        return
    try:
        t = telemetry_pb2.Telemetry()
        t.time = int(time.time())
        for f, v in values.items():
            setattr(t.environment_metrics, f, round(float(v), 2))
        iface = userdata["interface"]
        iface.sendData(t, BROADCAST_ADDR, portnums_pb2.PortNum.TELEMETRY_APP)
        state["sent_at"] = time.time()
        state["sent"] = dict(values)
        log.info(
            "Forwarded telemetry to the mesh: %s",
            ", ".join(f"{f}={v:.2f}" for f, v in values.items()),
        )
        # Local receipt: the node cannot hear its own broadcasts, so
        # the injected values never appear in the received-packet
        # stream. Publish them on this node's own telemetry topics,
        # same shape as received environmentMetrics — so Home
        # Assistant treats them like any other node's sensors and
        # the forwarding is verifiable locally.
        try:
            my = iface.getMyNodeInfo() or {}
            myid = (my.get("user") or {}).get("id", "!unknown").lstrip("!")
            # Receipt keys derive automatically from whatever fields
            # FORWARD_TOPICS mapped — no hardcoded field list.
            receipt = {_to_camel(f): v for f, v in values.items()}
            base = f"{NODE_TOPIC_PREFIX}/{myid}/telemetry/environmentMetrics"
            for k, v in receipt.items():
                publish(f"{base}/{k}", round(float(v), 2), retain=True)
            publish(f"{base}/json", _json_dump(receipt), retain=True)
        except Exception as e:
            log.error("Forward: receipt publish failed: %s", e)
    except Exception as e:
        log.error("Forward: telemetry broadcast failed: %s", e)

def on_mqtt_message(client, userdata, msg):
    """Handle requests on meshtastic/bridge/get/... and text messages on
    meshtastic/bridge/to/.../text."""
    try:
        # HA sensor / zigbee2mqtt -> mesh telemetry forwarding —
        # handled before the bridge/# filter below, because the
        # FORWARD_TOPICS live outside meshtastic/... on the broker.
        mapping = FORWARD_TOPICS.get(msg.topic)
        if mapping is not None:
            _forward_telemetry(
                userdata, mapping, msg.topic, msg.payload.decode("utf-8")
            )
            return

        payload = msg.payload.decode("utf-8")
        parts = msg.topic.split("/")

        # Only react to requests and outbound texts; ignore our own
        # publishes (status/heartbeat/resp) that we receive back on
        # the meshtastic/bridge/# subscription.
        if len(parts) < 3 or parts[2] not in ("get", "to", "set"):
            return

        # meshtastic/bridge/get/<field> -> query local node info
        if parts[2] == "get":
            if len(parts) < 4:
                return
            field = parts[3]
            # meshtastic/bridge/get/nodes -> full node list as JSON
            # (answer on meshtastic/bridge/resp/nodes, retained)
            if field == "nodes":
                nodes = userdata["interface"].nodes or {}
                out = []
                for nid, n in nodes.items():
                    user = n.get("user") or {}
                    metrics = n.get("deviceMetrics") or {}
                    pos = n.get("position") or {}
                    entry = {
                        "id": str(user.get("id") or nid).replace("!", ""),
                        "longName": user.get("longName"),
                        "shortName": user.get("shortName"),
                        "hwModel": user.get("hwModel"),
                        "batteryLevel": metrics.get("batteryLevel"),
                        "lastHeard": n.get("lastHeard"),
                        "snr": n.get("snr"),
                    }
                    lat = pos.get("latitude")
                    if lat is None and pos.get("latitudeI") is not None:
                        lat = pos["latitudeI"] / 1e7
                    lon = pos.get("longitude")
                    if lon is None and pos.get("longitudeI") is not None:
                        lon = pos["longitudeI"] / 1e7
                    if lat is not None and lon is not None:
                        entry["lat"] = lat
                        entry["lon"] = lon
                        if pos.get("altitude") is not None:
                            entry["alt"] = pos.get("altitude")
                    out.append(entry)
                out.sort(key=lambda x: (x.get("longName") or "").lower())
                client.publish(
                    f"{TOPIC_PREFIX}/resp/nodes",
                    json.dumps(out),
                    retain=True,
                )
                return
            # meshtastic/bridge/get/topology -> edge list built from
            # NeighborInfo packets (answer on .../resp/topology)
            if field == "topology":
                client.publish(
                    f"{TOPIC_PREFIX}/resp/topology",
                    json.dumps(MESH_TOPOLOGY),
                    retain=True,
                )
                return
            # meshtastic/bridge/get/node/<id>/<field> -> query a single
            # remote node from the node DB (answer retained on
            # meshtastic/bridge/resp/node/<id>/<field>)
            if field == "node":
                if len(parts) < 6:
                    return
                nodeid = parts[4].replace("!", "").replace("0x", "").lower()
                nfield = parts[5]
                resp_topic = f"{TOPIC_PREFIX}/resp/node/{nodeid}/{nfield}"
                entry = {}
                try:
                    nodes = userdata["interface"].nodes or {}
                    entry = nodes.get(int(nodeid, 16)) or nodes.get(nodeid) or {}
                except Exception:
                    entry = {}
                if not entry:
                    client.publish(resp_topic, "unknown node", retain=True)
                    return
                if nfield == "all":
                    client.publish(resp_topic, _json_dump(entry), retain=True)
                    return
                user = entry.get("user") or {}
                metrics = entry.get("deviceMetrics") or {}
                pos = entry.get("position") or {}
                lat = pos.get("latitude")
                if lat is None and pos.get("latitudeI") is not None:
                    lat = pos["latitudeI"] / 1e7
                lon = pos.get("longitude")
                if lon is None and pos.get("longitudeI") is not None:
                    lon = pos["longitudeI"] / 1e7
                values = {
                    "longName": user.get("longName"),
                    "shortName": user.get("shortName"),
                    "hwModel": user.get("hwModel"),
                    "batteryLevel": metrics.get("batteryLevel"),
                    "voltage": metrics.get("voltage"),
                    "channelUtilization": metrics.get("channelUtilization"),
                    "airUtilTx": metrics.get("airUtilTx"),
                    "uptimeSeconds": metrics.get("uptimeSeconds"),
                    "snr": entry.get("snr"),
                    "lastHeard": entry.get("lastHeard"),
                    "lat": lat,
                    "lon": lon,
                    "alt": pos.get("altitude"),
                }
                client.publish(
                    resp_topic,
                    str(values.get(nfield, "unknown field")),
                    retain=True,
                )
                return
            # meshtastic/bridge/get/channel -> share URL for this
            # node's channel (the content a QR code would encode)
            if field == "channel":
                url = userdata["interface"].localNode.getURL()
                client.publish(
                    f"{TOPIC_PREFIX}/resp/channel", url, retain=True
                )
                return
            # meshtastic/bridge/get/channelchallenge -> one-time nonce
            # for set/channel (valid CHANNEL_CHALLENGE_TTL seconds,
            # NOT retained: read it live or lose it)
            if field == "channelchallenge":
                nonce = secrets.token_hex(3)
                CHANNEL_CHALLENGE["nonce"] = nonce
                CHANNEL_CHALLENGE["expires"] = (
                    time.time() + CHANNEL_CHALLENGE_TTL
                )
                client.publish(
                    f"{TOPIC_PREFIX}/resp/channelchallenge",
                    nonce,
                    retain=False,
                )
                return
            # meshtastic/bridge/get/traceroute/<hexid>[/<hoplimit>]
            # -> forces that node to answer with the route through
            #    the mesh (the MQTT-native way to verify sending:
            #    no human needed, the target node replies on its
            #    own). Result retained on .../resp/traceroute:
            #    pending -> ok (hops + SNR), failed or timeout.
            if field == "traceroute":
                if len(parts) < 5:
                    return
                dest = parts[4].replace("!", "").replace("0x", "").lower()
                hop_limit = 3
                if len(parts) > 5:
                    try:
                        hop_limit = min(7, max(1, int(parts[5])))
                    except ValueError:
                        pass
                iface = userdata["interface"]
                resp_topic = f"{TOPIC_PREFIX}/resp/traceroute"
                state = {"done": False}

                def _finish(body):
                    if not state["done"]:
                        state["done"] = True
                        client.publish(
                            resp_topic, _json_dump(body), retain=True
                        )

                def _on_trace(p):
                    try:
                        decoded = p.get("decoded", {})
                        if decoded.get("portnum") == "ROUTING_APP":
                            err = (decoded.get("routing") or {}).get("errorReason")
                            # Routing ACKs (errorReason empty) arrive BEFORE the
                            # actual RouteDiscovery response - they are not a
                            # verdict. Only a real error (NAK) finishes as "failed".
                            if err and err != "NONE":
                                _finish(
                                    {"result": "failed", "dest": dest, "error": err}
                                )
                            return
                        from meshtastic.protobuf import mesh_pb2
                        from google.protobuf import json_format
                        rd = mesh_pb2.RouteDiscovery()
                        rd.ParseFromString(decoded.get("payload") or b"")
                        rd_dict = json_format.MessageToDict(rd)
                        frm = p.get("from")
                        _finish({
                            "result": "ok",
                            "dest": dest,
                            "from": (
                                f"{frm:08x}" if isinstance(frm, int)
                                else None
                            ),
                            "route": [
                                f"{n:08x}" for n in rd_dict.get("route", [])
                            ],
                            "snrTowards": [
                                s / 4
                                for s in rd_dict.get("snrTowards", [])
                            ],
                            "routeBack": [
                                f"{n:08x}"
                                for n in rd_dict.get("routeBack", [])
                            ],
                            "snrBack": [
                                s / 4 for s in rd_dict.get("snrBack", [])
                            ],
                        })
                    except Exception as e:
                        _finish(
                            {"result": "error", "dest": dest,
                             "error": str(e)}
                        )

                def _timeout():
                    time.sleep(15 * hop_limit + 15)
                    _finish({"result": "timeout", "dest": dest})

                def _send():
                    try:
                        from meshtastic.protobuf import (
                            mesh_pb2,
                            portnums_pb2,
                        )
                        iface.sendData(
                            mesh_pb2.RouteDiscovery(),
                            destinationId=int(dest, 16),
                            portNum=portnums_pb2.PortNum.TRACEROUTE_APP,
                            wantResponse=True,
                            onResponse=_on_trace,
                            hopLimit=hop_limit,
                        )
                    except Exception as e:
                        _finish(
                            {"result": "error", "dest": dest,
                             "error": str(e)}
                        )

                client.publish(
                    resp_topic,
                    _json_dump({"result": "pending", "dest": dest}),
                    retain=True,
                )
                threading.Thread(target=_timeout, daemon=True).start()
                threading.Thread(target=_send, daemon=True).start()
                return
            # meshtastic/bridge/get/ping/<hexid> -> ping one node:
            # a RouteDiscovery with wantResponse=True (the same
            # mechanism the firmware's own ping uses). Answer
            # retained on .../resp/ping: pending -> ok / failed /
            # timeout. hopLimit=1 pings direct neighbors; raise it
            # (max 7) for nodes further away.
            if field == "ping":
                if len(parts) < 5:
                    return
                dest = parts[4].replace("!", "").replace("0x", "").lower()
                iface = userdata["interface"]
                resp_topic = f"{TOPIC_PREFIX}/resp/ping"
                state = {"done": False}

                def _finish(body):
                    if not state["done"]:
                        state["done"] = True
                        client.publish(resp_topic, _json_dump(body), retain=True)

                def _on_pong(p):
                    try:
                        decoded = p.get("decoded", {})
                        if decoded.get("portnum") == "ROUTING_APP":
                            err = (decoded.get("routing") or {}).get("errorReason")
                            # Routing ACKs are not a verdict; only a
                            # real error (NAK) finishes as "failed".
                            if err and err != "NONE":
                                _finish({"result": "failed", "dest": dest, "error": err})
                            return
                        # RouteDiscovery answer = the pong itself
                        frm = p.get("from")
                        _finish({
                            "result": "ok",
                            "dest": dest,
                            "from": f"{frm:08x}" if isinstance(frm, int) else None,
                        })
                    except Exception as e:
                        _finish({"result": "error", "dest": dest, "error": str(e)})

                def _timeout():
                    time.sleep(20)          # 1 hop + margin
                    _finish({"result": "timeout", "dest": dest})

                def _send():
                    try:
                        from meshtastic.protobuf import mesh_pb2, portnums_pb2
                        iface.sendData(
                            mesh_pb2.RouteDiscovery(),
                            destinationId=int(dest, 16),
                            portNum=portnums_pb2.PortNum.TRACEROUTE_APP,
                            wantResponse=True,
                            onResponse=_on_pong,
                            hopLimit=1,       # direct-neighbor ping
                        )
                    except Exception as e:
                        _finish({"result": "error", "dest": dest, "error": str(e)})

                client.publish(resp_topic,
                               _json_dump({"result": "pending", "dest": dest}),
                               retain=True)
                threading.Thread(target=_timeout, daemon=True).start()
                threading.Thread(target=_send, daemon=True).start()
                return
            try:
                my = userdata["interface"].getMyNodeInfo()
                metrics = my.get("deviceMetrics") or {}
                values = {
                    "batteryLevel": metrics.get("batteryLevel"),
                    "voltage": metrics.get("voltage"),
                    "channelUtilization": metrics.get("channelUtilization"),
                    "airUtilTx": metrics.get("airUtilTx"),
                    "uptimeSeconds": metrics.get("uptimeSeconds"),
                    "firmwareVersion": _get_firmware(userdata["interface"]),
                    "longName": my.get("user", {}).get("longName"),
                    "shortName": my.get("user", {}).get("shortName"),
                }
                client.publish(
                    f"{TOPIC_PREFIX}/resp/{field}",
                    str(values.get(field, "unknown field")),
                    retain=True,
                )
            except Exception as e:
                client.publish(f"{TOPIC_PREFIX}/resp/{field}", f"error: {e}")
            return

        # meshtastic/bridge/set/channel -> import a channel URL.
        # Two-step safety: get/channelchallenge first, then publish
        # JSON {"url": ..., "confirm": "<nonce>"} here. The node
        # reboots itself to apply the new channel set; the bridge's
        # reconnect loop handles the BLE dropout automatically.
        if parts[2] == "set" and len(parts) >= 4 and parts[3] == "channel":
            result = f"{TOPIC_PREFIX}/resp/channel/result"
            try:
                body = json.loads(payload)
                url = str(body["url"])
                confirm = str(body.get("confirm", ""))
            except Exception:
                client.publish(
                    result,
                    'invalid payload - expected JSON {"url": ..., "confirm": ...}',
                    retain=True,
                )
                return
            nonce = CHANNEL_CHALLENGE["nonce"]
            if (
                not nonce
                or confirm != nonce
                or time.time() > CHANNEL_CHALLENGE["expires"]
            ):
                client.publish(
                    result,
                    "challenge missing or expired - request get/channelchallenge first",
                    retain=True,
                )
                return
            # Region guard: refuse channel sets that would move the
            # node to another LoRa region (silent mesh split).
            new_region = _channelset_region(url)
            if new_region is not None:
                try:
                    cur_region = int(
                        userdata["interface"].localNode.localConfig.lora.region
                    )
                    if new_region != cur_region:
                        client.publish(
                            result,
                            f"refused: URL uses region {new_region}, "
                            f"node is on {cur_region}",
                            retain=True,
                        )
                        return
                except Exception:
                    pass  # current region unknown - don't block
            try:
                userdata["interface"].localNode.setURL(url)
                client.publish(
                    result,
                    "channel set - node will reboot to apply",
                    retain=True,
                )
                log.info("Channel set imported via set/channel: %s", url)
            except Exception as e:
                client.publish(result, f"error: {e}", retain=True)
            return

        # meshtastic/bridge/to/<nodeid>/text -> send text onto the mesh
        if parts[-1] != "text" or len(parts) < 5:
            return
        target = parts[3]  # "all", "" (broadcast) or hex node id
        interface = userdata.get("interface")
        if interface is None:
            log.warning("No BLE interface available, dropping message")
            return

        if target in ("all", "broadcast", ""):
            interface.sendText(payload)
        else:
            node_num = int(target.replace("!", "").replace("0x", ""), 16)
            interface.sendText(payload, destinationId=node_num)
        log.info("Sent to mesh [%s]: %r", target or "broadcast", payload)
    except Exception:
        log.exception("Error forwarding MQTT message to mesh")


mqttc.on_message = on_mqtt_message


# --------------------------------------------------------------------------
# BLE connect loop with auto-reconnect
# --------------------------------------------------------------------------
# Live reference to the current interface so the SIGTERM handler can
# close it. `systemctl stop` sends SIGTERM; without a handler Python
# is killed instantly with NO GATT disconnect, and the nRF52 then
# sometimes stays "connected" (a connected peripheral stops
# advertising!) until the node is rebooted.
CURRENT_IFACE = {"iface": None}


def _handle_sigterm(signum, frame):
    """Close the BLE link cleanly on stop signals so the node
    resumes advertising immediately instead of hanging."""
    log.info("Signal %s - closing BLE connection cleanly", signum)
    iface = CURRENT_IFACE["iface"]
    if iface is not None:
        try:
            iface.close()  # proper GATT disconnect
        except Exception:
            pass
    try:
        publish(f"{TOPIC_PREFIX}/status", "offline", retain=True)
        mqttc.loop_stop()
    except Exception:
        pass
    sys.exit(0)


def _parse_args():
    """CLI transport switches: --ble [MAC] and --serial [PORT],
    mutually exclusive, each with an optional target. A given target
    overrides BLE_TARGET/SERIAL_PORT; the bare switch means "this
    transport, config default (or auto-detect)". With no switch at
    all, the TRANSPORT config default applies."""
    p = argparse.ArgumentParser(
        description="Meshtastic <-> MQTT bridge "
                    "(Bluetooth or USB serial transport)"
    )
    t = p.add_mutually_exclusive_group()
    t.add_argument(
        "--ble", nargs="?", const="", default=None, metavar="MAC",
        help="Bluetooth transport; optional MAC from "
             "`meshtastic --ble-scan` (default: BLE_TARGET)",
    )
    t.add_argument(
        "--serial", nargs="?", const="", default=None, metavar="PORT",
        help="USB serial transport; optional device path, e.g. "
             "/dev/ttyACM0 (default: SERIAL_PORT, auto-detect "
             "when unset)",
    )
    return p.parse_args()


def run_bridge():
    args = _parse_args()
    if args.ble is not None:
        transport, cli_target = "ble", (args.ble or None)
    elif args.serial is not None:
        transport, cli_target = "serial", (args.serial or None)
    else:
        transport, cli_target = TRANSPORT, None
    if transport == "serial" and SerialInterface is None:
        log.error("--serial requires pyserial (pip install pyserial)")
        sys.exit(2)
    log.info("Transport: %s", transport)
    # systemd stop = SIGTERM: make sure the node link is closed cleanly
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)
    mqttc.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    mqttc.loop_start()
    mqttc.subscribe(f"{TOPIC_PREFIX}/#", qos=1)
    for fwd_topic in FORWARD_TOPICS:
        # HA sensor -> mesh telemetry forwarding (see _forward_telemetry)
        mqttc.subscribe(fwd_topic, qos=1)
    if FORWARD_TOPICS:
        log.info(
            "Telemetry forwarding enabled: watching %s",
            ", ".join(FORWARD_TOPICS),
        )

    # Attach the packet callback via pubsub. Subscribe once, outside
    # the reconnect loop — re-subscribing on every reconnect would
    # stack duplicate listeners and duplicate every MQTT message.
    pub.subscribe(on_packet, "meshtastic.receive")

    while True:
        iface = None
        try:
            if transport == "serial":
                port = (
                    cli_target if cli_target is not None else SERIAL_PORT
                )
                log.info(
                    "Connecting to serial device %s ...",
                    port or "auto-detect",
                )
                iface = SerialInterface(devPath=port)
            else:
                mac = cli_target if cli_target is not None else BLE_TARGET
                log.info(
                    "Connecting to BLE device %s ...", mac or "auto-detect"
                )
                iface = BLEInterface(mac)
            CURRENT_IFACE["iface"] = iface
            mqttc.user_data_set({"interface": iface})

            # Publish local node info (firmware, battery, voltage) once
            # connected — this data lives in the node DB
            try:
                my = iface.getMyNodeInfo()
                user = my.get("user", {})
                myid = user.get("id", "!unknown").lstrip("!")
                metrics = my.get("deviceMetrics") or {}
                for key in ("batteryLevel", "voltage",
                            "channelUtilization", "airUtilTx"):
                    val = metrics.get(key)
                    if isinstance(val, (int, float)):
                        publish(f"{NODE_TOPIC_PREFIX}/{myid}/telemetry/device/{key}",
                                val, retain=True)
                info = {
                    "longName": user.get("longName"),
                    "shortName": user.get("shortName"),
                    "hwModel": user.get("hwModel"),
                }
                fw = _get_firmware(iface)
                if fw:
                    info["firmwareVersion"] = fw
                publish(f"{NODE_TOPIC_PREFIX}/{myid}/info",
                        json.dumps(info), retain=True)
            except Exception as e:
                log.error("publishing local node info failed: %s", e)

            publish(f"{TOPIC_PREFIX}/status", "online", retain=True)
            log.info("Connected. Bridging %s <-> MQTT.", transport.upper())

            # Heartbeat so you can see the bridge is alive
            while True:
                time.sleep(HEARTBEAT_INTERVAL)
                publish(
                    f"{TOPIC_PREFIX}/heartbeat",
                    datetime.datetime.now(datetime.timezone.utc).isoformat(),
                )

        except KeyboardInterrupt:
            log.info("Shutting down.")
            if iface:
                iface.close()
            publish(f"{TOPIC_PREFIX}/status", "offline", retain=True)
            mqttc.loop_stop()
            return

        except Exception as e:
            log.error("Connection error: %s", e)
            CURRENT_IFACE["iface"] = None
            try:
                if iface:
                    iface.close()
            except Exception:
                pass
            mqttc.user_data_set({"interface": None})
            publish(f"{TOPIC_PREFIX}/status", "offline", retain=True)
            log.info("Retrying in %s seconds...", RECONNECT_DELAY)
            time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    run_bridge()
