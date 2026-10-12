#!/usr/bin/env python3
"""
Meshtastic BLE -> MQTT bridge for Raspberry Pi.

Bridges a Meshtastic node (e.g. SenseCAP Solar Node, nRF52840) over
Bluetooth LE to a local MQTT broker (mosquitto), so Home Assistant,
Node-RED or any LAN client can read mesh traffic and send messages
back to the mesh.

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

Send text to the mesh (LAN -> mesh):
  meshtastic/bridge/to/all/text                  -> broadcast on default channel
  meshtastic/bridge/to/<hex-nodeid>/text         -> direct message to one node

Get fields: batteryLevel, voltage, channelUtilization, airUtilTx,
uptimeSeconds, firmwareVersion, longName, shortName

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
"""

import json
import ssl
import time
import logging
import datetime

import paho.mqtt.client as mqtt
from pubsub import pub
from meshtastic.ble_interface import BLEInterface

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
BLE_TARGET = "DE:AD:BE:EF:DE:ED"   # BLE MAC address from `meshtastic --ble-scan`, may work with BLE name

MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_USER = None                 # "user" if mosquitto requires auth
MQTT_PASS = None                 # "pass"
MQTT_TLS = False                 # True if broker requires TLS
TOPIC_PREFIX = "meshtastic/bridge"
NODE_TOPIC_PREFIX = "meshtastic/node"
RECONNECT_DELAY = 5             # seconds between reconnect attempts
HEARTBEAT_INTERVAL = 60         # seconds

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


def on_packet(packet, interface):
    """Main callback for every mesh packet received from the node."""
    try:
        nodeid = node_id_hex(packet)
        decoded = packet.get("decoded", {})
        ptype = decoded.get("portnum", decoded.get("type"))

        # Only publish the raw JSON when there is actual content —
        # routing ACKs etc. carry an empty decoded dict and would
        # otherwise flood the broker with '{}' messages.
        if decoded:
            publish(f"{NODE_TOPIC_PREFIX}/{nodeid}/json", json.dumps(decoded))

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
                    json.dumps(values),
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

        # Text messages + unified chat stream (broadcast vs DM)
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
def on_mqtt_message(client, userdata, msg):
    """Handle requests on meshtastic/bridge/get/... and text messages on
    meshtastic/bridge/to/.../text."""
    try:
        payload = msg.payload.decode("utf-8")
        parts = msg.topic.split("/")

        # Only react to requests and outbound texts; ignore our own
        # publishes (status/heartbeat/resp) that we receive back on
        # the meshtastic/bridge/# subscription.
        if len(parts) < 3 or parts[2] not in ("get", "to"):
            return

        # meshtastic/bridge/get/<field> -> query local node info
        if parts[2] == "get":
            if len(parts) < 4:
                return
            field = parts[3]
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
def run_bridge():
    mqttc.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    mqttc.loop_start()
    mqttc.subscribe(f"{TOPIC_PREFIX}/#", qos=1)

    # Attach the packet callback via pubsub (the lib's actual event
    # mechanism for received packets). Subscribe exactly ONCE, outside
    # the reconnect loop — re-subscribing on every reconnect would
    # stack duplicate listeners and duplicate every MQTT message.
    pub.subscribe(on_packet, "meshtastic.receive")

    while True:
        iface = None
        try:
            log.info("Connecting to BLE device %s ...", BLE_TARGET)
            iface = BLEInterface(BLE_TARGET)
            mqttc.user_data_set({"interface": iface})

            # Publish local node info (firmware, battery, voltage) once
            # connected — this data lives in the node DB, not the
            # packet stream.
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
            log.info("Connected. Bridging BLE <-> MQTT.")

            # Heartbeat so you can monitor if the bridge is alive in HA
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
            log.error("BLE connection error: %s", e)
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
