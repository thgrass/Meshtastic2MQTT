# Meshtastic2MQTT

A small Python script that connects a [Meshtastic](https://meshtastic.org) node over Bluetooth LE and bridges it to a local MQTT broker - so Home Assistant, Node-RED, Grafana or anything else on your LAN can read the mesh and talk back to it.

Written for a Raspberry Pi and a solar-powered SenseCAP node, but it should work with any Meshtastic device that exposes BLE (T-Beam, RAK, Heltec, etc.).

## What it does

- Bridges every mesh packet to MQTT: telemetry, positions, text messages and node info
- Publishes a clean chat stream that tells broadcasts and DMs apart
- Lets you send text into the mesh via MQTT (broadcast or direct message)
- Answers on-demand queries about the local node (battery, voltage, firmware...)
- Reconnects automatically when BLE drops, and announces itself dead via MQTT Last Will if it can't

## Requirements

- Raspberry Pi (or any Linux box with Bluetooth)
- A Meshtastic node, paired with the Pi over BLE
- Python 3.9+

```bash
sudo apt install python3-venv bluetooth bluez mosquitto mosquitto-clients
python3 -m venv ~/bridge-venv
~/bridge-venv/bin/pip install meshtastic paho-mqtt
```

## Setup

1. Find your node:

   ```bash
   ~/bridge-venv/bin/meshtastic --ble-scan
   ```

2. Pair it once (this matters - the lib reuses the stored pairing keys):

   ```bash
   bluetoothctl
   [bluetooth]# pair DE:AD:BE:EF:DE:ED
   [bluetooth]# trust DE:AD:BE:EF:DE:ED
   [bluetooth]# exit
   ```

3. Put the address in `BLE_TARGET` in `Meshtastic2MQTT.py` (also check `MQTT_HOST` / credentials if your broker isn't plain localhost).

4. Run it:

   ```bash
   ~/bridge-venv/bin/python3 Meshtastic2MQTT.py
   ```

5. Or run it as a service with the included `meshtastic2mqtt.service` systemd unit (edit the paths and user first).

## Topics

**Mesh → MQTT**

| Topic | Content |
|---|---|
| `meshtastic/node/<id>/telemetry/<type>/<key>` | numeric sensor values (retained) |
| `meshtastic/node/<id>/position` | `{lat, lon, alt}` |
| `meshtastic/node/<id>/text` | plain text message from that node |
| `meshtastic/node/<id>/json` | raw decoded packet (only if non-empty) |
| `meshtastic/chat` | every text message: `{"fromId", "fromName", "to": "all"\|"you", "channel", "text"}` |
| `meshtastic/chat/dm` | only DMs addressed to your node - handy for push notifications |
| `meshtastic/bridge/status` | `online` / `offline` (retained, Last Will) |
| `meshtastic/bridge/heartbeat` | ISO timestamp every 60 s |

**MQTT → mesh**

| Topic | Effect |
|---|---|
| `meshtastic/bridge/to/all/text` | broadcast on the default channel |
| `meshtastic/bridge/to/<hex-id>/text` | direct message to one node |

**Request/response**

| Topic | Effect |
|---|---|
| `meshtastic/bridge/get/<field>` | asks the local node; answer arrives retained on `.../resp/<field>` |

Fields: `batteryLevel`, `voltage`, `channelUtilization`, `airUtilTx`, `uptimeSeconds`, `firmwareVersion`, `longName`, `shortName`.

`get/nodes` is special: it returns the full mesh node list as JSON on `meshtastic/bridge/resp/nodes` - one entry per node with `id`, `longName`, `shortName`, `hwModel`, `batteryLevel`, `lastHeard` (Unix time), `snr` and, if the node broadcasts GPS, `lat`/`lon`/`alt`. That's everything you need to draw a map of your mesh, without ever disconnecting the bridge to run `meshtastic --nodes`.

`get/topology` returns the mesh's link graph as JSON on `meshtastic/bridge/resp/topology`, shaped like `{"<source-id>": [{"to": "<peer-id>", "snr": -8.25}, ...]}`. It fills up from NeighborInfo packets — enable the module on your node (and ideally your neighbors') with `meshtastic --set neighbor_info.enabled true` (stop the bridge first; only one client can use the node at a time). Until nodes have it enabled, the answer stays `{}`.

`get/node/<id>/<field>` queries a single remote node from the node DB - e.g. `get/node/1fa0034c/batteryLevel`. Fields: `longName`, `shortName`, `hwModel`, `batteryLevel`, `voltage`, `channelUtilization`, `airUtilTx`, `uptimeSeconds`, `snr`, `lastHeard`, `lat`, `lon`, `alt`, or `all` for the full DB entry as JSON. The answer arrives retained on `meshtastic/bridge/resp/node/<id>/<field>`. Values are as fresh as the last packet your node heard from that node - minutes for chatty neighbors, hours for quiet ones.

`get/channel` publishes the share URL for your node's channel on `meshtastic/bridge/resp/channel` - the exact content a Meshtastic QR code encodes. Render it into a QR with any generator, or open it on a phone with the app installed.

`set/channel` imports one — the reverse direction: publish JSON `{"url": "<channel URL>", "confirm": "<nonce>"}` to `meshtastic/bridge/set/channel`. Get the nonce first via `get/channelchallenge` (delivered on `resp/channelchallenge`, valid 10 min). The result lands retained on `meshtastic/bridge/resp/channel/result`. **This replaces the node's channels and LoRa settings completely** - the node reboots itself, and the bridge's reconnect loop handles the BLE dropout. Two safety rails: the challenge nonce stops a stray MQTT message from reconfiguring your radio, and the bridge refuses imports that would change the LoRa region (which would silently split your mesh).

`get/traceroute/<id>` makes a specific remote node answer with its route through the mesh - the MQTT-native way to verify your own sending works end to end, no human on the other end needed: the target node replies on its own, so a route response proves your TX and RX across the whole path. Result retained on `meshtastic/bridge/resp/traceroute`: `pending` → `ok` (each hop's node ID and SNR) → or `timeout`/`failed` (no working path right now). Optional hop limit 1–7 as a suffix: `get/traceroute/<id>/5`.

**MQTT sensors → mesh telemetry:** the bridge watches every topic in the `FORWARD_TOPICS` mapping and broadcasts the values as genuine `EnvironmentTelemetry` - remote nodes and apps then show your node as a sensor node. One packet every 15 minutes carries *all* current values, exactly like a real BME280 node. Two entry forms: `"topic": "temperature"` for topics whose payload is a bare number, or - the zigbee2mqtt case - `"zigbee2mqtt/<device>": {"temperature": "temperature", "humidity": "relative_humidity"}`, which maps the JSON keys of the device message straight to mesh fields (pick any subset; `battery`/`linkquality` etc. are ignored). Mesh etiquette is built in: at most one packet per 15 minutes and only when some value moved ≥ 0.3 (`FORWARD_MIN_INTERVAL` / `FORWARD_DEADBAND` constants at the top of the script; empty the dict to disable). These values can be changed in the script. The injected values are additionally published locally on `meshtastic/node/<this-node>/telemetry/environmentMetrics/<field>` (retained).

**Store & Forward replays, unwrapped:** a Store & Forward server (a community router, recognizable by its `STORE_FORWARD_APP` heartbeats every 15 min) answers a DM containing the text `SF` by replaying the public broadcast chatter. Over the mesh those replays arrive wrapped in `STORE_FORWARD_APP` packets; the bridge unwraps them, so they appear as normal entries on `meshtastic/chat` (broadcasts) and `meshtastic/chat/dm` (DMs), correctly attributed to their original senders. Note: the server never replays your *own* messages back to you, and only what it heard while it was online.

## Try it

```bash
# watch everything
mosquitto_sub -h localhost -t 'meshtastic/#' -v

# just the chat
mosquitto_sub -h localhost -t 'meshtastic/chat' -v

# ask the node for its battery, get the answer on .../resp/batteryLevel
mosquitto_pub -h localhost -t 'meshtastic/bridge/get/batteryLevel' -n

# list all nodes in the mesh
mosquitto_pub -h localhost -t 'meshtastic/bridge/get/nodes' -n
mosquitto_sub -h localhost -t 'meshtastic/bridge/resp/nodes' -C 1 | jq .

# ...or find one by name
mosquitto_sub -h localhost -t 'meshtastic/bridge/resp/nodes' -C 1 | \
  jq -r '.[] | select(.longName | test("Solartastic"; "i"))'

# positions for mapping (id, name, lat, lon)
mosquitto_sub -h localhost -t 'meshtastic/bridge/resp/nodes' -C 1 | \
  jq -r '.[] | select(.lat) | "\(.id) \(.longName) \(.lat),\(.lon)"'

# mesh topology — who hears whom (needs the neighbor_info module enabled on nodes)
mosquitto_pub -h localhost -t 'meshtastic/bridge/get/topology' -n
mosquitto_sub -h localhost -t 'meshtastic/bridge/resp/topology' -C 1 | jq .

# battery of one specific remote node
mosquitto_pub -h localhost -t 'meshtastic/bridge/get/node/1fa0034c/batteryLevel' -n
mosquitto_sub -h localhost -t 'meshtastic/bridge/resp/node/1fa0034c/batteryLevel' -C 1

# share this node's channel (URL = QR code content)
mosquitto_pub -h localhost -t 'meshtastic/bridge/get/channel' -n
mosquitto_sub -h localhost -t 'meshtastic/bridge/resp/channel' -C 1

# import a channel URL (two-step: nonce first, then the import)
mosquitto_sub -h localhost -t 'meshtastic/bridge/resp/channelchallenge' -C 1 &
mosquitto_pub -h localhost -t 'meshtastic/bridge/get/channelchallenge' -n
mosquitto_pub -h localhost -t 'meshtastic/bridge/set/channel' \
  -m '{"url": "https://meshtastic.org/e/#GAMi...", "confirm": "<nonce>"}'
mosquitto_sub -h localhost -t 'meshtastic/bridge/resp/channel/result' -C 1

# verify two-way delivery to a specific node (node answers by itself)
mosquitto_pub -h localhost -t 'meshtastic/bridge/get/traceroute/aa98a368' -n
sleep 30
mosquitto_sub -h localhost -t 'meshtastic/bridge/resp/traceroute' -C 1 | jq .

# forward a test measurement (first value broadcasts immediately)
mosquitto_pub -h localhost -t 'system/sht' -m '{"Temp":21.7,"Humid":58}'

# say hello to everyone
mosquitto_pub -h localhost -t 'meshtastic/bridge/to/all/text' -m 'hello mesh'
```

## Drawing a map or graph of your mesh

Two requests give you everything a visualization needs:

```bash
mosquitto_pub -h localhost -t 'meshtastic/bridge/get/nodes' -n
mosquitto_sub -h localhost -t 'meshtastic/bridge/resp/nodes' -C 1 > nodes.json

mosquitto_pub -h localhost -t 'meshtastic/bridge/get/topology' -n
mosquitto_sub -h localhost -t 'meshtastic/bridge/resp/topology' -C 1 > topology.json
```

- `nodes.json` - every node with `lat`/`lon`/`alt` (if it broadcasts GPS), name, battery, `snr`, `lastHeard`
- `topology.json` - edge list `{"<source-id>": [{"to": "<peer-id>", "snr": -8.25}, ...]}`

Join both on `id` and you have a geographic graph: nodes at their real positions, edges between them, SNR as color or line thickness. Drops straight into Leaflet, D3, Graphviz or Python's `networkx`.

Two caveats:

- Nodes without GPS stay off the map (they're still in the node list, just without coordinates).
- The topology only fills up if nodes broadcast NeighborInfo (see `get/topology` above). Until then the only link data you have is the `snr` values in `nodes.json` - how well *your* node hears each sender.

Since both `resp` topics are retained, a cron job that re-issues the two gets and stores the answers gives you a history of your mesh's shape for free.

## Home Assistant

Add the topics you care about to `configuration.yaml`, e.g.:

```yaml
mqtt:
  sensor:
    - name: "Mesh node battery"
      state_topic: "meshtastic/node/991ad4fa/telemetry/device/batteryLevel"
      unit_of_measurement: "%"
      device_class: battery
    - name: "Mesh chat"
      state_topic: "meshtastic/chat"
      value_template: "{{ value_json.text }}"
```

Wire `meshtastic/chat/dm` to a notification automation if you want a ping when someone messages you directly.

## Notes

- Battery and friends refresh when the node broadcasts nodeinfo (every ~3 h by default). For hourly updates, enable device telemetry on the node (`telemetry.device_telemetry_enabled`) and set `device_update_interval`.
- Meshtastic is best-effort: broadcasts aren't guaranteed to reach every node, and this bridge can't change that.
- Only one client can talk to the node at a time - don't run this while also running `meshtastic` CLI against the same node or having your phone connected.

## License

MIT - do whatever you want, no warranty. Tested with firmware 2.7.x and meshtastic-python 2.x; PRs welcome.
