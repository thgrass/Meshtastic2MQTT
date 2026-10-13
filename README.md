# Meshtastic2MQTT

A small Python script that connects a [Meshtastic](https://meshtastic.org) node over Bluetooth LE and bridges it to a local MQTT broker — so Home Assistant, Node-RED, Grafana or anything else on your LAN can read the mesh and talk back to it.

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

2. Pair it once (this matters — the lib reuses the stored pairing keys):

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
| `meshtastic/chat/dm` | only DMs addressed to your node — handy for push notifications |
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

`get/nodes` is special: it returns the full mesh node list as JSON on `meshtastic/bridge/resp/nodes` — one entry per node with `id`, `longName`, `shortName`, `hwModel`, `batteryLevel`, `lastHeard` (Unix time), `snr` and, if the node broadcasts GPS, `lat`/`lon`/`alt`. That's everything you need to draw a map of your mesh, without ever disconnecting the bridge to run `meshtastic --nodes`.

`get/topology` returns the mesh's link graph as JSON on `meshtastic/bridge/resp/topology`, shaped like `{"<source-id>": [{"to": "<peer-id>", "snr": -8.25}, ...]}`. It fills up from NeighborInfo packets — enable the module on your node (and ideally your neighbors') with `meshtastic --set neighborinfo.enabled true` (stop the bridge first; only one client can use the node at a time). Until nodes have it enabled, the answer stays `{}`.

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

# mesh topology — who hears whom (needs neighborinfo enabled on nodes)
mosquitto_pub -h localhost -t 'meshtastic/bridge/get/topology' -n
mosquitto_sub -h localhost -t 'meshtastic/bridge/resp/topology' -C 1 | jq .

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

- `nodes.json` — every node with `lat`/`lon`/`alt` (if it broadcasts GPS), name, battery, `snr`, `lastHeard`
- `topology.json` — edge list `{"<source-id>": [{"to": "<peer-id>", "snr": -8.25}, ...]}`

Join both on `id` and you have a geographic graph: nodes at their real positions, edges between them, SNR as color or line thickness. Drops straight into Leaflet, D3, Graphviz or Python's `networkx`.

Two caveats:

- Nodes without GPS stay off the map (they're still in the node list, just without coordinates).
- The topology only fills up if nodes broadcast NeighborInfo (see `get/topology` above). Until then the only link data you have is the `snr` values in `nodes.json` — how well *your* node hears each sender.

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
- Only one client can talk to the node at a time — don't run this while also running `meshtastic` CLI against the same node or having your phone connected.

## License

MIT — do whatever you want, no warranty. Tested with firmware 2.7.x and meshtastic-python 2.x; PRs welcome.
