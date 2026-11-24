# ha-lite

A small Home Assistant MQTT bridge in Rust. It collects BTHome sensor readings
from MQTT (or a local Bluetooth adapter), tracks lights and switches announced
through Home Assistant or Tasmota MQTT discovery, and serves a tiny web UI plus
a JSON API.

The binary is deliberately self-contained: no database, no external services
beyond the broker, and no Home Assistant integration required. Point it at an
MQTT broker and it starts discovering.

## Features

- BTHome v2 decoding over MQTT (`sensors/bthome...`) or local BLE
  advertisements (unencrypted payloads).
- Home Assistant MQTT discovery for `switch` and `light` entities.
- Tasmota discovery (`tasmota/discovery/<MAC>/config`), including multi-relay
  devices.
- Legacy `lights/<id>/state` and `lights/<id>/set` topic convention.
- Optimistic light commands that revert if the MQTT publish fails.
- Reconnects to the broker and to BLE adapters with backoff.
- Small built-in HTML UI and a JSON state endpoint.

## Requirements

- Rust (edition 2021)
- An MQTT broker reachable over plain TCP (the connection is not configured
  for TLS)
- Linux with BlueZ if you want local BLE scanning; MQTT-only operation needs
  no Bluetooth stack
- `dbus`/`libdbus` at build and run time

## Configuration

Settings come from the process environment, optionally loaded from a `.env`
file in the working directory. Real environment variables win over `.env`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `MQTT_HOST` | `127.0.0.1` | MQTT broker host |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_USER` | empty | Broker username; omitted when empty |
| `MQTT_PASSWORD` | empty | Broker password |
| `MQTT_CLIENT_ID` | `ha-lite-<hostname>` | Broker client id; set it when running more than one instance, so they don't evict each other |
| `HA_LITE_HOST` | `0.0.0.0` | HTTP bind address |
| `HA_LITE_PORT` | `8082` | HTTP port |
| `RUST_LOG` | `info` | `tracing` filter, for example `debug` |

Example `.env`:

```env
MQTT_HOST=192.168.100.3
MQTT_PORT=1883
MQTT_USER=krr
MQTT_PASSWORD=change-me
# MQTT_CLIENT_ID=ha-lite-alarm

HA_LITE_HOST=0.0.0.0
HA_LITE_PORT=8082
```

The app binds to all interfaces by default. There is no HTTP authentication, so
run it on a trusted network or put it behind a reverse proxy; use
`HA_LITE_HOST=127.0.0.1` if it should only be reachable locally.

## Running

```sh
cargo build --release
./target/release/ha-lite
```

Open <http://localhost:8082/> for the UI. Logs go to stderr (or the journal when
run as a service). SIGTERM and Ctrl-C shut down gracefully and release any BLE
scan.

## HTTP API

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/` | Minimal web UI |
| `GET` | `/api/state` | Sensors and lights as JSON |
| `POST` | `/api/lights/:id/on` | Turn one light on; idempotent (requires `X-Requested-With: ha-lite`) |
| `POST` | `/api/lights/:id/off` | Turn one light off; idempotent (same header) |
| `POST` | `/api/lights/:id/toggle` | Flip one light; a retry flips it back, so prefer `on`/`off` (same header) |

`GET /api/state` returns:

```json
{
  "sensors": {
    "sensors/bthome1": {
      "temp_c": 21.5,
      "humidity": 40.2,
      "last_seen": 1760000000
    }
  },
  "temp_c": 21.5,
  "humidity": 40.2,
  "lights": {
    "desk": { "name": "Desk", "on": true }
  }
}
```

`sensors` is keyed by MQTT topic or BLE address. The top-level `temp_c` and
`humidity` mirror the most recent report across all sensors.

The `on`, `off` and `toggle` endpoints return `204` on success, `403` when the
`X-Requested-With: ha-lite` header is missing, `404` for an unknown light, and
`502` when the broker is disconnected or the command could not be queued.
Requiring a non-safelisted header forces cross-origin requests through a CORS
preflight, which blocks drive-by LAN form/fetch CSRF.

```sh
curl -X POST -H 'X-Requested-With: ha-lite' \
  http://localhost:8082/api/lights/desk/on
```

## MQTT topics

Subscribed:

| Topic | Purpose |
| --- | --- |
| `sensors/#` | Sensor feed; `sensors/bthome...` payloads are decoded as BTHome v2, either as hex text or raw bytes |
| `homeassistant/#` | Home Assistant discovery configs for `switch` and `light` |
| `tasmota/discovery/#` | Tasmota discovery configs |
| discovered `stat_t` topics | Light/switch state updates |
| `lights/+/state` | Legacy light state, payload exactly `ON` or `OFF` |

Published:

- The discovered entity's `cmd_t` topic when the UI or API switches it.
- `lights/<id>/set` for a light known only through the legacy convention.
- An empty payload to OpenBeken `.../get` topics and Tasmota
  `cmnd/<topic>/STATE` topics to request the initial state.

Removing a retained discovery config (an empty payload) removes the entities it
announced.

## Deployment

- systemd, manual install: [`deploy/README.md`](deploy/README.md)
- Arch Linux package: [`packaging/arch/README.md`](packaging/arch/README.md)

## Development

```sh
cargo test
cargo fmt
cargo clippy --all-targets
```

## License

GPL-2.0-only. See [`LICENSE`](LICENSE).
