# Deploying ha-lite as a systemd service

The unit assumes:

- Binary: `/opt/ha-lite/ha-lite`
- Configuration: `/etc/ha-lite/ha-lite.env` (systemd `EnvironmentFile`)
- Service user: `ha-lite`, with membership in the `bluetooth` group

## Install

```sh
# Build and install the binary.
cargo build --release
sudo install -d -o root -g root /opt/ha-lite
sudo install -m 0755 target/release/ha-lite /opt/ha-lite/ha-lite

# Create an unprivileged service user. The bluetooth group lets it talk to
# BlueZ over D-Bus; skip it if you only consume BTHome over MQTT.
sudo useradd --system --no-create-home --shell /usr/sbin/nologin ha-lite
sudo usermod -aG bluetooth ha-lite

# Install the environment file. Keep it readable by root only because it
# holds the MQTT password.
sudo install -d -m 0755 /etc/ha-lite
sudo install -m 0600 /dev/null /etc/ha-lite/ha-lite.env
sudoedit /etc/ha-lite/ha-lite.env
```

Example `/etc/ha-lite/ha-lite.env`:

```env
MQTT_HOST=192.168.100.3
MQTT_PORT=1883
MQTT_USER=krr
MQTT_PASSWORD=change-me

HA_LITE_HOST=0.0.0.0
HA_LITE_PORT=8082

# RUST_LOG=info
```

Systemd passes these variables into the process, and they take precedence
over any `.env` file in the working directory.

## Enable

```sh
sudo install -m 0644 deploy/ha-lite.service /etc/systemd/system/ha-lite.service
sudo systemctl daemon-reload
sudo systemctl enable --now ha-lite
systemctl status ha-lite
journalctl -u ha-lite -f
```

Stopping the service sends SIGTERM, which ha-lite handles by aborting the BLE
scanner and releasing its adapter before exiting.
