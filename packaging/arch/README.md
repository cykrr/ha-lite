# Arch Linux package

`ha-lite-git` builds the current `main` branch with Cargo and installs:

- `/usr/bin/ha-lite`
- `/usr/lib/systemd/system/ha-lite.service`
- `/usr/lib/sysusers.d/ha-lite.conf` (creates the unprivileged `ha-lite`
  user and adds it to the `bluetooth` group)
- `/etc/ha-lite/ha-lite.env` (tracked as a pacman backup file)

## Build and install

```sh
cd packaging/arch
makepkg -si
```

Edit `/etc/ha-lite/ha-lite.env` to point at your MQTT broker, then:

```sh
sudo systemctl enable --now ha-lite
journalctl -u ha-lite -f
```

The package depends on `dbus` for BlueZ communication. Install `bluez` and
enable `bluetooth.service` if you want ha-lite to scan BTHome advertisements
with a local adapter; MQTT-only operation works without it.

To regenerate `.SRCINFO` for an AUR upload:

```sh
makepkg --printsrcinfo > .SRCINFO
```
