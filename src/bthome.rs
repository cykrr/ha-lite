//! BTHome v2 decoding.
//!
//! BLE advertisements and MQTT proxy payloads both end up here; the decoder
//! knows nothing about either transport.

use std::sync::Arc;

use crate::app::{record_sensor, unix_secs, AppContext};

#[derive(Clone, Copy)]
pub(crate) enum BthomeKind {
    U8,
    I8,
    U16,
    I16,
    U24,
    U32,
    I32,
}

impl BthomeKind {
    fn len(self) -> usize {
        match self {
            BthomeKind::U8 | BthomeKind::I8 => 1,
            BthomeKind::U16 | BthomeKind::I16 => 2,
            BthomeKind::U24 => 3,
            BthomeKind::U32 | BthomeKind::I32 => 4,
        }
    }

    fn read(self, b: &[u8]) -> f64 {
        match self {
            BthomeKind::U8 => b[0] as f64,
            BthomeKind::I8 => b[0] as i8 as f64,
            BthomeKind::U16 => u16::from_le_bytes([b[0], b[1]]) as f64,
            BthomeKind::I16 => i16::from_le_bytes([b[0], b[1]]) as f64,
            BthomeKind::U24 => (b[0] as u32 | (b[1] as u32) << 8 | (b[2] as u32) << 16) as f64,
            BthomeKind::U32 => u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as f64,
            BthomeKind::I32 => i32::from_le_bytes([b[0], b[1], b[2], b[3]]) as f64,
        }
    }
}

pub(crate) struct BthomeObject {
    id: u8,
    name: &'static str,
    kind: BthomeKind,
    factor: f64,
}

pub(crate) struct BthomeReading {
    pub(crate) name: &'static str,
    pub(crate) value: f64,
}

/// BTHome v2 object table (sensor + binary sensor + device info).
/// Variable-length objects (text/raw/events) are intentionally absent; the
/// decoder stops when it hits one, as the spec requires for unknown ids.
const BTHOME_OBJECTS: &[BthomeObject] = &[
    BthomeObject { id: 0x00, name: "packet_id", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x01, name: "battery", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x02, name: "temperature", kind: BthomeKind::I16, factor: 0.01 },
    BthomeObject { id: 0x03, name: "humidity", kind: BthomeKind::U16, factor: 0.01 },
    BthomeObject { id: 0x04, name: "pressure", kind: BthomeKind::U24, factor: 0.01 },
    BthomeObject { id: 0x05, name: "illuminance", kind: BthomeKind::U24, factor: 0.01 },
    BthomeObject { id: 0x06, name: "mass_kg", kind: BthomeKind::U16, factor: 0.01 },
    BthomeObject { id: 0x07, name: "mass_lb", kind: BthomeKind::U16, factor: 0.01 },
    BthomeObject { id: 0x08, name: "dewpoint", kind: BthomeKind::I16, factor: 0.01 },
    BthomeObject { id: 0x09, name: "count", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x0A, name: "energy", kind: BthomeKind::U24, factor: 0.001 },
    BthomeObject { id: 0x0B, name: "power", kind: BthomeKind::U24, factor: 0.01 },
    BthomeObject { id: 0x0C, name: "voltage", kind: BthomeKind::U16, factor: 0.001 },
    BthomeObject { id: 0x0D, name: "pm2_5", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x0E, name: "pm10", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x0F, name: "generic_boolean", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x10, name: "power_state", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x11, name: "opening", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x12, name: "co2", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x13, name: "tvoc", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x14, name: "moisture", kind: BthomeKind::U16, factor: 0.01 },
    BthomeObject { id: 0x15, name: "battery_low", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x16, name: "battery_charging", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x17, name: "carbon_monoxide", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x18, name: "cold", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x19, name: "connectivity", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x1A, name: "door", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x1B, name: "garage_door", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x1C, name: "gas", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x1D, name: "heat", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x1E, name: "light", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x1F, name: "lock", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x20, name: "moisture_state", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x21, name: "motion", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x22, name: "moving", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x23, name: "occupancy", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x24, name: "plug", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x25, name: "presence", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x26, name: "problem", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x27, name: "running", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x28, name: "safety", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x29, name: "smoke", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x2A, name: "sound", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x2B, name: "tamper", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x2C, name: "vibration", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x2D, name: "window", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x2E, name: "humidity", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x2F, name: "moisture", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x3D, name: "count", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x3E, name: "count", kind: BthomeKind::U32, factor: 1.0 },
    BthomeObject { id: 0x3F, name: "rotation", kind: BthomeKind::I16, factor: 0.1 },
    BthomeObject { id: 0x40, name: "distance_mm", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x41, name: "distance_m", kind: BthomeKind::U16, factor: 0.1 },
    BthomeObject { id: 0x42, name: "duration", kind: BthomeKind::U24, factor: 0.001 },
    BthomeObject { id: 0x43, name: "current", kind: BthomeKind::U16, factor: 0.001 },
    BthomeObject { id: 0x44, name: "speed", kind: BthomeKind::U16, factor: 0.01 },
    BthomeObject { id: 0x45, name: "temperature", kind: BthomeKind::I16, factor: 0.1 },
    BthomeObject { id: 0x46, name: "uv_index", kind: BthomeKind::U8, factor: 0.1 },
    BthomeObject { id: 0x47, name: "volume", kind: BthomeKind::U16, factor: 0.1 },
    BthomeObject { id: 0x48, name: "volume", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x49, name: "volume_flow_rate", kind: BthomeKind::U16, factor: 0.001 },
    BthomeObject { id: 0x4A, name: "voltage", kind: BthomeKind::U16, factor: 0.1 },
    BthomeObject { id: 0x4B, name: "gas", kind: BthomeKind::U24, factor: 0.001 },
    BthomeObject { id: 0x4C, name: "gas", kind: BthomeKind::U32, factor: 0.001 },
    BthomeObject { id: 0x4D, name: "energy", kind: BthomeKind::U32, factor: 0.001 },
    BthomeObject { id: 0x4E, name: "volume", kind: BthomeKind::U32, factor: 0.001 },
    BthomeObject { id: 0x4F, name: "water", kind: BthomeKind::U32, factor: 0.001 },
    BthomeObject { id: 0x50, name: "timestamp", kind: BthomeKind::U32, factor: 1.0 },
    BthomeObject { id: 0x51, name: "acceleration", kind: BthomeKind::U16, factor: 0.001 },
    BthomeObject { id: 0x52, name: "gyroscope", kind: BthomeKind::U16, factor: 0.001 },
    BthomeObject { id: 0x55, name: "volume_storage", kind: BthomeKind::U32, factor: 0.001 },
    BthomeObject { id: 0x56, name: "conductivity", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x57, name: "temperature", kind: BthomeKind::I8, factor: 1.0 },
    BthomeObject { id: 0x58, name: "temperature", kind: BthomeKind::I8, factor: 0.35 },
    BthomeObject { id: 0x59, name: "count", kind: BthomeKind::I8, factor: 1.0 },
    BthomeObject { id: 0x5A, name: "count", kind: BthomeKind::I16, factor: 1.0 },
    BthomeObject { id: 0x5B, name: "count", kind: BthomeKind::I32, factor: 1.0 },
    BthomeObject { id: 0x5C, name: "power", kind: BthomeKind::I32, factor: 0.01 },
    BthomeObject { id: 0x5D, name: "current", kind: BthomeKind::I16, factor: 0.001 },
    BthomeObject { id: 0x5E, name: "direction", kind: BthomeKind::U16, factor: 0.01 },
    BthomeObject { id: 0x5F, name: "precipitation", kind: BthomeKind::U16, factor: 0.1 },
    BthomeObject { id: 0x60, name: "channel", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x61, name: "rotational_speed", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0x62, name: "speed", kind: BthomeKind::I32, factor: 0.000001 },
    BthomeObject { id: 0x63, name: "acceleration", kind: BthomeKind::I32, factor: 0.000001 },
    BthomeObject { id: 0x64, name: "light_level", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0x65, name: "settings_revision", kind: BthomeKind::U8, factor: 1.0 },
    BthomeObject { id: 0xF0, name: "device_type_id", kind: BthomeKind::U16, factor: 1.0 },
    BthomeObject { id: 0xF1, name: "firmware_version", kind: BthomeKind::U32, factor: 1.0 },
    BthomeObject { id: 0xF2, name: "firmware_version", kind: BthomeKind::U24, factor: 1.0 },
];

/// Returns `None` when the payload isn't unencrypted BTHome v2.
pub(crate) fn decode_bthome(payload: &[u8]) -> Option<Vec<BthomeReading>> {
    if payload.len() < 2 || (payload[0] & 0xE0) != 0x40 {
        return None;
    }
    if payload[0] & 0x01 != 0 {
        return None; // Encrypted; needs a pre-shared key
    }

    let mut readings = Vec::new();
    let mut i = 1;
    while i < payload.len() {
        let id = payload[i];
        let spec = match BTHOME_OBJECTS.binary_search_by_key(&id, |o| o.id) {
            Ok(index) => &BTHOME_OBJECTS[index],
            Err(_) => {
                tracing::debug!(object_id = %format!("0x{id:02x}"), offset = i, "unknown BTHome object; stopping");
                break;
            }
        };

        let len = spec.kind.len();
        if i + 1 + len > payload.len() {
            tracing::debug!(object_id = %format!("0x{id:02x}"), "truncated BTHome object; stopping");
            break;
        }

        let raw = spec.kind.read(&payload[i + 1..i + 1 + len]);
        readings.push(BthomeReading {
            name: spec.name,
            value: raw * spec.factor,
        });
        i += 1 + len;
    }

    Some(readings)
}

pub(crate) fn summarize_bthome(readings: &[BthomeReading]) -> String {
    readings
        .iter()
        .map(|r| format!("{}={:.3}", r.name, r.value))
        .collect::<Vec<_>>()
        .join(" ")
}

/// First temperature and humidity in a packet. BTHome allows repeated object
/// ids (e.g. internal + external probe); the first one is the primary reading.
pub(crate) fn bthome_temperature_humidity(
    readings: &[BthomeReading],
) -> (Option<f64>, Option<f64>) {
    let (mut temp, mut humidity) = (None, None);
    for reading in readings {
        match reading.name {
            "temperature" if temp.is_none() => temp = Some(round2(reading.value)),
            "humidity" if humidity.is_none() => humidity = Some(round2(reading.value)),
            _ => {}
        }
    }
    (temp, humidity)
}

/// Round to the 0.01 resolution BTHome uses for temperature/humidity, so
/// binary floats don't leak artifacts like 42.830000000000002 into JSON.
fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

pub(crate) async fn record_bthome(
    ctx: &Arc<AppContext>,
    source: &str,
    readings: &[BthomeReading],
) {
    let (t, h) = bthome_temperature_humidity(readings);
    if t.is_none() && h.is_none() {
        return;
    }
    let mut state = ctx.state.write().await;
    record_sensor(&mut state, source, t, h, unix_secs());
}

/// Proxies usually send a hex string, sometimes with a trailing newline; raw
/// BTHome bytes are accepted too. Whitespace is ignored for hex text only, so
/// it can't corrupt a raw packet that happens to contain 0x20/0x0a.
pub(crate) fn bthome_bytes(payload: &[u8]) -> Vec<u8> {
    if let Ok(text) = std::str::from_utf8(payload) {
        let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        if !compact.is_empty() && compact.chars().all(|c| c.is_ascii_hexdigit()) {
            if let Ok(bytes) = hex::decode(&compact) {
                return bytes;
            }
        }
    }
    payload.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(hex_payload: &str) -> Vec<BthomeReading> {
        decode_bthome(&hex::decode(hex_payload).unwrap()).unwrap()
    }

    #[test]
    fn decodes_temperature_humidity_packet() {
        // 00 pid, 01 battery 83, 02 temp 28.32, 03 humidity 40.16
        let readings = decode("400084015302100b03b00f");
        let (t, h) = bthome_temperature_humidity(&readings);
        assert!((t.unwrap() - 28.32).abs() < 0.001);
        assert!((h.unwrap() - 40.16).abs() < 0.001);
    }

    #[test]
    fn decodes_contact_packet() {
        // 00 pid, 0C voltage 2.867V, 10 power off, 11 opening open
        let readings = decode("4000810c330b10001101");
        let summary = summarize_bthome(&readings);
        assert!(summary.contains("voltage=2.867"), "{summary}");
        assert!(summary.contains("power_state=0.000"), "{summary}");
        assert!(summary.contains("opening=1.000"), "{summary}");
        let (t, h) = bthome_temperature_humidity(&readings);
        assert_eq!(t, None);
        assert_eq!(h, None);
    }

    #[test]
    fn rejects_encrypted_packet() {
        assert!(decode_bthome(&hex::decode("4100").unwrap()).is_none());
    }

    #[test]
    fn bthome_object_table_is_sorted_for_binary_search() {
        assert!(
            BTHOME_OBJECTS.windows(2).all(|pair| pair[0].id < pair[1].id),
            "BTHOME_OBJECTS must stay strictly ascending by id"
        );
    }

    #[test]
    fn keeps_first_of_duplicate_temperature_objects() {
        // 00 pid, 02 temp 28.32, 02 temp 20.00, 03 humidity 40.16
        let readings = decode("40008402100b02d00703b00f");
        let (t, h) = bthome_temperature_humidity(&readings);
        assert_eq!(t, Some(28.32));
        assert_eq!(h, Some(40.16));
    }

    #[test]
    fn mqtt_hex_with_trailing_newline_decodes() {
        // A proxy that appends a newline must not be read as ASCII bytes.
        let bytes = bthome_bytes(b"400084015302100b03b00f\r\n");
        assert_eq!(bytes[0], 0x40);

        let readings = decode_bthome(&bytes).unwrap();
        let (t, h) = bthome_temperature_humidity(&readings);
        assert_eq!(t, Some(28.32));
        assert_eq!(h, Some(40.16));
    }

    #[test]
    fn raw_bthome_bytes_are_not_mistaken_for_hex_text() {
        let raw = [0x40, 0x00, 0xcb, 0x0c, 0x2b, 0x0b, 0x10, 0x00, 0x11, 0x01];
        assert_eq!(bthome_bytes(&raw), raw.to_vec());
    }
}
