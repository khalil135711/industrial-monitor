<img width="1912" height="805" alt="Bildschirmfoto 2026-11-12 um 16 16 03" src="https://github.com/user-attachments/assets/691a0a2f-9699-422e-8157-9602eebbb59f" />


# Industrial Monitor

A local production monitor. It watches your machines, PLCs, sensors and cameras on the factory network, shows a live status board, and raises a popup alarm and an entry on the Errors page when something goes wrong. Nothing leaves your network.

## Features
- **Status board:** machines grouped by production line, faults shown first in red.
- **Popup alarm:** a full-screen message with a beep when a new fault appears.
- **Errors page:** time, level, machine, problem and status. Acknowledge faults; they resolve automatically when the machine recovers.
- **Four checks per machine:** responds to ping, a network port is open, a Modbus TCP register stays between a minimum and maximum, or an MQTT topic keeps sending messages with a value in range.
- **False-alarm filter:** a fault is raised only after 2 failed checks in a row (`FAILS_TO_ALARM`).
- **Network scan:** finds devices with industrial ports open: Modbus 502, OPC UA 4840, Siemens S7 102, EtherNet/IP 44818, RTSP 554, MQTT 1883.
- **Saved locally** in `data.json`. Python standard library only.

## MQTT
Add a machine, set **What to check** to *MQTT topic is fresh and in range*, and enter:
- **IP address:** the broker (for example Mosquitto), default port 1883.
- **Topic:** such as `plant/line1/temperature`. Wildcards `+` and `#` work.
- **JSON field (optional):** if the message is JSON like `{"data":{"temp":71}}`, enter `data.temp`.
- **Minimum / maximum:** a fault is raised when the value goes outside this range.
- **No message for N seconds:** a fault is raised when the sensor goes silent.
- **Username / password:** optional.

Details: MQTT 3.1.1, QoS 0 and 1, no TLS (port 8883 is not supported). One login is used per broker, taken from the first machine on that broker. Passwords are stored in plain text in `data.json`; the browser never receives them back.

## Run
```bash
python3 server.py
```
Open http://localhost:8766 (or `http://<this-pc-ip>:8766` on a control-room screen).

## Settings (top of `server.py`)
| Setting | Default | Meaning |
| --- | --- | --- |
| `PORT` | 8766 | Web port |
| `CHECK_EVERY` | 5 | Seconds between checks |
| `FAILS_TO_ALARM` | 2 | Failed checks before an alarm |

## Limits
- Reads one Modbus holding register or one MQTT topic per machine. OPC UA and S7 are detected by port only, not read.
- The popup shows only while the dashboard is open in a browser. Keep it open on a control-room screen.
- No login, no email or SMS alerts yet. Use only on a trusted, isolated network.
- This is a monitoring aid, not a safety system. Never rely on it for emergency stops or personnel safety.

## License
MIT, see [LICENSE](LICENSE).
