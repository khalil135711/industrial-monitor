#!/usr/bin/env python3
"""Local production monitor. Python 3.8+, standard library only.
Run: python3 server.py -> http://localhost:8766
Checks per asset: ping | tcp (port open) | modbus (read one holding register, alarm outside min/max)
| mqtt (subscribe to a topic; alarm if no message arrives in time, or the value is out of range).
"""
import concurrent.futures as cf, ipaddress, json, os, platform, select, socket, struct, subprocess, threading, time, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HERE, "data.json")
PORT = 8766
CHECK_EVERY = 5      # seconds between checks
FAILS_TO_ALARM = 2   # consecutive failures before an error is raised (avoids false alarms)
INDUSTRIAL = {502: ("plc", "Modbus TCP"), 4840: ("plc", "OPC UA"), 102: ("plc", "Siemens S7"),
              44818: ("plc", "EtherNet/IP"), 554: ("camera", "RTSP"), 1883: ("sensor", "MQTT")}
lock = threading.Lock()
S = {"assets": {}, "errors": []}
scan = {"running": False, "found": 0, "error": None}


def save():
    with lock, open(DB, "w") as f:
        json.dump(S, f, indent=1)


def load():
    try:
        S.update(json.load(open(DB)))
    except Exception:
        pass


def ping(ip):
    cmd = ["ping", "-n", "1", "-w", "800", ip] if platform.system() == "Windows" else ["ping", "-c", "1", "-W", "1", ip]
    try:
        return subprocess.run(cmd, capture_output=True, timeout=3).returncode == 0
    except Exception:
        return False


def tcp(ip, port, t=1.0):
    try:
        with socket.create_connection((ip, int(port)), timeout=t):
            return True
    except OSError:
        return False


def modbus_read(ip, port, unit, reg):
    req = struct.pack(">HHHBBHH", 1, 0, 6, unit, 3, reg, 1)
    with socket.create_connection((ip, port), timeout=1.5) as s:
        s.settimeout(1.5)
        s.sendall(req)
        r = s.recv(256)
    if len(r) < 9 or r[7] & 0x80:
        raise IOError("Modbus exception reply")
    return struct.unpack(">H", r[9:11])[0]


# ---------- MQTT 3.1.1: minimal built-in client (receives QoS 0/1, no TLS) ----------
MQ, MQ_T0, BROKERS = {}, {}, {}   # asset_id -> (payload, ts) | asset_id -> first check time | (ip, port) -> status


def _rl(n):
    out = b""
    while True:
        d, n = n % 128, n // 128
        out += bytes([d | (128 if n else 0)])
        if not n:
            return out


def _str(x):
    b = x.encode()
    return struct.pack(">H", len(b)) + b


def _readn(s, n):
    buf = b""
    while len(buf) < n:
        c = s.recv(n - len(buf))
        if not c:
            raise ConnectionError("broker closed the connection")
        buf += c
    return buf


def topic_match(flt, topic):
    f, t = flt.split("/"), topic.split("/")
    for i, p in enumerate(f):
        if p == "#":
            return True
        if i >= len(t) or (p != "+" and p != t[i]):
            return False
    return len(f) == len(t)


def mqtt_assets(ip, port):
    return [a for a in list(S["assets"].values()) if a.get("check", {}).get("kind") == "mqtt"
            and a["ip"] == ip and int(a["check"].get("port") or 1883) == port]


def broker_loop(ip, port):
    st = BROKERS[(ip, port)]
    while True:
        try:
            first = mqtt_assets(ip, port)
            c = first[0]["check"] if first else {}
            user, pw = c.get("user", ""), c.get("pass", "")
            s = socket.create_connection((ip, port), timeout=5)
            s.settimeout(5)
            flags = 0x02 | (0x80 if user else 0) | (0x40 if user and pw else 0)
            body = _str("MQTT") + bytes([4, flags]) + struct.pack(">H", 30) + _str("monitor-" + uuid.uuid4().hex[:6])
            body += (_str(user) if user else b"") + (_str(pw) if user and pw else b"")
            s.sendall(bytes([0x10]) + _rl(len(body)) + body)
            h = _readn(s, 2)
            r = _readn(s, h[1])
            if h[0] >> 4 != 2 or len(r) < 2 or r[1] != 0:
                raise ConnectionError("broker refused the connection (check username and password)")
            st.update(ok=True, err="")
            subbed, last_ping, pid = set(), time.time(), 0
            while True:
                new = {a["check"].get("topic", "") for a in mqtt_assets(ip, port)} - {""} - subbed
                if new:
                    pid += 1
                    body = struct.pack(">H", pid) + b"".join(_str(t) + b"\x00" for t in new)
                    s.sendall(bytes([0x82]) + _rl(len(body)) + body)
                    subbed |= new
                if time.time() - last_ping > 15:
                    s.sendall(b"\xc0\x00")
                    last_ping = time.time()
                if not select.select([s], [], [], 1)[0]:
                    continue
                b0 = _readn(s, 1)[0]
                n, mult = 0, 1
                while True:
                    d = _readn(s, 1)[0]
                    n += (d & 127) * mult
                    mult *= 128
                    if not d & 128:
                        break
                data = _readn(s, n) if n else b""
                if b0 >> 4 == 3:  # PUBLISH
                    tl = struct.unpack(">H", data[:2])[0]
                    topic, i, qos = data[2:2 + tl].decode(errors="replace"), 2 + tl, (b0 >> 1) & 3
                    if qos:
                        if qos == 1:
                            s.sendall(b"\x40\x02" + data[i:i + 2])
                        i += 2
                    payload = data[i:].decode(errors="replace").strip()
                    for a in mqtt_assets(ip, port):
                        if topic_match(a["check"].get("topic", ""), topic):
                            MQ[a["id"]] = (payload, time.time())
        except Exception as e:
            st.update(ok=False, err=str(e) or e.__class__.__name__)
            time.sleep(5)


def mqtt_check(a):
    c = a["check"]
    port, topic = int(c.get("port") or 1883), c.get("topic", "")
    k = (a["ip"], port)
    with lock:
        if k not in BROKERS:
            BROKERS[k] = {"ok": False, "err": "connecting"}
            threading.Thread(target=broker_loop, args=k, daemon=True).start()
    if not topic:
        return False, None, "No MQTT topic configured"
    if not BROKERS[k]["ok"]:
        return False, None, f"MQTT broker problem: {BROKERS[k]['err']}"
    t0 = MQ_T0.setdefault(a["id"], time.time())
    stale = float(c.get("stale") or 60)
    got = MQ.get(a["id"])
    if not got:
        if time.time() - t0 > stale:
            return False, None, f"No message on {topic} for {int(stale)} s"
        return True, None, ""
    raw, ts = got
    if time.time() - ts > stale:
        return False, raw[:30], f"No new message on {topic} for {int(time.time() - ts)} s"
    val = raw
    if c.get("key"):
        try:
            val = json.loads(raw)
            for part in c["key"].split("."):
                val = val[part]
        except Exception:
            return False, raw[:30], f"Field '{c['key']}' not found in the message"
    try:
        v = float(val)
    except (TypeError, ValueError):
        return True, str(val)[:40], ""
    v = int(v) if v == int(v) else round(v, 2)
    lo, hi = c.get("min"), c.get("max")
    if lo not in (None, "") and v < float(lo):
        return False, v, f"Value {v} is below the minimum {lo}"
    if hi not in (None, "") and v > float(hi):
        return False, v, f"Value {v} is above the maximum {hi}"
    return True, v, ""


def run_check(a):
    """Returns (ok, value, problem_text)"""
    c, ip = a.get("check", {"kind": "ping"}), a["ip"]
    k = c.get("kind", "ping")
    try:
        if k == "ping":
            return (True, None, "") if ping(ip) else (False, None, "No reply to ping (machine or network down)")
        if k == "tcp":
            p = c.get("port", 80)
            return (True, None, "") if tcp(ip, p) else (False, None, f"Port {p} not reachable")
        if k == "mqtt":
            return mqtt_check(a)
        if k == "modbus":
            v = modbus_read(ip, int(c.get("port", 502)), int(c.get("unit", 1)), int(c.get("register", 0)))
            lo, hi = c.get("min"), c.get("max")
            if lo not in (None, "") and v < float(lo):
                return False, v, f"Value {v} is below the minimum {lo}"
            if hi not in (None, "") and v > float(hi):
                return False, v, f"Value {v} is above the maximum {hi}"
            return True, v, ""
    except Exception as e:
        return False, None, f"Cannot read device: {e}"
    return True, None, ""


def raise_error(a, msg, sev):
    e = {"id": uuid.uuid4().hex[:8], "ts": int(time.time()), "asset_id": a["id"], "asset": a["name"], "line": a.get("line", ""),
         "severity": sev, "message": msg, "status": "active", "resolved_ts": None}
    S["errors"].insert(0, e)
    del S["errors"][1000:]


def tick(a):
    ok, val, why = run_check(a)
    with lock:
        a["value"] = val
        if ok:
            a["fails"] = 0
            if a.get("state") != "ok":
                a["state"], a["since"] = "ok", int(time.time())
            for e in S["errors"]:  # auto-resolve when it recovers
                if e["asset_id"] == a["id"] and e["status"] != "resolved":
                    e["status"], e["resolved_ts"] = "resolved", int(time.time())
        else:
            a["fails"] = a.get("fails", 0) + 1
            if a["fails"] >= FAILS_TO_ALARM and a.get("state") != "fault":
                a["state"], a["since"] = "fault", int(time.time())
                raise_error(a, why, "critical" if val is None else "warning")
            a["problem"] = why
    return a


def monitor():
    while True:
        with cf.ThreadPoolExecutor(16) as ex:
            list(ex.map(tick, list(S["assets"].values())))
        save()
        time.sleep(CHECK_EVERY)


def run_scan():
    if scan["running"]:
        return
    scan.update(running=True, found=0, error=None)
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))
        net = ipaddress.ip_network(s.getsockname()[0] + "/24", strict=False)
        s.close()

        def probe(ip):
            return ip, [p for p in INDUSTRIAL if tcp(ip, p, 0.5)]

        with cf.ThreadPoolExecutor(64) as ex:
            for ip, ports in ex.map(probe, [str(h) for h in net.hosts()]):
                if not ports or any(x["ip"] == ip for x in S["assets"].values()):
                    continue
                typ, proto = INDUSTRIAL[ports[0]]
                aid = uuid.uuid4().hex[:8]
                with lock:
                    S["assets"][aid] = {"id": aid, "name": f"{proto} device {ip}", "ip": ip, "type": typ, "line": "",
                                        "check": {"kind": "tcp", "port": ports[0]}, "state": "unknown", "fails": 0}
                scan["found"] += 1
        save()
    except Exception as e:
        scan["error"] = str(e)
    finally:
        scan["running"] = False


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, obj, code=200, ct="application/json"):
        b = obj if isinstance(obj, bytes) else json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def body(self):
        return json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")

    def do_GET(self):
        if self.path == "/api/state":
            pub = [dict(a, check={x: y for x, y in a.get("check", {}).items() if x != "pass"}) for a in S["assets"].values()]
            return self.send({"assets": pub, "errors": S["errors"], "scan": scan, "now": int(time.time())})
        if self.path in ("/", "/index.html"):
            return self.send(open(os.path.join(HERE, "index.html"), "rb").read(), ct="text/html; charset=utf-8")
        self.send({"error": "not found"}, 404)

    def do_POST(self):
        p = self.path
        if p == "/api/scan":
            threading.Thread(target=run_scan, daemon=True).start()
            return self.send({"ok": True})
        if p == "/api/assets":
            b = self.body()
            if not b.get("ip"):
                return self.send({"error": "IP address is required"}, 400)
            aid = uuid.uuid4().hex[:8]
            S["assets"][aid] = {"id": aid, "name": b.get("name") or b["ip"], "ip": b["ip"], "type": b.get("type", "other"),
                                "line": b.get("line", ""), "check": b.get("check", {"kind": "ping"}), "state": "unknown", "fails": 0}
            save()
            return self.send(S["assets"][aid])
        if p.startswith("/api/errors/") and p.endswith("/ack"):
            for e in S["errors"]:
                if e["id"] == p.split("/")[3] and e["status"] == "active":
                    e["status"] = "acknowledged"
            save()
            return self.send({"ok": True})
        if p == "/api/errors/clear":
            S["errors"] = [e for e in S["errors"] if e["status"] != "resolved"]
            save()
            return self.send({"ok": True})
        self.send({"error": "not found"}, 404)

    def do_PUT(self):
        a = S["assets"].get(self.path.rsplit("/", 1)[-1])
        if not a:
            return self.send({"error": "not found"}, 404)
        b = self.body()
        old_pw = a.get("check", {}).get("pass", "")
        if b.get("check", {}).get("kind") == "mqtt" and not b["check"].get("pass"):
            b["check"]["pass"] = old_pw
        a.update({k: v for k, v in b.items() if k in ("name", "ip", "type", "line", "check")})
        a["fails"], a["state"] = 0, "unknown"
        save()
        self.send(a)

    def do_DELETE(self):
        S["assets"].pop(self.path.rsplit("/", 1)[-1], None)
        save()
        self.send({"ok": True})


if __name__ == "__main__":
    load()
    threading.Thread(target=monitor, daemon=True).start()
    print(f"Production monitor: http://localhost:{PORT}")
    ThreadingHTTPServer(("0.0.0.0", PORT), H).serve_forever()
