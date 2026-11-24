#!/usr/bin/env python3
"""Read FYTA plant sensors once and push their values as Prometheus metrics (remote_write), then exit.

FYTA (https://fyta.de) sells plant sensors that upload to a cloud. This tool logs in to that cloud,
reads the latest values of one or more plants, and pushes them to a Prometheus remote_write receiver,
so Grafana can graph them and alert when the soil is dry. Run it from cron, from a Kubernetes
CronJob, or by hand. Exit 0 means every plant was read and the series reached the receiver. Any other
outcome is non-zero, so a failed run is a failed Job.

Standard library only, on purpose: the container image needs no pip packages, and a package outage
cannot stop a read. The remote_write body is built here by hand (a minimal protobuf encoder and a
snappy encoder that writes literal-only blocks, which every snappy decoder accepts).

Configuration, environment only (nothing else is read):
  FYTA_EMAIL, FYTA_PASSWORD    the FYTA account login
  FYTA_TOKEN                   an existing bearer token; skips the login (handy for local tests)
  FYTA_PLANT_IDS               comma-separated plant ids; unset = every plant of the account
  METRICS_URL                  remote_write endpoint; required unless DRY_RUN is on
  METRICS_USER, METRICS_PASSWORD   optional basic auth; refused over plain http except to localhost
  INSTANCE_LABEL               value of the `instance` label (default "fyta")
  JOB_LABEL                    value of the `job` label (default "fyta")
  DRY_RUN=1                    build the payload, decode it back, print the series, send nothing
  FYTA_BASE_URL                API base, default https://web.fyta.de/api (tests and proxies only)

Exit codes: 0 pushed, 1 FYTA or receiver failed (or no readings came back), 2 bad configuration.

Pushed series carry the push time as their timestamp and Prometheus writes no stale marker for a
pushed series, so a dashboard or alert must read `last_over_time(metric[30m])`, not the bare name.
Credentials and the FYTA token are never printed or logged, not even in part.

This is an unofficial client of FYTA's public API. Their terms and limits apply.
"""
import base64
import json
import math
import os
import re
import struct
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

DEFAULT_BASE_URL = "https://web.fyta.de/api"
DEFAULT_LABEL = "fyta"
HTTP_TIMEOUT = 20
USER_AGENT = "fyta-exporter"

# API measure name -> the key FYTA uses in `GET user-plant/<id>` under `measurements`.
MEASURES = ("moisture", "temperature", "light", "salinity")
# Row key in `POST user-plant/measurements/<id>` -> metric name.
ROW_METRICS = {
    "soil_moisture": "fyta_soil_moisture_percent",
    "temperature": "fyta_temperature_celsius",
    "light": "fyta_light_umol",
    "soil_fertility": "fyta_soil_ec_ms_per_cm",
}

METRIC_NAME = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
LABEL_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
PLANT_ID = re.compile(r"^[0-9]{1,18}$")
LABEL_LIMIT = 64


class FytaError(Exception):
    """A FYTA call failed. The message never carries a credential."""


class AuthError(FytaError):
    """FYTA refused the login or the token; asking again for another plant will not help."""


class PushError(Exception):
    """The receiver did not take the series. The message never carries a credential."""


class ConfigError(Exception):
    """The environment is wrong. The message never carries a credential."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never follow a redirect: it would re-send the Authorization header somewhere else."""

    def redirect_request(self, *args, **kwargs):
        return None


OPENER = urllib.request.build_opener(_NoRedirect)


# ---------------------------------------------------------------- FYTA

class FytaClient:
    def __init__(self, email=None, password=None, base_url=DEFAULT_BASE_URL, urlopen=OPENER.open,
                 token=None):
        self._email = email
        self._password = password
        self._base = base_url.rstrip("/")
        self._urlopen = urlopen
        self._token = token  # in memory only, never printed

    def _send(self, method, path, body=None, auth=True):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if auth:
            headers["Authorization"] = "Bearer " + str(self._token)
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self._base + "/" + path, data=data, method=method, headers=headers)
        with self._urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return json.load(resp)

    def login(self):
        if not self._email or not self._password:
            raise AuthError("the token was rejected and no login is configured")
        try:
            body = self._send("POST", "auth/login", {"email": self._email, "password": self._password},
                              auth=False)
        except urllib.error.HTTPError as exc:
            exc.close()
            raise AuthError("login rejected: HTTP %d" % exc.code) from None
        token = body.get("access_token") if isinstance(body, dict) else None
        if not token:
            raise AuthError("login answered without an access_token")
        self._token = token

    def call(self, method, path, body=None):
        """Call the API; log in first when there is no token, and once more on HTTP 401."""
        if self._token is None:
            self.login()
        try:
            return self._send(method, path, body)
        except urllib.error.HTTPError as exc:
            exc.close()
            if exc.code != 401:
                raise FytaError("%s %s: HTTP %d" % (method, path, exc.code)) from None
        self._token = None
        self.login()
        try:
            return self._send(method, path, body)
        except urllib.error.HTTPError as exc:
            exc.close()
            raise AuthError("%s %s: HTTP %d after a new login" % (method, path, exc.code)) from None


def num(value):
    """FYTA sends some numbers as strings ("27"); None, booleans, junk and NaN stay None."""
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def epoch(stamp):
    """'2026-11-24 13:22:58' (UTC) -> unix seconds; None when it does not parse."""
    try:
        return datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
    except (TypeError, ValueError):
        return None


def clean_label(value, limit=LABEL_LIMIT):
    """A label value from outside: control characters out, whitespace collapsed, length capped."""
    text = "".join(ch if ch.isprintable() else " " for ch in str(value))
    return " ".join(text.split())[:limit].strip()


def discover(client):
    """Every plant of the account that has a sensor, from `GET user-plant` -> [(id, nickname), ...]."""
    data = client.call("GET", "user-plant")
    found, seen = [], set()
    for plant in (data.get("plants") or []) if isinstance(data, dict) else []:
        if not isinstance(plant, dict):
            continue
        pid = plant.get("id")
        if isinstance(pid, bool) or not isinstance(pid, (int, str)) or not PLANT_ID.match(str(pid)):
            continue
        sensor = plant.get("sensor")
        if plant.get("sensors") == [] or (isinstance(sensor, dict) and sensor.get("has_sensor") is False):
            continue  # a plant without a sensor has no readings
        if str(pid) not in seen:
            seen.add(str(pid))
            found.append((str(pid), clean_label(plant.get("nickname") or "")))
    return found


def parse_measurements(data, now):
    """Latest reading per measure, the newest row time, and the DLI of the last full day.

    Rows come newest first, but the newest is picked by time, not by position. A measure that is
    null in every row (air_humidity on a Beam sensor) is left out, never reported as 0.
    """
    rows = []
    for row in (data.get("measurements") or []) if isinstance(data, dict) else []:
        if not isinstance(row, dict):
            continue
        ts = epoch(row.get("date_utc"))
        if ts is not None:
            rows.append((ts, row))
    rows.sort(key=lambda item: item[0], reverse=True)

    out = {"values": {}, "last_data": rows[0][0] if rows else None, "dli": None, "dli_ts": None}
    for key, metric in ROW_METRICS.items():
        for _ts, row in rows:
            value = num(row.get(key))
            if value is not None:
                out["values"][metric] = value
                break

    today = datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%d")
    full_days = []
    for item in (data.get("dli_light") or []) if isinstance(data, dict) else []:
        if not isinstance(item, dict):
            continue
        ts = epoch(item.get("date_utc"))
        value = num(item.get("dli_light"))
        if ts is not None and value is not None and item["date_utc"][:10] < today:
            full_days.append((ts, value))
    if full_days:
        out["dli_ts"], out["dli"] = max(full_days)
    return out


def parse_plant(data):
    """Nickname, battery, per-measure status (2 low, 3 good) and good range, from `GET user-plant/<id>`."""
    plant = (data.get("plant") if isinstance(data, dict) else None) or {}
    out = {"nickname": clean_label(plant.get("nickname") or ""), "battery": None, "status": {},
           "good_min": {}, "good_max": {}, "last_data": None}
    sensors = plant.get("sensors") or []
    if sensors and isinstance(sensors[0], dict):
        out["battery"] = num(sensors[0].get("battery_level"))
        out["last_data"] = epoch(sensors[0].get("last_data_received_at"))
    measures = plant.get("measurements") or {}
    if out["battery"] is None:
        out["battery"] = num(measures.get("battery"))
    for name in MEASURES:
        block = measures.get(name) or {}
        status = num(block.get("status"))
        if status is not None:
            out["status"][name] = status
        values = block.get("values") or {}
        for src, dst in (("min_good", "good_min"), ("max_good", "good_max")):
            value = num(values.get(src))
            if value is not None:
                out[dst][name] = value
    return out


def collect(client, plant_id, now):
    """Two FYTA calls -> (readings, plant info). Raises FytaError when there is nothing to report."""
    plant = client.call("GET", "user-plant/" + plant_id)
    week = client.call("POST", "user-plant/measurements/" + plant_id, {"search": {"timeline": "week"}})
    readings = parse_measurements(week, now)
    info = parse_plant(plant)
    if not readings["values"]:
        raise FytaError("plant %s: FYTA returned no readings" % plant_id)
    if readings["last_data"] is None:
        readings["last_data"] = info["last_data"]
    return readings, info


# ---------------------------------------------------------------- metrics

def plant_labels(base, plant_id, nickname=""):
    labels = dict(base, plant_id=str(plant_id))
    if nickname:
        labels["plant"] = nickname
    return labels


def build_series(readings, info, labels, now):
    """The metrics of one plant as a list of (labels dict including __name__, value). Gaps are skipped."""
    series = []

    def add(name, value, **extra):
        if value is not None and math.isfinite(value):
            series.append((dict(labels, __name__=name, **extra), float(value)))

    for metric in ROW_METRICS.values():
        add(metric, readings["values"].get(metric))
    add("fyta_battery_percent", info.get("battery"))
    add("fyta_dli_mol_per_day", readings.get("dli"))
    add("fyta_dli_timestamp_seconds", readings.get("dli_ts"))
    last_data = readings.get("last_data")
    add("fyta_last_data_timestamp_seconds", last_data)
    # Age at push time. A pushed series does not age by itself: graph time() - last_data_timestamp.
    add("fyta_last_data_age_seconds", None if last_data is None else round(now - last_data, 1))
    for name, value in sorted(info.get("status", {}).items()):
        add("fyta_measure_status", value, measure=name)
    for name, value in sorted(info.get("good_min", {}).items()):
        add("fyta_measure_good_min", value, measure=name)
    for name, value in sorted(info.get("good_max", {}).items()):
        add("fyta_measure_good_max", value, measure=name)
    add("fyta_scrape_success", 1)
    add("fyta_last_success_timestamp_seconds", round(now, 3))
    return series


def failure_series(labels):
    """What is pushed for a plant FYTA failed on: only the success flag, 0."""
    return [(dict(labels, __name__="fyta_scrape_success"), 0.0)]


# ---------------------------------------------------------------- remote_write encoding

def uvarint(value):
    """Protobuf / snappy unsigned varint."""
    if value < 0:
        raise ValueError("uvarint needs a non-negative number")
    out = bytearray()
    while value > 0x7F:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _len_field(number, payload):
    return uvarint((number << 3) | 2) + uvarint(len(payload)) + payload


def encode_write_request(series, timestamp_ms):
    """WriteRequest{timeseries:[TimeSeries{labels:[Label{name,value}], samples:[Sample{value,timestamp}]}]}.

    Field numbers: WriteRequest.timeseries=1; TimeSeries.labels=1, samples=2; Label.name=1, value=2;
    Sample.value=1 (double, wire type 1), timestamp=2 (int64 varint). Labels are sorted by name.
    """
    body = bytearray()
    for labels, value in series:
        for name, text in labels.items():
            if name == "__name__":
                if not METRIC_NAME.match(str(text)):
                    raise ValueError("invalid metric name %r" % (text,))
            elif not LABEL_NAME.match(name):
                raise ValueError("invalid label name %r" % (name,))
        ts = bytearray()
        for name in sorted(labels):
            label = _len_field(1, name.encode()) + _len_field(2, str(labels[name]).encode())
            ts += _len_field(1, label)
        sample = b"\x09" + struct.pack("<d", value) + b"\x10" + uvarint(timestamp_ms)
        ts += _len_field(2, sample)
        body += _len_field(1, bytes(ts))
    return bytes(body)


def snappy_encode(data):
    """Snappy block format, literal elements only: valid for every decoder, no compression.

    A block is the uncompressed length as a varint, then elements. A literal element is a tag byte
    (low bits 00, upper six bits = length-1 when that is under 60), or tag 60..63 followed by 1..4
    little-endian length-1 bytes. Literals are cut at 65536 bytes, the size every encoder stays under.
    """
    out = bytearray(uvarint(len(data)))
    for start in range(0, len(data), 65536):
        chunk = data[start:start + 65536]
        length = len(chunk) - 1
        if length < 60:
            out.append(length << 2)
        else:
            extra = (length.bit_length() + 7) // 8  # 1..4 length bytes follow the tag
            out.append((59 + extra) << 2)
            out += length.to_bytes(extra, "little")
        out += chunk
    return bytes(out)


def _read_uvarint(data, pos):
    shift = result = 0
    while True:
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if byte < 0x80:
            return result, pos
        shift += 7


def snappy_decode(data):
    """Inverse of snappy_encode (literal elements only); anything else is an error."""
    length, pos = _read_uvarint(data, 0)
    out = bytearray()
    while pos < len(data):
        tag = data[pos]
        pos += 1
        if tag & 3:
            raise ValueError("snappy copy element: not written by snappy_encode")
        size = tag >> 2
        if size >= 60:
            extra = size - 59
            size = int.from_bytes(data[pos:pos + extra], "little")
            pos += extra
        size += 1
        if pos + size > len(data):
            raise ValueError("snappy literal runs past the end")
        out += data[pos:pos + size]
        pos += size
    if len(out) != length:
        raise ValueError("snappy length does not match")
    return bytes(out)


def _fields(data):
    pos = 0
    while pos < len(data):
        key, pos = _read_uvarint(data, pos)
        number, wire = key >> 3, key & 7
        if wire == 2:
            size, pos = _read_uvarint(data, pos)
            yield number, data[pos:pos + size]
            pos += size
        elif wire == 1:
            yield number, data[pos:pos + 8]
            pos += 8
        elif wire == 0:
            value, pos = _read_uvarint(data, pos)
            yield number, value
        else:
            raise ValueError("unexpected wire type %d" % wire)


def decode_write_request(data):
    """-> [(labels dict, [(value, timestamp_ms), ...]), ...]; used by DRY_RUN to show what is sent."""
    out = []
    for number, payload in _fields(data):
        if number != 1:
            continue
        labels, samples = {}, []
        for field, content in _fields(payload):
            if field == 1:
                parts = dict(_fields(content))
                labels[parts[1].decode()] = parts.get(2, b"").decode()
            elif field == 2:
                parts = dict(_fields(content))
                samples.append((struct.unpack("<d", parts[1])[0], parts.get(2, 0)))
        out.append((labels, samples))
    return out


# ---------------------------------------------------------------- push

def _is_local(host):
    return host in ("localhost", "127.0.0.1", "::1")


def push(url, body, user, password, urlopen=OPENER.open):
    """POST one snappy-compressed WriteRequest. Basic auth goes only over https or to localhost."""
    parts = urllib.parse.urlsplit(url)
    headers = {
        "Content-Encoding": "snappy",
        "Content-Type": "application/x-protobuf",
        "X-Prometheus-Remote-Write-Version": "0.1.0",
        "User-Agent": USER_AGENT,
    }
    if user or password:
        if parts.scheme != "https" and not _is_local(parts.hostname or ""):
            raise PushError("refusing to send a password over plain http outside this machine")
        token = base64.b64encode(("%s:%s" % (user or "", password or "")).encode()).decode()
        headers["Authorization"] = "Basic " + token
    req = urllib.request.Request(url, data=body, method="POST", headers=headers)
    try:
        with urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            status = getattr(resp, "status", 200)
    except urllib.error.HTTPError as exc:
        exc.close()
        raise PushError("receiver answered HTTP %d" % exc.code) from None
    except (urllib.error.URLError, OSError) as exc:
        raise PushError("receiver unreachable: " + type(exc).__name__) from None
    if not 200 <= status < 300:
        raise PushError("receiver answered HTTP %d" % status)


def payload(series, now):
    return snappy_encode(encode_write_request(series, int(now * 1000)))


def _escape(text):
    return text.replace("\\", "\\\\").replace('"', '\\"')


def show(body):
    """Decode a payload back and format it, one line per series."""
    lines = []
    for labels, samples in decode_write_request(snappy_decode(body)):
        name = labels.get("__name__", "")
        rest = ",".join('%s="%s"' % (k, _escape(v)) for k, v in sorted(labels.items()) if k != "__name__")
        for value, ts in samples:
            lines.append("%s{%s} %r @%d" % (name, rest, value, ts))
    return lines


# ---------------------------------------------------------------- configuration and main

def truthy(text):
    return str(text or "").strip().lower() in ("1", "true", "yes", "on")


def _check_url(name, url, allow_plain_local):
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ConfigError("%s must be an http(s) URL" % name)
    if parts.username or parts.password:
        raise ConfigError("%s must not carry a login; use the separate variables" % name)
    if allow_plain_local and parts.scheme != "https" and not _is_local(parts.hostname):
        raise ConfigError("%s must be https (plain http only to localhost)" % name)
    return parts


def load_config(env):
    """Validate the environment. Raises ConfigError; the message never carries a credential."""
    cfg = {
        "dry": truthy(env.get("DRY_RUN")),
        "url": (env.get("METRICS_URL") or "").strip(),
        "user": env.get("METRICS_USER") or None,
        "password": env.get("METRICS_PASSWORD") or None,
        "base_url": (env.get("FYTA_BASE_URL") or DEFAULT_BASE_URL).strip(),
        "token": (env.get("FYTA_TOKEN") or "").strip() or None,
        "email": env.get("FYTA_EMAIL") or None,
        "fyta_password": env.get("FYTA_PASSWORD") or None,
        "job": clean_label(env.get("JOB_LABEL") or DEFAULT_LABEL) or DEFAULT_LABEL,
        "instance": clean_label(env.get("INSTANCE_LABEL") or DEFAULT_LABEL) or DEFAULT_LABEL,
        "plant_ids": [],
    }
    _check_url("FYTA_BASE_URL", cfg["base_url"], True)
    if cfg["url"]:
        _check_url("METRICS_URL", cfg["url"], False)
    elif not cfg["dry"]:
        raise ConfigError("METRICS_URL must be set (or DRY_RUN=1)")
    if cfg["url"] and not cfg["dry"] and (cfg["user"] or cfg["password"]):
        parts = urllib.parse.urlsplit(cfg["url"])
        if parts.scheme != "https" and not _is_local(parts.hostname):
            raise ConfigError("refusing to send METRICS_PASSWORD over plain http outside this machine")
    if not cfg["token"] and not (cfg["email"] and cfg["fyta_password"]):
        raise ConfigError("set FYTA_EMAIL and FYTA_PASSWORD, or FYTA_TOKEN")
    seen = set()
    for item in (env.get("FYTA_PLANT_IDS") or "").split(","):
        item = item.strip()
        if not item:
            continue
        if not PLANT_ID.match(item):
            raise ConfigError("FYTA_PLANT_IDS must be comma-separated numbers")
        if item not in seen:
            seen.add(item)
            cfg["plant_ids"].append(item)
    return cfg


def run(cfg, client, now, send, err):
    """Read every plant and send the series. Returns the exit code (0 or 1)."""
    base = {"job": cfg["job"], "instance": cfg["instance"]}
    plants = [(pid, "") for pid in cfg["plant_ids"]]
    failures = []  # label sets of plants FYTA failed on
    try:
        if not plants:
            plants = discover(client)
            if not plants:
                raise FytaError("no plant with a sensor found in the account")
    except (FytaError, OSError, ValueError) as exc:
        print("FYTA failed: " + _describe(exc), file=err)
        return _push_failures([dict(base)], send, err)

    series = []
    for index, (pid, listed_name) in enumerate(plants):
        try:
            readings, info = collect(client, pid, now)
        except AuthError as exc:
            print("FYTA failed: " + str(exc), file=err)
            failures += [plant_labels(base, p, n) for p, n in plants[index:]]
            break
        except (FytaError, OSError, ValueError) as exc:
            print("FYTA failed: " + _describe(exc), file=err)
            failures.append(plant_labels(base, pid, listed_name))
            continue
        labels = plant_labels(base, pid, info["nickname"] or listed_name)
        series += build_series(readings, info, labels, now)

    for labels in failures:
        series += failure_series(labels)
    try:
        send(series)
    except PushError as exc:
        print("push failed: %s" % exc, file=err)
        return 1
    return 1 if failures else 0


def _describe(exc):
    """FytaError messages are ours and safe; anything else is reported by type only."""
    return str(exc) if isinstance(exc, FytaError) else type(exc).__name__


def _push_failures(label_sets, send, err):
    """Tell Prometheus the poll failed; the exit code is 1 either way."""
    try:
        send([item for labels in label_sets for item in failure_series(labels)])
    except PushError as exc:
        print("could not push the failure flag: %s" % exc, file=err)
    return 1


def main(argv=None, env=None, client=None, urlopen=OPENER.open, now=None, out=None, err=None,
         fyta_urlopen=OPENER.open):
    """Run once. `urlopen` talks to the receiver and `fyta_urlopen` to FYTA; tests replace both."""
    env = os.environ if env is None else env
    out = sys.stdout if out is None else out
    err = sys.stderr if err is None else err
    now = time.time() if now is None else now
    try:
        cfg = load_config(env)
    except ConfigError as exc:
        print("configuration error: %s" % exc, file=err)
        return 2
    if client is None:
        client = FytaClient(cfg["email"], cfg["fyta_password"], cfg["base_url"], urlopen=fyta_urlopen,
                            token=cfg["token"])

    def send(series):
        body = payload(series, now)
        host = urllib.parse.urlsplit(cfg["url"]).hostname if cfg["url"] else None
        if cfg["dry"]:
            print("DRY_RUN: %d series, %d bytes%s" % (len(series), len(body), " for " + host if host else ""),
                  file=out)
            for line in show(body):
                print(line, file=out)
            return
        push(cfg["url"], body, cfg["user"], cfg["password"], urlopen)
        print("pushed %d series to %s" % (len(series), host), file=out)

    return run(cfg, client, now, send, err)


if __name__ == "__main__":
    sys.exit(main())
