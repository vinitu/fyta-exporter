"""Offline tests for fyta_exporter.py: masked fixtures, no FYTA, no network beyond 127.0.0.1.

The payload is checked with a decoder written in this file on its own (a full snappy decoder, a
generic protobuf reader), not with the decoder in the script, so a symmetric mistake in the script's
encoder and decoder cannot pass. One test also sends a real HTTP POST to a sink on 127.0.0.1.

    python3 -m unittest discover -s tests          # from the repository root
"""
import base64
import io
import json
import re
import struct
import sys
import threading
import unittest
import urllib.error
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
FIXTURES = HERE / "fixtures"
sys.path.insert(0, str(ROOT))

import fyta_exporter as fe  # noqa: E402


def fixture(name):
    return json.loads((FIXTURES / name).read_text())


LIST = fixture("plant-list.json")
PLANT = {"1001": fixture("plant-1001.json"), "1002": fixture("plant-1002.json")}
WEEK = {"1001": fixture("week-1001.json"), "1002": fixture("week-1002.json")}

# The fixture weeks end on 2026-11-24 13:22:58 UTC.
NOW = datetime(2026, 11, 24, 13, 30, tzinfo=timezone.utc).timestamp()
NOW_MS = int(NOW * 1000)
LATEST = fe.epoch("2026-11-24 13:22:58")
EMAIL, PASSWORD, TOKEN = "user@example.invalid", "pw-for-tests", "tok-for-tests-123"
MUSER, MPASS = "sender-for-tests", "push-pw-for-tests"
SECRETS = (EMAIL, PASSWORD, TOKEN, MPASS, base64.b64encode(("%s:%s" % (MUSER, MPASS)).encode()).decode())
BASE = {"job": "fyta", "instance": "fyta"}


# ------------------------------------------------ an independent decoder

def ref_varint(buf, pos):
    result = shift = 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7


def ref_snappy(buf):
    """Full snappy block decoder: literals and copies, as the receiver would read it."""
    length, pos = ref_varint(buf, 0)
    out = bytearray()
    while pos < len(buf):
        tag = buf[pos]
        pos += 1
        kind = tag & 3
        if kind == 0:
            n = tag >> 2
            if n < 60:
                n += 1
            else:
                width = n - 59
                n = int.from_bytes(buf[pos:pos + width], "little") + 1
                pos += width
            out += buf[pos:pos + n]
            pos += n
        else:
            if kind == 1:
                n, offset, pos = 4 + ((tag >> 2) & 7), ((tag >> 5) << 8) | buf[pos], pos + 1
            elif kind == 2:
                n, offset, pos = 1 + (tag >> 2), int.from_bytes(buf[pos:pos + 2], "little"), pos + 2
            else:
                n, offset, pos = 1 + (tag >> 2), int.from_bytes(buf[pos:pos + 4], "little"), pos + 4
            for _ in range(n):
                out.append(out[-offset])
    assert len(out) == length, (len(out), length)
    return bytes(out)


def ref_message(buf):
    """Generic protobuf reader: field number -> list of raw values (bytes, int or 8 bytes)."""
    out, pos = {}, 0
    while pos < len(buf):
        key, pos = ref_varint(buf, pos)
        field, wire = key >> 3, key & 7
        if wire == 0:
            value, pos = ref_varint(buf, pos)
        elif wire == 1:
            value, pos = buf[pos:pos + 8], pos + 8
        elif wire == 2:
            size, pos = ref_varint(buf, pos)
            value, pos = buf[pos:pos + size], pos + size
        else:
            raise AssertionError("wire type %d" % wire)
        out.setdefault(field, []).append(value)
    assert pos == len(buf)
    return out


def ref_decode(body):
    """Snappy bytes of a WriteRequest -> [(labels, value, timestamp_ms), ...]."""
    request = ref_message(ref_snappy(body))
    assert set(request) <= {1}
    found = []
    for raw in request.get(1, []):
        ts = ref_message(raw)
        assert set(ts) <= {1, 2}
        names, labels = [], {}
        for label_raw in ts[1]:
            label = ref_message(label_raw)
            name = label[1][0].decode()
            names.append(name)
            labels[name] = label[2][0].decode()
        assert names == sorted(names), "labels must be sorted by name"
        assert len(ts[2]) == 1
        sample = ref_message(ts[2][0])
        found.append((labels, struct.unpack("<d", sample[1][0])[0], sample[2][0]))
    return found


def by_name(decoded, plant_id=None):
    """{metric name: [(labels, value)]} for the decoded series, optionally for one plant."""
    out = {}
    for labels, value, _ts in decoded:
        if plant_id is None or labels.get("plant_id") == plant_id:
            out.setdefault(labels["__name__"], []).append((labels, value))
    return out


# ------------------------------------------------ fakes

class FakeResponse(io.BytesIO):
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def http_error(code):
    return urllib.error.HTTPError("https://fyta.invalid/x", code, "err", {}, io.BytesIO())


class FakeApi:
    """A urlopen stand-in for FYTA. `script` maps (method, path) to a list of answers: a dict or an HTTP code."""

    def __init__(self, script):
        self.script = {key: list(answers) for key, answers in script.items()}
        self.calls = []

    def __call__(self, req, timeout=None):
        path = req.full_url.split("/api/", 1)[1]
        key = (req.get_method(), path)
        self.calls.append((key, req.get_header("Authorization")))
        answer = self.script[key].pop(0)
        if isinstance(answer, int):
            raise http_error(answer)
        return FakeResponse(json.dumps(answer).encode())


class FakeReceiver:
    """A urlopen stand-in for the remote_write endpoint."""

    def __init__(self, outcome=200):
        self.outcome = outcome
        self.requests = []

    def __call__(self, req, timeout=None):
        self.requests.append(req)
        if isinstance(self.outcome, Exception):
            raise self.outcome
        if isinstance(self.outcome, int) and self.outcome >= 300:
            raise urllib.error.HTTPError(req.full_url, self.outcome, "err", {}, io.BytesIO())
        resp = FakeResponse(b"")
        resp.status = self.outcome
        return resp


def good_script(ids=("1001", "1002"), login=True, listing=False):
    script = {}
    if login:
        script[("POST", "auth/login")] = [{"access_token": TOKEN}, {"access_token": TOKEN}]
    if listing:
        script[("GET", "user-plant")] = [LIST]
    for pid in ids:
        script[("GET", "user-plant/" + pid)] = [PLANT[pid]]
        script[("POST", "user-plant/measurements/" + pid)] = [WEEK[pid]]
    return script


def fyta(script=None):
    api = FakeApi(good_script() if script is None else script)
    return fe.FytaClient(EMAIL, PASSWORD, base_url="https://fyta.invalid/api", urlopen=api), api


BASE_ENV = {
    "FYTA_EMAIL": EMAIL, "FYTA_PASSWORD": PASSWORD, "FYTA_BASE_URL": "https://fyta.invalid/api",
    "FYTA_PLANT_IDS": "1001,1002", "METRICS_URL": "https://metrics.example.invalid/api/v1/write",
}


def run_main(script=None, receiver=None, env=None, now=NOW, drop=()):
    """Run main() against fakes. Returns (exit code, stdout, stderr, FakeApi, FakeReceiver)."""
    api = FakeApi(good_script() if script is None else script)
    receiver = FakeReceiver() if receiver is None else receiver
    out, err = io.StringIO(), io.StringIO()
    environment = {k: v for k, v in BASE_ENV.items() if k not in drop}
    environment.update(env or {})
    code = fe.main(env=environment, urlopen=receiver, fyta_urlopen=api, now=now, out=out, err=err)
    return code, out.getvalue(), err.getvalue(), api, receiver


def pushed(receiver):
    """Decode the single request a FakeReceiver got."""
    assert len(receiver.requests) == 1, len(receiver.requests)
    return ref_decode(receiver.requests[0].data)


def series_for(pid):
    readings, info = fe.collect(fyta()[0], pid, NOW)
    return fe.build_series(readings, info, fe.plant_labels(BASE, pid, info["nickname"]), NOW)


# ------------------------------------------------ parsing

class ParseMeasurements(unittest.TestCase):
    def test_latest_values_come_from_the_newest_row(self):
        out = fe.parse_measurements(WEEK["1001"], NOW)["values"]
        self.assertEqual(out["fyta_soil_moisture_percent"], 41.5)
        self.assertEqual(out["fyta_temperature_celsius"], 22.8)
        self.assertEqual(out["fyta_light_umol"], 310.4)
        self.assertEqual(out["fyta_soil_ec_ms_per_cm"], 1.12)

    def test_row_order_does_not_matter(self):
        week = WEEK["1001"]
        shuffled = dict(week, measurements=list(reversed(week["measurements"])))
        self.assertEqual(fe.parse_measurements(shuffled, NOW), fe.parse_measurements(week, NOW))

    def test_last_data_is_the_newest_row_time(self):
        self.assertEqual(fe.parse_measurements(WEEK["1001"], NOW)["last_data"], LATEST)

    def test_dli_is_the_last_full_day(self):
        out = fe.parse_measurements(WEEK["1001"], NOW)
        self.assertEqual(out["dli"], 6.5)
        self.assertEqual(out["dli_ts"], fe.epoch("2026-11-23 00:00:00"))

    def test_todays_partial_dli_is_not_used(self):
        week = WEEK["1001"]
        data = dict(week, dli_light=[{"dli_light": 1.5, "date_utc": "2026-11-24 00:00:00"}] + week["dli_light"])
        self.assertEqual(fe.parse_measurements(data, NOW)["dli"], 6.5)

    def test_null_measure_is_left_out_not_zero(self):
        week = WEEK["1001"]
        self.assertNotIn("fyta_air_humidity_percent", fe.parse_measurements(week, NOW)["values"])
        rows = [dict(r, soil_fertility=None) for r in week["measurements"]]
        out = fe.parse_measurements(dict(week, measurements=rows), NOW)["values"]
        self.assertNotIn("fyta_soil_ec_ms_per_cm", out)
        self.assertIn("fyta_soil_moisture_percent", out)

    def test_a_null_in_the_newest_row_falls_back_to_the_previous_one(self):
        rows = WEEK["1001"]["measurements"]
        data = dict(WEEK["1001"], measurements=[dict(rows[0], light=None)] + rows[1:])
        self.assertEqual(fe.parse_measurements(data, NOW)["values"]["fyta_light_umol"], rows[1]["light"])

    def test_empty_and_missing_sections(self):
        for data in ({}, {"measurements": [], "dli_light": []}, {"measurements": None, "dli_light": None},
                     None, [], "junk", {"measurements": ["x", 3, None], "dli_light": [None, "y"]}):
            out = fe.parse_measurements(data, NOW)
            self.assertEqual(out["values"], {})
            self.assertIsNone(out["last_data"])
            self.assertIsNone(out["dli"])

    def test_junk_values_are_skipped(self):
        rows = [{"soil_moisture": "n/a", "light": True, "temperature": "21.5", "soil_fertility": float("nan"),
                 "date_utc": "2026-11-24 13:00:00"},
                {"date_utc": "not a date", "soil_moisture": 5}]
        out = fe.parse_measurements({"measurements": rows}, NOW)["values"]
        self.assertEqual(out, {"fyta_temperature_celsius": 21.5})


class ParsePlant(unittest.TestCase):
    def test_nickname_battery_status_and_ranges(self):
        out = fe.parse_plant(PLANT["1002"])
        self.assertEqual(out["nickname"], "Monstera")
        self.assertEqual(out["battery"], 64.0)
        self.assertEqual(out["status"], {"moisture": 2.0, "temperature": 3.0, "light": 2.0, "salinity": 3.0})
        self.assertEqual(out["good_min"]["moisture"], 35.0)
        self.assertEqual(out["good_max"]["salinity"], 1.6)

    def test_battery_falls_back_to_the_measurements_block(self):
        data = json.loads(json.dumps(PLANT["1001"]))
        data["plant"]["sensors"] = []
        self.assertEqual(fe.parse_plant(data)["battery"], 87.0)

    def test_missing_pieces_do_not_raise(self):
        for data in ({}, None, {"plant": None}, {"plant": {"sensors": [None], "measurements": {"light": None}}}):
            out = fe.parse_plant(data)
            self.assertIsNone(out["battery"])
            self.assertEqual(out["status"], {})
            self.assertEqual(out["nickname"], "")


class Discovery(unittest.TestCase):
    def test_every_plant_with_a_sensor_is_found(self):
        client, api = fyta(good_script(ids=(), login=True, listing=True))
        self.assertEqual(fe.discover(client), [("1001", "Fiddle leaf"), ("1002", "Monstera")])
        self.assertEqual([k for k, _ in api.calls], [("POST", "auth/login"), ("GET", "user-plant")])

    def test_a_plant_without_a_sensor_is_skipped(self):
        names = [n for _i, n in fe.discover(fyta(good_script(ids=(), listing=True))[0])]
        self.assertNotIn("Cactus without sensor", names)

    def test_junk_in_the_list_is_ignored_and_ids_are_unique(self):
        data = {"plants": [None, "x", {"id": True}, {"id": "../etc"}, {"id": -3}, {"id": 5, "nickname": "a"},
                           {"id": "5", "nickname": "dup"}, {"id": 6}]}
        api = FakeApi({("GET", "user-plant"): [data]})
        client = fe.FytaClient(base_url="https://fyta.invalid/api", urlopen=api, token=TOKEN)
        self.assertEqual(fe.discover(client), [("5", "a"), ("6", "")])

    def test_empty_answers(self):
        for data in ({}, {"plants": None}, {"plants": []}, [], None):
            api = FakeApi({("GET", "user-plant"): [data]})
            client = fe.FytaClient(base_url="https://fyta.invalid/api", urlopen=api, token=TOKEN)
            self.assertEqual(fe.discover(client), [])


class Labels(unittest.TestCase):
    def test_control_characters_and_whitespace_are_cleaned(self):
        self.assertEqual(fe.clean_label("  Fiddle\tleaf \n\x00  two  "), "Fiddle leaf two")

    def test_length_is_capped(self):
        out = fe.clean_label("x" * 500)
        self.assertEqual(len(out), fe.LABEL_LIMIT)

    def test_non_ascii_text_is_kept(self):
        self.assertEqual(fe.clean_label("Zażółć gęślą"), "Zażółć gęślą")

    def test_non_strings_and_empty(self):
        self.assertEqual(fe.clean_label(None), "None")
        self.assertEqual(fe.clean_label(" \n "), "")

    def test_a_messy_nickname_reaches_the_payload_clean(self):
        plant = json.loads(json.dumps(PLANT["1001"]))
        plant["plant"]["nickname"] = '  Big\n"one"\t\x07 '
        script = good_script(ids=("1002",))
        script[("GET", "user-plant/1001")] = [plant]
        script[("POST", "user-plant/measurements/1001")] = [WEEK["1001"]]
        code, _out, err, _api, receiver = run_main(script)
        self.assertEqual(code, 0, err)
        plants = {l["plant"] for l, _v, _t in pushed(receiver) if l["plant_id"] == "1001"}
        self.assertEqual(plants, {'Big "one"'})

    def test_dry_run_escapes_quotes(self):
        plant = json.loads(json.dumps(PLANT["1001"]))
        plant["plant"]["nickname"] = 'a"b'
        script = good_script(ids=("1002",))
        script[("GET", "user-plant/1001")] = [plant]
        script[("POST", "user-plant/measurements/1001")] = [WEEK["1001"]]
        _c, out, _e, _a, _r = run_main(script, env={"DRY_RUN": "1"})
        self.assertIn('plant="a\\"b"', out)


# ------------------------------------------------ FYTA client

class Client(unittest.TestCase):
    LOGIN = ("POST", "auth/login")
    LIST_KEY = ("GET", "user-plant")

    def client(self, script, **kwargs):
        api = FakeApi(script)
        return fe.FytaClient(EMAIL, PASSWORD, base_url="https://fyta.invalid/api", urlopen=api, **kwargs), api

    def test_logs_in_once_and_reuses_the_token(self):
        c, api = self.client({self.LOGIN: [{"access_token": "T1"}], self.LIST_KEY: [LIST, LIST]})
        c.call("GET", "user-plant")
        c.call("GET", "user-plant")
        self.assertEqual([k for k, _ in api.calls], [self.LOGIN, self.LIST_KEY, self.LIST_KEY])
        self.assertEqual(api.calls[1][1], "Bearer T1")

    def test_a_preset_token_skips_the_login(self):
        c, api = self.client({self.LIST_KEY: [LIST]}, token="T0")
        c.call("GET", "user-plant")
        self.assertEqual([k for k, _ in api.calls], [self.LIST_KEY])
        self.assertEqual(api.calls[0][1], "Bearer T0")

    def test_logs_in_again_on_401_and_retries(self):
        c, api = self.client({self.LOGIN: [{"access_token": "T1"}, {"access_token": "T2"}],
                              self.LIST_KEY: [401, LIST]})
        self.assertEqual(c.call("GET", "user-plant"), LIST)
        self.assertEqual([k for k, _ in api.calls], [self.LOGIN, self.LIST_KEY, self.LOGIN, self.LIST_KEY])
        self.assertEqual(api.calls[3][1], "Bearer T2")

    def test_a_second_401_is_an_error_not_a_loop(self):
        c, api = self.client({self.LOGIN: [{"access_token": "T1"}, {"access_token": "T2"}],
                              self.LIST_KEY: [401, 401]})
        with self.assertRaises(fe.AuthError):
            c.call("GET", "user-plant")
        self.assertEqual(len(api.calls), 4)

    def test_rejected_login_is_an_error_without_secrets(self):
        c, _ = self.client({self.LOGIN: [401]})
        with self.assertRaises(fe.AuthError) as ctx:
            c.call("GET", "user-plant")
        text = str(ctx.exception)
        self.assertIn("401", text)
        for secret in SECRETS:
            self.assertNotIn(secret, text)

    def test_login_without_a_token_is_an_error(self):
        c, _ = self.client({self.LOGIN: [{"expires_in": 5184000}]})
        with self.assertRaises(fe.AuthError):
            c.call("GET", "user-plant")

    def test_other_http_errors_do_not_trigger_a_login(self):
        c, api = self.client({self.LOGIN: [{"access_token": "T1"}], self.LIST_KEY: [500]})
        with self.assertRaises(fe.FytaError) as ctx:
            c.call("GET", "user-plant")
        self.assertNotIsInstance(ctx.exception, fe.AuthError)
        self.assertEqual(len(api.calls), 2)

    def test_a_preset_token_without_a_login_fails_on_401(self):
        api = FakeApi({self.LIST_KEY: [401]})
        c = fe.FytaClient(None, None, base_url="https://fyta.invalid/api", urlopen=api, token=TOKEN)
        with self.assertRaises(fe.AuthError) as ctx:
            c.call("GET", "user-plant")
        self.assertNotIn(TOKEN, str(ctx.exception))
        self.assertEqual([k for k, _ in api.calls], [self.LIST_KEY])


# ------------------------------------------------ metrics

class Series(unittest.TestCase):
    def test_metric_set_and_values(self):
        rows = {}
        for labels, value in series_for("1001"):
            rows.setdefault(labels["__name__"], []).append((labels, value))
        single = {name: items[0][1] for name, items in rows.items() if len(items) == 1}
        self.assertEqual(single["fyta_soil_moisture_percent"], 41.5)
        self.assertEqual(single["fyta_temperature_celsius"], 22.8)
        self.assertEqual(single["fyta_light_umol"], 310.4)
        self.assertEqual(single["fyta_soil_ec_ms_per_cm"], 1.12)
        self.assertEqual(single["fyta_battery_percent"], 87.0)
        self.assertEqual(single["fyta_dli_mol_per_day"], 6.5)
        self.assertEqual(single["fyta_scrape_success"], 1.0)
        self.assertEqual(single["fyta_last_success_timestamp_seconds"], NOW)
        self.assertEqual(single["fyta_last_data_timestamp_seconds"], LATEST)
        self.assertEqual(single["fyta_last_data_age_seconds"], round(NOW - LATEST, 1))
        for name in ("fyta_measure_status", "fyta_measure_good_min", "fyta_measure_good_max"):
            self.assertEqual({l["measure"] for l, _ in rows[name]},
                             {"moisture", "temperature", "light", "salinity"})
        moisture_min = [v for l, v in rows["fyta_measure_good_min"] if l["measure"] == "moisture"]
        self.assertEqual(moisture_min, [35.0])

    def test_names_and_labels_follow_the_prometheus_rules(self):
        for labels, value in series_for("1001"):
            self.assertRegex(labels["__name__"], r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
            self.assertTrue(labels["__name__"].startswith("fyta_"))
            for name, text in labels.items():
                if name != "__name__":
                    self.assertRegex(name, r"^[a-zA-Z_][a-zA-Z0-9_]*$")
                    self.assertFalse(name.startswith("__"))
                self.assertIsInstance(text, str)
            self.assertEqual(labels["job"], "fyta")
            self.assertEqual(labels["instance"], "fyta")
            self.assertEqual(labels["plant_id"], "1001")
            self.assertEqual(labels["plant"], "Fiddle leaf")
            self.assertTrue(value == value and abs(value) != float("inf"))

    def test_series_are_unique(self):
        keys = [frozenset(l.items()) for l, _ in series_for("1001")]
        self.assertEqual(len(keys), len(set(keys)))

    def test_the_plant_label_is_left_out_when_there_is_no_nickname(self):
        labels = fe.plant_labels(BASE, "1001", "")
        self.assertNotIn("plant", labels)
        self.assertEqual(labels["plant_id"], "1001")

    def test_missing_fields_are_skipped_without_a_crash(self):
        readings = fe.parse_measurements({"measurements": [{"date_utc": "2026-11-24 13:00:00",
                                                            "soil_moisture": 40}]}, NOW)
        series = fe.build_series(readings, fe.parse_plant({}), fe.plant_labels(BASE, "1001"), NOW)
        names = {l["__name__"] for l, _ in series}
        self.assertEqual(names, {"fyta_soil_moisture_percent", "fyta_last_data_timestamp_seconds",
                                 "fyta_last_data_age_seconds", "fyta_scrape_success",
                                 "fyta_last_success_timestamp_seconds"})

    def test_failure_series_is_only_the_flag(self):
        series = fe.failure_series(fe.plant_labels(BASE, "1001", "Fiddle leaf"))
        self.assertEqual([(l["__name__"], v) for l, v in series], [("fyta_scrape_success", 0.0)])
        self.assertEqual(series[0][0]["plant_id"], "1001")


# ------------------------------------------------ encoding

class Encoding(unittest.TestCase):
    def test_hand_computed_bytes_for_one_series(self):
        # Label{name="__name__",value="a"} = 0a 08 "__name__" 12 01 "a"  (13 bytes)
        # Sample{1.0, 1} = 09 <1.0 little endian> 10 01                    (11 bytes)
        # TimeSeries = 0a 0d <label> 12 0b <sample>                        (28 bytes)
        expected = (b"\x0a\x1c" + b"\x0a\x0d" + b"\x0a\x08__name__\x12\x01a"
                    + b"\x12\x0b" + b"\x09" + bytes.fromhex("000000000000f03f") + b"\x10\x01")
        self.assertEqual(fe.encode_write_request([({"__name__": "a"}, 1.0)], 1), expected)

    def test_snappy_of_a_short_body_is_length_then_one_literal(self):
        self.assertEqual(fe.snappy_encode(b"abc"), b"\x03" + bytes([2 << 2]) + b"abc")
        self.assertEqual(fe.snappy_encode(b""), b"\x00")
        self.assertEqual(fe.snappy_encode(b"x" * 60), b"\x3c" + bytes([59 << 2]) + b"x" * 60)
        # 61 bytes: length-1 = 60 does not fit in the tag, so tag 60<<2 and one length byte follow.
        self.assertEqual(fe.snappy_encode(b"x" * 61), b"\x3d" + bytes([60 << 2, 60]) + b"x" * 61)

    def test_snappy_round_trips_at_every_boundary(self):
        for size in (0, 1, 59, 60, 61, 255, 256, 257, 65535, 65536, 65537, 131072, 200003):
            data = bytes((i * 7 + 3) % 256 for i in range(size))
            self.assertEqual(ref_snappy(fe.snappy_encode(data)), data, size)
            self.assertEqual(fe.snappy_decode(fe.snappy_encode(data)), data, size)

    def test_snappy_output_is_a_valid_block(self):
        """Walk the block by the format rules: preamble, then literal elements only, no slack."""
        for size in (0, 1, 59, 60, 61, 256, 65535, 65536, 65537, 131072, 200003):
            data = bytes(range(256)) * (size // 256) + bytes(range(size % 256))
            block = fe.snappy_encode(data)
            length, pos = ref_varint(block, 0)
            self.assertEqual(length, size)  # the preamble is the uncompressed length
            chunks, total = [], 0
            while pos < len(block):
                tag = block[pos]
                pos += 1
                self.assertEqual(tag & 3, 0, "only literal elements (tag type 00)")
                n = tag >> 2
                if n >= 60:  # 60..63: one to four length bytes follow
                    width = n - 59
                    n = int.from_bytes(block[pos:pos + width], "little")
                    pos += width
                    self.assertGreaterEqual(n, 60, "an inline length would have fitted the tag")
                    self.assertTrue(n >> (8 * (width - 1)) or width == 1, "no needless length bytes")
                n += 1
                self.assertLessEqual(n, 65536)
                self.assertEqual(block[pos:pos + n], data[total:total + n])
                pos += n
                total += n
                chunks.append(n)
            self.assertEqual(pos, len(block))
            self.assertEqual(total, size)
            expected = [65536] * (size // 65536) + ([size % 65536] if size % 65536 else [])
            self.assertEqual(chunks, expected, size)

    def test_snappy_tags_for_the_size_classes(self):
        self.assertEqual(fe.snappy_encode(b"x" * 59)[1], 58 << 2)            # inline length
        self.assertEqual(fe.snappy_encode(b"x" * 60)[1], 59 << 2)            # the last inline length
        self.assertEqual(fe.snappy_encode(b"x" * 61)[1], 60 << 2)            # one length byte
        self.assertEqual(fe.snappy_encode(b"x" * 256)[2], 60 << 2)           # 255 still fits in one byte
        big = fe.snappy_encode(b"x" * 257)
        self.assertEqual((big[2], big[3:5]), (61 << 2, (256).to_bytes(2, "little")))  # two length bytes
        # 65536 bytes: length-1 = 65535 needs two bytes; the next chunk of 1 byte is an inline literal.
        two = fe.snappy_encode(b"y" * 65537)
        self.assertEqual(two[3], 61 << 2)
        self.assertEqual(two[-2], 0)  # tag of a one-byte literal

    def test_reference_decoder_reads_a_real_snappy_stream_with_copies(self):
        # "abcabcabc" as snappy would write it: literal "abc", then copy(len 6, offset 3).
        stream = b"\x09" + bytes([2 << 2]) + b"abc" + bytes([((6 - 4) << 2) | 1, 3])
        self.assertEqual(ref_snappy(stream), b"abcabcabc")

    def test_every_series_round_trips_through_the_reference_decoder(self):
        series = series_for("1001")
        decoded = ref_decode(fe.payload(series, NOW))
        self.assertEqual(len(decoded), len(series))
        for (labels, value), (got_labels, got_value, got_ts) in zip(series, decoded):
            self.assertEqual(got_labels, labels)
            self.assertEqual(got_value, value)
            self.assertEqual(got_ts, NOW_MS)

    def test_the_script_decoder_agrees_with_the_reference(self):
        body = fe.payload(series_for("1001"), NOW)
        mine = [(l, s[0][0], s[0][1]) for l, s in fe.decode_write_request(fe.snappy_decode(body))]
        self.assertEqual(mine, ref_decode(body))

    def test_labels_are_sorted_and_name_is_a_label(self):
        decoded = ref_decode(fe.payload([({"zeta": "1", "__name__": "m", "alpha": "2"}, 3.5)], NOW))
        self.assertEqual(list(decoded[0][0]), ["__name__", "alpha", "zeta"])

    def test_utf8_and_big_values(self):
        decoded = ref_decode(fe.payload([({"__name__": "m", "x": "zażółć"}, -1.5e300)], NOW))
        self.assertEqual(decoded[0][0]["x"], "zażółć")
        self.assertEqual(decoded[0][1], -1.5e300)

    def test_invalid_names_are_refused(self):
        for bad in ({"__name__": "1abc"}, {"__name__": "ok", "bad-label": "x"}, {"__name__": "ok", "": "x"}):
            with self.assertRaises(ValueError):
                fe.encode_write_request([(bad, 1.0)], 1)

    def test_timestamp_is_the_push_time_in_milliseconds(self):
        self.assertEqual({t for _l, _v, t in ref_decode(fe.payload(series_for("1001"), NOW))}, {NOW_MS})


# ------------------------------------------------ configuration

class Config(unittest.TestCase):
    def code(self, env, **kwargs):
        err = io.StringIO()
        code = fe.main(env=env, out=io.StringIO(), err=err, **kwargs)
        return code, err.getvalue()

    def test_nothing_configured_is_exit_2(self):
        code, err = self.code({})
        self.assertEqual(code, 2)
        self.assertIn("METRICS_URL", err)

    def test_a_login_or_a_token_is_required(self):
        env = {"METRICS_URL": BASE_ENV["METRICS_URL"]}
        for extra in ({}, {"FYTA_EMAIL": EMAIL}, {"FYTA_PASSWORD": PASSWORD}):
            code, err = self.code(dict(env, **extra))
            self.assertEqual(code, 2)
            self.assertIn("FYTA_TOKEN", err)

    def test_a_token_alone_is_enough(self):
        self.assertEqual(fe.load_config({"METRICS_URL": "https://m.example.invalid/w", "FYTA_TOKEN": TOKEN})["token"],
                         TOKEN)

    def test_metrics_url_is_required_unless_dry_run(self):
        env = {"FYTA_TOKEN": TOKEN}
        self.assertEqual(self.code(env)[0], 2)
        self.assertIsNotNone(fe.load_config(dict(env, DRY_RUN="1")))

    def test_bad_plant_ids_are_refused(self):
        for bad in ("abc", "1,../2", "1;2", "-5", "1.5"):
            code, err = self.code(dict(BASE_ENV, FYTA_PLANT_IDS=bad))
            self.assertEqual(code, 2, bad)
            self.assertIn("FYTA_PLANT_IDS", err)

    def test_plant_ids_are_trimmed_and_unique(self):
        cfg = fe.load_config(dict(BASE_ENV, FYTA_PLANT_IDS=" 7, 8 ,,7"))
        self.assertEqual(cfg["plant_ids"], ["7", "8"])

    def test_labels_default_and_override(self):
        cfg = fe.load_config(BASE_ENV)
        self.assertEqual((cfg["job"], cfg["instance"]), ("fyta", "fyta"))
        cfg = fe.load_config(dict(BASE_ENV, INSTANCE_LABEL=" office\n", JOB_LABEL="plants"))
        self.assertEqual((cfg["job"], cfg["instance"]), ("plants", "office"))

    def test_urls_must_be_http_without_a_login_inside(self):
        for url in ("ftp://m.example.invalid/w", "m.example.invalid/w", "https://u:p@m.example.invalid/w"):
            code, err = self.code(dict(BASE_ENV, METRICS_URL=url))
            self.assertEqual(code, 2, url)
            self.assertNotIn("p@", err)
        self.assertEqual(self.code(dict(BASE_ENV, FYTA_BASE_URL="http://fyta.example.invalid/api"))[0], 2)

    def test_a_password_over_plain_http_is_refused_before_any_call(self):
        api = FakeApi({})
        code, err = self.code(dict(BASE_ENV, METRICS_URL="http://prometheus.example.invalid/api/v1/write",
                                   METRICS_USER=MUSER, METRICS_PASSWORD=MPASS), fyta_urlopen=api)
        self.assertEqual(code, 2)
        self.assertIn("plain http", err)
        self.assertNotIn(MPASS, err)
        self.assertEqual(api.calls, [])

    def test_the_push_function_refuses_too(self):
        receiver = FakeReceiver()
        with self.assertRaises(fe.PushError) as ctx:
            fe.push("http://prometheus.example.invalid/w", b"", MUSER, MPASS, receiver)
        self.assertNotIn(MPASS, str(ctx.exception))
        self.assertEqual(receiver.requests, [])

    def test_basic_auth_is_allowed_to_localhost_and_over_https(self):
        for url in ("http://127.0.0.1:9099/api/v1/write", "http://localhost/w", "https://m.example.invalid/w"):
            code, _o, err, _a, receiver = run_main(env={"METRICS_URL": url, "METRICS_USER": MUSER,
                                                        "METRICS_PASSWORD": MPASS})
            self.assertEqual(code, 0, err)
            header = receiver.requests[0].get_header("Authorization")
            self.assertEqual(base64.b64decode(header.split()[1]).decode(), "%s:%s" % (MUSER, MPASS))

    def test_dry_run_allows_plain_http_with_a_password(self):
        code, *_rest = run_main(env={"DRY_RUN": "1", "METRICS_URL": "http://prometheus.example.invalid/w",
                                     "METRICS_USER": MUSER, "METRICS_PASSWORD": MPASS})
        self.assertEqual(code, 0)


# ------------------------------------------------ runs

class Runs(unittest.TestCase):
    def test_a_good_run_pushes_once_and_exits_0(self):
        code, out, err, _api, receiver = run_main()
        self.assertEqual(code, 0, err)
        self.assertEqual(len(receiver.requests), 1)
        req = receiver.requests[0]
        self.assertEqual(req.get_method(), "POST")
        self.assertEqual(req.get_header("Content-encoding"), "snappy")
        self.assertEqual(req.get_header("Content-type"), "application/x-protobuf")
        self.assertEqual(req.get_header("X-prometheus-remote-write-version"), "0.1.0")
        self.assertEqual(len(ref_decode(req.data)), len(series_for("1001")) + len(series_for("1002")))
        self.assertIn("pushed", out)
        self.assertEqual(err, "")

    def test_no_basic_auth_without_credentials(self):
        receiver = run_main()[4]
        self.assertIsNone(receiver.requests[0].get_header("Authorization"))

    def test_multi_plant_output_has_one_set_per_plant(self):
        _c, _o, _e, _a, receiver = run_main()
        decoded = pushed(receiver)
        for pid, nick, moisture, battery in (("1001", "Fiddle leaf", 41.5, 87.0), ("1002", "Monstera", 28.0, 64.0)):
            rows = by_name(decoded, pid)
            self.assertEqual(rows["fyta_soil_moisture_percent"][0][1], moisture)
            self.assertEqual(rows["fyta_battery_percent"][0][1], battery)
            self.assertEqual(rows["fyta_scrape_success"][0][1], 1.0)
            self.assertEqual({l["plant"] for l, _v in rows["fyta_scrape_success"]}, {nick})
        keys = [frozenset(l.items()) for l, _v, _t in decoded]
        self.assertEqual(len(keys), len(set(keys)))

    def test_plant_ids_from_the_environment_are_used_and_the_list_is_not_called(self):
        _c, _o, _e, api, _r = run_main(env={"FYTA_PLANT_IDS": "1002"}, script=good_script(ids=("1002",)))
        paths = [k[1] for k, _a in api.calls]
        self.assertNotIn("user-plant", paths)
        self.assertEqual(paths, ["auth/login", "user-plant/1002", "user-plant/measurements/1002"])

    def test_without_plant_ids_every_plant_is_discovered(self):
        script = good_script(ids=("1001", "1002"), listing=True)
        code, _o, err, api, receiver = run_main(script, drop=("FYTA_PLANT_IDS",))
        self.assertEqual(code, 0, err)
        self.assertEqual({l["plant_id"] for l, _v, _t in pushed(receiver)}, {"1001", "1002"})
        self.assertIn("user-plant", [k[1] for k, _a in api.calls])
        self.assertNotIn("user-plant/1003", [k[1] for k, _a in api.calls])  # the plant without a sensor

    def test_the_token_path_does_not_log_in(self):
        script = good_script(login=False)
        code, _o, err, api, receiver = run_main(script, env={"FYTA_TOKEN": TOKEN}, drop=("FYTA_EMAIL", "FYTA_PASSWORD"))
        self.assertEqual(code, 0, err)
        self.assertNotIn(("POST", "auth/login"), [k for k, _a in api.calls])
        self.assertEqual({a for _k, a in api.calls}, {"Bearer " + TOKEN})
        self.assertEqual(len(pushed(receiver)), len(series_for("1001")) + len(series_for("1002")))

    def test_a_rejected_token_without_a_login_is_exit_1_and_flags_failure(self):
        script = {("GET", "user-plant/1001"): [401], ("GET", "user-plant/1002"): [401]}
        code, _o, err, api, receiver = run_main(script, env={"FYTA_TOKEN": TOKEN}, drop=("FYTA_EMAIL", "FYTA_PASSWORD"))
        self.assertEqual(code, 1)
        self.assertNotIn(TOKEN, err)
        self.assertEqual(len(api.calls), 1)  # an auth failure stops the whole run
        self.assertEqual(sorted((l["plant_id"], v) for l, v, _t in pushed(receiver)),
                         [("1001", 0.0), ("1002", 0.0)])

    def test_labels_follow_the_environment(self):
        _c, _o, _e, _a, receiver = run_main(env={"INSTANCE_LABEL": "office", "JOB_LABEL": "plants"})
        for labels, _v, _t in pushed(receiver):
            self.assertEqual((labels["job"], labels["instance"]), ("plants", "office"))

    def test_receiver_errors_give_exit_1(self):
        for outcome in (400, 401, 403, 404, 429, 500, 503, 302,
                        urllib.error.URLError("refused"), TimeoutError("slow"), ConnectionResetError()):
            code, _out, err, _api, receiver = run_main(receiver=FakeReceiver(outcome))
            self.assertEqual(code, 1, outcome)
            self.assertIn("push failed", err)
            self.assertEqual(len(receiver.requests), 1)

    def test_a_redirect_is_not_followed(self):
        handler = fe._NoRedirect()
        self.assertIsNone(handler.redirect_request(None, None, 302, "Found", {}, "https://elsewhere.invalid/"))


class Failures(unittest.TestCase):
    def test_login_401_gives_exit_1_and_flags_the_failure(self):
        code, _out, err, _api, receiver = run_main({("POST", "auth/login"): [401]})
        self.assertEqual(code, 1)
        self.assertIn("FYTA failed", err)
        self.assertIn("401", err)
        decoded = pushed(receiver)
        self.assertEqual(sorted((l["plant_id"], l["__name__"], v) for l, v, _t in decoded),
                         [("1001", "fyta_scrape_success", 0.0), ("1002", "fyta_scrape_success", 0.0)])

    def test_login_failure_during_discovery_flags_one_series_without_a_plant(self):
        code, _out, err, _api, receiver = run_main({("POST", "auth/login"): [401]}, drop=("FYTA_PLANT_IDS",))
        self.assertEqual(code, 1)
        decoded = pushed(receiver)
        self.assertEqual([(l["__name__"], v) for l, v, _t in decoded], [("fyta_scrape_success", 0.0)])
        self.assertNotIn("plant_id", decoded[0][0])

    def test_no_plants_found_is_exit_1(self):
        script = {("POST", "auth/login"): [{"access_token": TOKEN}], ("GET", "user-plant"): [{"plants": []}]}
        code, _out, err, _api, receiver = run_main(script, drop=("FYTA_PLANT_IDS",))
        self.assertEqual(code, 1)
        self.assertIn("no plant", err)
        self.assertEqual([v for _l, v, _t in pushed(receiver)], [0.0])

    def test_one_failing_plant_still_pushes_the_other_and_exits_1(self):
        script = good_script()
        script[("GET", "user-plant/1002")] = [500]
        code, _out, err, _api, receiver = run_main(script)
        self.assertEqual(code, 1)
        self.assertIn("HTTP 500", err)
        decoded = pushed(receiver)
        good = by_name(decoded, "1001")
        self.assertEqual(good["fyta_scrape_success"][0][1], 1.0)
        self.assertIn("fyta_soil_moisture_percent", good)
        bad = by_name(decoded, "1002")
        self.assertEqual(list(bad), ["fyta_scrape_success"])
        self.assertEqual(bad["fyta_scrape_success"][0][1], 0.0)

    def test_empty_data_gives_exit_1(self):
        for week in ({"measurements": [], "dli_light": []}, {}, {"measurements": [{"date_utc": "x"}]}):
            script = good_script(ids=("1001",))
            script[("POST", "user-plant/measurements/1001")] = [week]
            code, _out, err, _api, receiver = run_main(script, env={"FYTA_PLANT_IDS": "1001"})
            self.assertEqual(code, 1)
            self.assertIn("no readings", err)
            self.assertEqual([l["__name__"] for l, _v, _t in pushed(receiver)], ["fyta_scrape_success"])

    def test_network_down_towards_fyta_gives_exit_1(self):
        def down(req, timeout=None):
            raise urllib.error.URLError("no route")
        err = io.StringIO()
        receiver = FakeReceiver()
        code = fe.main(env=BASE_ENV, fyta_urlopen=down, urlopen=receiver, now=NOW, out=io.StringIO(), err=err)
        self.assertEqual(code, 1)
        self.assertIn("FYTA failed", err.getvalue())
        self.assertIn("URLError", err.getvalue())
        self.assertEqual(len(receiver.requests), 1)

    def test_a_body_that_is_not_json_gives_exit_1(self):
        def html(req, timeout=None):
            return FakeResponse(b"<html>maintenance</html>")
        err = io.StringIO()
        code = fe.main(env=BASE_ENV, fyta_urlopen=html, urlopen=FakeReceiver(), now=NOW,
                       out=io.StringIO(), err=err)
        self.assertEqual(code, 1)
        self.assertIn("FYTA failed", err.getvalue())

    def test_failure_flag_push_failing_too_is_still_exit_1(self):
        code, _out, err, _api, _receiver = run_main({("POST", "auth/login"): [401]}, receiver=FakeReceiver(500))
        self.assertEqual(code, 1)
        self.assertIn("push failed", err)

    def test_failure_flag_push_failing_during_discovery_is_still_exit_1(self):
        code, _out, err, _api, _receiver = run_main({("POST", "auth/login"): [401]}, receiver=FakeReceiver(500),
                                                    drop=("FYTA_PLANT_IDS",))
        self.assertEqual(code, 1)
        self.assertIn("could not push the failure flag", err)

    def test_a_failure_with_a_dead_receiver_is_still_exit_1(self):
        script = good_script()
        script[("GET", "user-plant/1002")] = [500]
        code, _out, err, _api, _receiver = run_main(script, receiver=FakeReceiver(500))
        self.assertEqual(code, 1)
        self.assertIn("push failed", err)


# ------------------------------------------------ DRY_RUN and secrets

class DryRun(unittest.TestCase):
    def test_dry_run_sends_nothing_and_prints_every_series(self):
        code, out, err, _api, receiver = run_main(env={"DRY_RUN": "1", "METRICS_USER": MUSER,
                                                       "METRICS_PASSWORD": MPASS})
        self.assertEqual(code, 0, err)
        self.assertEqual(receiver.requests, [])
        lines = [l for l in out.splitlines() if l.startswith("fyta_")]
        self.assertEqual(len(lines), len(series_for("1001")) + len(series_for("1002")))
        self.assertIn('fyta_soil_moisture_percent{instance="fyta",job="fyta",plant="Fiddle leaf",plant_id="1001"} '
                      "41.5 @%d" % NOW_MS, lines)
        self.assertIn("DRY_RUN", out)

    def test_dry_run_needs_no_receiver(self):
        code, out, _e, _a, receiver = run_main(env={"DRY_RUN": "1"}, drop=("METRICS_URL",))
        self.assertEqual(code, 0)
        self.assertEqual(receiver.requests, [])
        self.assertIn("DRY_RUN", out)

    def test_dry_run_prints_no_credentials(self):
        _c, out, err, _a, _r = run_main(env={"DRY_RUN": "true", "METRICS_USER": MUSER, "METRICS_PASSWORD": MPASS,
                                             "FYTA_TOKEN": TOKEN})
        for secret in SECRETS + (MUSER,):
            self.assertNotIn(secret, out + err)
        self.assertNotIn("Bearer", out + err)
        self.assertNotRegex(out + err, r"(?i)password|secret|authorization|token")

    def test_dry_run_off_values(self):
        for value in ("", "0", "false", "no"):
            receiver = run_main(env={"DRY_RUN": value})[4]
            self.assertEqual(len(receiver.requests), 1, value)

    def test_no_credential_appears_in_any_output(self):
        creds = {"METRICS_USER": MUSER, "METRICS_PASSWORD": MPASS}
        scenarios = [
            run_main(env=creds),
            run_main({("POST", "auth/login"): [401]}, env=creds),
            run_main(receiver=FakeReceiver(403), env=creds),
            run_main(receiver=FakeReceiver(urllib.error.URLError("%s %s" % (MPASS, TOKEN))), env=creds),
            run_main(env=dict(creds, DRY_RUN="1")),
        ]
        for _code, out, err, _api, _receiver in scenarios:
            for secret in SECRETS:
                self.assertNotIn(secret, out + err)
            self.assertNotIn("Bearer", out + err)

    def test_a_token_that_leaks_into_an_exception_is_not_printed(self):
        def boom(req, timeout=None):
            raise OSError("connect failed for " + TOKEN)
        err = io.StringIO()
        fe.main(env=BASE_ENV, fyta_urlopen=boom, urlopen=FakeReceiver(), now=NOW, out=io.StringIO(), err=err)
        self.assertNotIn(TOKEN, err.getvalue())

    def test_the_payload_carries_no_credential(self):
        body = ref_snappy(fe.payload(series_for("1001"), NOW))
        for secret in (EMAIL, PASSWORD, TOKEN, "Bearer"):
            self.assertNotIn(secret.encode(), body)


# ------------------------------------------------ a real HTTP round trip on 127.0.0.1

class SinkHandler(BaseHTTPRequestHandler):
    seen = []

    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        SinkHandler.seen.append((self.path, dict(self.headers), body))
        self.send_response(204)
        self.end_headers()

    def log_message(self, *args):
        pass


class LoopbackSink(unittest.TestCase):
    def test_a_real_post_reaches_a_local_receiver(self):
        SinkHandler.seen = []
        server = HTTPServer(("127.0.0.1", 0), SinkHandler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            url = "http://127.0.0.1:%d/api/v1/write" % server.server_address[1]
            api = FakeApi(good_script())
            out, err = io.StringIO(), io.StringIO()
            code = fe.main(env=dict(BASE_ENV, METRICS_URL=url, METRICS_USER=MUSER, METRICS_PASSWORD=MPASS),
                           fyta_urlopen=api, now=NOW, out=out, err=err)
        finally:
            server.shutdown()
            server.server_close()
        self.assertEqual(code, 0, err.getvalue())
        path, headers, body = SinkHandler.seen[0]
        self.assertEqual(path, "/api/v1/write")
        self.assertEqual(headers["Content-Encoding"], "snappy")
        self.assertEqual(headers["X-Prometheus-Remote-Write-Version"], "0.1.0")
        self.assertEqual(len(ref_decode(body)), len(series_for("1001")) + len(series_for("1002")))


# ------------------------------------------------ the files around the script

class Files(unittest.TestCase):
    ALLOWED_IMPORTS = {"base64", "json", "math", "os", "re", "struct", "sys", "time", "urllib", "datetime"}

    def test_the_script_uses_the_standard_library_only(self):
        imports = set(re.findall(r"^(?:import|from) (\w+)", (ROOT / "fyta_exporter.py").read_text(), re.M))
        self.assertTrue(imports)
        self.assertLessEqual(imports, self.ALLOWED_IMPORTS)

    def test_the_script_runs_no_other_program(self):
        text = (ROOT / "fyta_exporter.py").read_text()
        for word in ("subprocess", "os.system", "eval(", "exec("):
            self.assertNotIn(word, text)

    def test_the_script_never_prints_the_token_or_password(self):
        text = (ROOT / "fyta_exporter.py").read_text()
        for line in text.splitlines():
            if "print(" in line or "logging" in line:
                self.assertNotRegex(line, r"_token|\bpassword\b|cfg\[\"fyta_password\"\]|cfg\[\"token\"\]|_password")

    def test_fixtures_carry_no_personal_data(self):
        names = sorted(p.name for p in FIXTURES.glob("*.json"))
        self.assertEqual(len(names), 5)
        for name in names:
            text = (FIXTURES / name).read_text()
            self.assertNotIn("http", text)
            self.assertNotIn("@", text)
            self.assertNotIn("token", text.lower())
            for mac in re.findall(r"(?:[0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}", text):
                self.assertTrue(mac.startswith(("AA:00:00", "00:00:00")), mac)
            for number in re.findall(r"\"id\": (\d+)", text):
                self.assertIn(int(number), (1001, 1002, 1003, 7001, 9001), number)  # only the invented ids


if __name__ == "__main__":
    unittest.main()
