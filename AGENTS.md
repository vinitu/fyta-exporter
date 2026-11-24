# AGENTS.md

A guide for people and coding agents who work on this repository.

## What this is

`fyta-exporter` reads FYTA plant sensors once and pushes the values as Prometheus metrics with
`remote_write`, then exits. It is meant to run as a Kubernetes CronJob and also from any shell.

## Layout

| Path | What it is |
|---|---|
| `fyta_exporter.py` | The whole tool, one file, Python standard library only |
| `tests/test_exporter.py` | Offline unit tests (`unittest`) |
| `tests/fixtures/` | Masked FYTA answers with invented ids and names |
| `Dockerfile`, `.dockerignore` | The container image (`python:3.13-slim`, user 65534, no pip) |
| `deploy/kubernetes/cronjob.yaml` | A generic example CronJob |
| `.github/workflows/ci.yml` | Runs the tests on Python 3.9 and 3.13 |
| `.github/workflows/release.yml` | On a `v*` tag, builds and pushes the image to ghcr.io |

## How to test

From the repository root:

```bash
python3 -m unittest discover -s tests
```

The tests are offline. They use fake HTTP functions, plus one real POST to a sink on `127.0.0.1`.
The payload is decoded with a decoder that lives in the tests and shares no code with the script,
so a mistake in the encoder cannot hide behind a matching mistake in the decoder.

## Rules

- **Standard library only.** No pip packages, in the code, the tests or the image. A test checks
  the imports.
- **Python 3.9 compatible.** No `match`, no `X | Y` types, no newer standard library calls.
- **Never print or log a credential.** That means the FYTA password, the token, the receiver
  password and the Basic auth header, not even in part. Report errors by HTTP status or exception
  type only. Tests check every output path for leaks.
- **No private data in the repository.** Fixtures use invented ids and names. Do not paste a real
  FYTA answer, plant id, MAC address, e-mail address or host name into code, tests, docs or
  workflows.
- **Configuration is environment only.** The list is in `fyta_exporter.py` (module docstring) and in
  the README. Change both together when a variable is added.
- **Never follow a redirect** when a request carries an `Authorization` header.
- A change to the metric names or labels is a breaking change for dashboards: say so in the README.
- Keep the workflows small. Every CI minute counts.
