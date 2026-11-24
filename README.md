# fyta-exporter

A small tool that reads your [FYTA](https://fyta.de) plant sensors from the FYTA cloud and pushes
their values to Prometheus. It is one Python file and uses only the Python standard library.

It reads the latest values of one or more plants (soil moisture, soil salinity, light, temperature,
battery and more), turns them into Prometheus metrics and sends them with `remote_write`. Then it
exits. Run it every 15 minutes from a Kubernetes CronJob, from cron, or by hand.

## Why push, and why a CronJob

FYTA has no Prometheus endpoint, and its sensors report only every 15 to 30 minutes, so an exporter
that Prometheus scrapes every 15 seconds would mostly repeat old values. A job that runs every 15
minutes, reads the plants once and pushes the result is simpler:

- There is nothing to keep running, and no port to open.
- A failed run is a failed Job, so Kubernetes and your alerts show it.
- The FYTA hub uploads new readings about every 15 to 30 minutes, so polling more often adds nothing.

## Quick start

You need a FYTA account with at least one plant that has a sensor, and a place to push to. To try it
without a receiver, use `DRY_RUN=1`: it reads your plants, prints the series and sends nothing.

With Python 3.9 or newer:

```bash
FYTA_EMAIL=you@example.com FYTA_PASSWORD='your-password' DRY_RUN=1 python3 fyta_exporter.py
```

With Docker (build it yourself, or use the image from the releases):

```bash
docker build -t fyta-exporter .
docker run --rm -e FYTA_EMAIL -e FYTA_PASSWORD -e DRY_RUN=1 fyta-exporter
```

To push, set `METRICS_URL` to a Prometheus remote_write receiver and drop `DRY_RUN`:

```bash
METRICS_URL=http://localhost:9090/api/v1/write python3 fyta_exporter.py
```

## Configuration

Everything is configured by environment variables. Nothing else is read.

| Variable | Required | Meaning |
|---|---|---|
| `FYTA_EMAIL`, `FYTA_PASSWORD` | yes, unless `FYTA_TOKEN` is set | FYTA account login. The tool logs in on every run |
| `FYTA_TOKEN` | no | An existing FYTA bearer token. Skips the login. Useful for local tests. If FYTA rejects it and no login is set, the run fails |
| `FYTA_PLANT_IDS` | no | Comma-separated plant ids, for example `1001,1002`. If unset, every plant of the account that has a sensor is read |
| `METRICS_URL` | yes, unless `DRY_RUN` | The remote_write endpoint. Must be `http` or `https`, with no login inside the URL |
| `METRICS_USER`, `METRICS_PASSWORD` | no | Basic auth for the receiver. Refused over plain `http`, except to `localhost` or `127.0.0.1` |
| `INSTANCE_LABEL` | no | Value of the `instance` label. Default `fyta` |
| `JOB_LABEL` | no | Value of the `job` label. Default `fyta` |
| `DRY_RUN` | no | `1` builds the payload, decodes it back, prints the series and sends nothing |
| `FYTA_BASE_URL` | no | The API base. Default `https://web.fyta.de/api`. For tests and proxies |

Exit codes:

| Code | Meaning |
|---|---|
| `0` | Every plant was read and the series reached the receiver |
| `1` | A FYTA call failed, a plant came back with no readings, or the receiver refused the data |
| `2` | The configuration is wrong. Nothing was read or sent |

When FYTA fails for a plant, the tool still pushes `fyta_scrape_success 0` for that plant (and the
good series of the other plants), then exits `1`. If the failure flag itself cannot be pushed, the
exit code is still `1`.

The FYTA password, the token and the receiver password are never printed or logged, not even in
part. Errors are reported by HTTP status or exception type only.

## Metrics

Every series carries `job`, `instance` and `plant_id`. When FYTA gives the plant a nickname, it also
carries `plant` (control characters removed, whitespace collapsed, at most 64 characters). The failure flag
of a plant whose name is not known yet may lack the `plant` label.

| Metric | Unit | Meaning |
|---|---|---|
| `fyta_soil_moisture_percent` | percent | Soil moisture |
| `fyta_temperature_celsius` | degrees Celsius | Temperature at the sensor |
| `fyta_light_umol` | umol (FYTA's light unit) | Light at the sensor |
| `fyta_soil_ec_ms_per_cm` | mS/cm | Soil salinity (EC) |
| `fyta_battery_percent` | percent | Sensor battery |
| `fyta_dli_mol_per_day` | mol/day | Daily light integral of the last full UTC day |
| `fyta_dli_timestamp_seconds` | unix seconds | The day that DLI belongs to |
| `fyta_last_data_timestamp_seconds` | unix seconds | Time of the newest reading FYTA has |
| `fyta_last_data_age_seconds` | seconds | Age of that reading **at push time**. It does not grow between pushes |
| `fyta_measure_status` | 2 low, 3 good | FYTA's verdict per measure. Label `measure`: `moisture`, `temperature`, `light`, `salinity` |
| `fyta_measure_good_min`, `fyta_measure_good_max` | as the measure | FYTA's good range for the plant. Label `measure` as above |
| `fyta_scrape_success` | 1 or 0 | 1 when the plant was read, 0 when it failed |
| `fyta_last_success_timestamp_seconds` | unix seconds | When the plant was last read successfully |

Each value is the newest non-empty value of the last week of readings. A measure that is empty in
every row (the FYTA Beam sensor does not report air humidity) is left out, never sent as 0. Salinity
is read only now and then, so its value can be a few hours older than the moisture.

### Pushed series have no stale markers

Every sample is stamped with the push time. Prometheus writes no stale marker for a pushed series,
so after the last push a plain query like `fyta_soil_moisture_percent` returns the value for 5
minutes and then nothing. A stopped job then looks like missing data. In dashboards and alerts,
always write the metric inside `last_over_time`:

```promql
last_over_time(fyta_soil_moisture_percent[30m])
```

The FYTA hub uploads about every 15 to 30 minutes, so a 30 minute window is the smallest that is
safe. To see how old the newest reading is right now, use
`time() - last_over_time(fyta_last_data_timestamp_seconds[30m])`.

## Kubernetes

`deploy/kubernetes/cronjob.yaml` is a generic example: it runs every 15 minutes, never twice at the
same time (`concurrencyPolicy: Forbid`), stops after 90 seconds, runs as user 65534 with a
read-only root filesystem and no capabilities.

The receiver must be Prometheus started with `--web.enable-remote-write-receiver`, or another
service that accepts Prometheus remote_write. Point `METRICS_URL` at
`http://<prometheus-service>.<namespace>.svc:9090/api/v1/write`.

1. Create the Secrets. Use the namespace where the CronJob will run. Put the values in your own shell,
   not in a file in git:

   ```bash
   kubectl create secret generic fyta-credentials \
     --from-literal=email='<fyta-email>' \
     --from-literal=password='<fyta-password>'

   kubectl create secret generic fyta-exporter-push \
     --from-literal=url='http://<prometheus-service>.<namespace>.svc:9090/api/v1/write'
   # Add --from-literal=user=... and --from-literal=password=... when the receiver wants Basic auth.
   ```

2. Edit the namespace, and the image tag if you want to pin one, then apply:

   ```bash
   kubectl apply -f deploy/kubernetes/cronjob.yaml
   ```

3. Run one Job now, instead of waiting up to 15 minutes, and read its log:

   ```bash
   kubectl create job --from=cronjob/fyta-exporter fyta-exporter-manual
   kubectl logs job/fyta-exporter-manual
   kubectl delete job fyta-exporter-manual
   ```

   A good run prints `pushed <n> series to <host>`.

4. Query it in Prometheus or Grafana, for example:

   ```promql
   last_over_time(fyta_soil_moisture_percent[30m])
   last_over_time(fyta_battery_percent[30m])
   ```

The image is published to `ghcr.io/vinitu/fyta-exporter` (and to Docker Hub as `<user>/fyta-exporter` when the `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` repository secrets are set) (`linux/amd64` and `linux/arm64`) when a
`v*` tag is pushed. If you fork the repository, the release workflow publishes to your own
`ghcr.io/<owner>/<repository>`, and the image name in the CronJob has to follow.

## Alerts and dashboards

A low-moisture alert, using FYTA's own good range for the plant. It fires when the soil has been
below the range for an hour:

```promql
last_over_time(fyta_soil_moisture_percent[30m])
  < on(job, instance, plant_id)
last_over_time(fyta_measure_good_min{measure="moisture"}[30m])
```

Use `for: 1h`. If you prefer a fixed number, write `last_over_time(fyta_soil_moisture_percent[30m]) < 30`.

Other useful rules:

```promql
# The sensor has not sent anything for more than 2 hours (hub offline, battery flat)
time() - last_over_time(fyta_last_data_timestamp_seconds[3h]) > 7200

# The job could not read the plant
last_over_time(fyta_scrape_success[30m]) == 0

# The job has not succeeded for an hour (it stopped, or every run fails)
time() - last_over_time(fyta_last_success_timestamp_seconds[3h]) > 3600

# Battery low
last_over_time(fyta_battery_percent[30m]) < 20
```

For a graph, plot `fyta_soil_moisture_percent` against `fyta_measure_good_min` and
`fyta_measure_good_max` with `measure="moisture"`, all inside `last_over_time`.

## Limitations

- **FYTA must upload to its cloud.** The tool reads what the FYTA hub or the FYTA app has already
  uploaded. With no hub in range and no app near the sensor, the readings do not move and
  `fyta_last_data_age_seconds` grows.
- **It uses FYTA's public API without being official.** This is an independent project, not made,
  endorsed or supported by FYTA. Read FYTA's own page about the API,
  <https://fyta-io.notion.site/FYTA-Public-API-d2f4c30306f74504924c9a40402a3afd>. The terms and
  limits that apply are FYTA's, and the API can change without notice.
- **The login is a plain account password.** Use a Kubernetes Secret or a secret manager, and do
  not put the password into a file in git.
- **Remote write 1.0 only.** The body is a Prometheus `WriteRequest` in a snappy block that has
  literal elements only (no compression, valid for every snappy decoder). It was checked against
  Grafana Alloy's `prometheus.receive_http`, which accepted it and forwarded it to a decoding sink.
  It was not tried against every receiver.
- **One sample per series per run.** There is no back-filling: the history between two runs is
  not sent.

## Tests

The tests are offline and need nothing but Python 3.9 or newer. From the repository root:

```bash
python3 -m unittest discover -s tests
```

CI runs them on Python 3.9 and 3.13.

## License

MIT. See [LICENSE](LICENSE).
