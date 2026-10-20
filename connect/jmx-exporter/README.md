# JMX exporter

Debezium metrics (`MilliSecondsBehindSource`, `QueueRemainingCapacity`,
`TotalNumberOfEventsSeen`, ...) are only exposed over JMX. The Prometheus JMX
exporter javaagent turns them into a `/metrics` endpoint on port 9404.

## Enable

1. Download the pinned jar into this directory (it is git-ignored):

   ```powershell
   .\dev.ps1 jmx-download
   ```

   or `make jmx-download`. It fetches
   `io.prometheus.jmx:jmx_prometheus_javaagent:1.0.1` from Maven Central as
   `jmx_prometheus_javaagent.jar`.

2. In `.env`, set:

   ```dotenv
   CONNECT_KAFKA_OPTS=-javaagent:/kafka/jmx-exporter/jmx_prometheus_javaagent.jar=9404:/kafka/jmx-exporter/config.yml
   ```

3. Recreate Kafka Connect and start the monitoring profile:

   ```powershell
   docker compose up -d --force-recreate kafka-connect
   docker compose --profile monitoring up -d
   ```

Check: `curl http://localhost:9404/metrics | grep MilliSecondsBehindSource`.

Leave `CONNECT_KAFKA_OPTS` empty when the jar is absent; otherwise the JVM
refuses to start.

## Files

- `config.yml`: exporter rules (Debezium connector metrics, task status,
  source-task and producer metrics).
