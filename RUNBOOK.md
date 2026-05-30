# Wavicle Runbook (Phase 2 Production)

## Overview
Wavicle is a proof-based caching engine that sits in front of a primary database (PostgreSQL) and automatically invalidates cache entries using logical replication. It implements the standard RESP3 protocol, allowing it to act as a drop-in replacement for Redis in many read-heavy caching scenarios.

## Deployment & Configuration

### Docker Compose (Standard Deployment)
The primary deployment method is via Docker Compose, which brings up both Wavicle and the PostgreSQL database.
```bash
docker-compose up --build -d
```

### Environment Variables
Wavicle is configured entirely via environment variables (which override `config.yaml`):
*   `WAVICLE_SERVER_LISTEN`: Address to listen on (default `:6379`).
*   `WAVICLE_MAX_CONNS`: Maximum concurrent RESP3 connections (default `10000`).
*   `WAVICLE_DB_TYPE`: `postgres`, `mysql`, or `crystal`.
*   `WAVICLE_DB_DSN`: Database connection string.
*   `WAVICLE_AUTH_PASSWORD`: RESP3 authentication password (Required for Phase 2).
*   `WAVICLE_METRICS_ENABLED`: Set to `true` to enable the Prometheus metrics server.
*   `WAVICLE_METRICS_LISTEN`: Address for metrics server (default `:8080`).

## Monitoring & Telemetry

### Prometheus Metrics
Exposed at `http://<host>:8080/metrics`. All metrics use the `wavicle_` prefix.

**Key metrics to alert on:**
1.  **`wavicle_replication_lag_ms`**: Alert if > 1000ms for sustained periods (e.g., 5m). High lag means the cache is returning stale data. Check PostgreSQL `pg_stat_replication` and network latency between DB and Wavicle.
2.  **`wavicle_errors_total`**: Alert if increasing rapidly. Check server logs for `[replication]` disconnections or storage I/O errors.
3.  **`wavicle_active_connections`**: Alert if approaching `WAVICLE_MAX_CONNS`. Clients will receive `-ERR max number of clients reached`. Check for client connection leaks.
4.  **`wavicle_wal_size_bytes`**: Should remain under ~50MB. If it grows unboundedly, WAL compaction is failing.

## Maintenance Operations

### WAL Compaction
Wavicle automatically compacts its Write-Ahead Log (`crystal.log`) in the background when it exceeds 50MB. This is a non-blocking process and requires no manual intervention. You can monitor compactions via the `wavicle_compactions_total` metric.

### Graceful Shutdown
Wavicle responds to `SIGINT` and `SIGTERM` signals (sent by `docker stop` or `kubectl scale`). The shutdown sequence:
1.  Stops accepting new RESP3 connections.
2.  Stops the PostgreSQL logical replication listener.
3.  Waits up to 10 seconds for all active connections to drain naturally.
4.  Performs a final `fsync` on the WAL and closes cleanly.

To trigger a manual graceful shutdown with a 15-second timeout:
```bash
docker-compose stop -t 15 wavicle
```

## Troubleshooting Guide

### Issue: Clients getting `NOAUTH Authentication required.`
*   **Cause**: The client is not sending the `AUTH <password>` command upon connecting.
*   **Fix**: Update the client application to authenticate. Ensure `WAVICLE_AUTH_PASSWORD` matches between the server env and the client config.

### Issue: Cache returning stale data / Not updating after DB write
1.  Check `wavicle_replication_lag_ms` in Prometheus. If it's high, the replication listener is lagging behind the DB.
2.  Check Wavicle logs for `PostgreSQL connection error` or `[replication]` reconnect loops.
3.  In PostgreSQL, run `SELECT * FROM pg_replication_slots;`. Ensure the slot `wavicle_slot` exists and `active` is true. If false, Wavicle is disconnected.

### Issue: "max number of clients reached"
1.  The server has hit its `WAVICLE_MAX_CONNS` limit.
2.  Investigate client applications for connection leaks (not closing connections).
3.  If the load is legitimate, increase `WAVICLE_MAX_CONNS` and restart the container.

### Issue: PostgreSQL UPSERT / `invalid path format` errors
1.  Wavicle Phase 1 maps RESP3 keys directly to database rows using a rigid format: `table:id:column`.
2.  Attempting to `SET` a key like `my_cache_key` will fail because Wavicle cannot deduce the target Postgres table. Ensure clients strictly follow the schema pattern.
