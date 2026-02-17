# Ceph RGW Multisite Sync Monitor

Bucket-level replication monitoring for Ceph RGW multisite clusters.

## Install

```bash
pip install .
```

Or from a wheel file (useful for offline/air-gapped environments):

```bash
pip install rgw_multisite_monitor-0.1.0-py3-none-any.whl
```

**Requirements:** Python 3.6+, `radosgw-admin` on PATH (comes with ceph-common).

## Quick Start

### Primary site (master zone)

```bash
# Step 1: Validate cluster access and generate config
rgw-monitor init

# Step 2: Start the dashboard
rgw-monitor start
```

Open `http://<node>:5000` — done.

### Secondary site (each secondary zone)

```bash
# One command — auto-detects zone name
rgw-monitor agent -u http://primary-node:5000
```

The primary dashboard will show the agent sync data within 60 seconds.

### Check everything is working

```bash
rgw-monitor status
```

```
  API:       ✓ reachable
  Ceph:      ✓
  Collector: ✓ running

  Zone Agents:
    us-west-2: ✓ active (last push: 23s ago, errors: 0)

  Buckets: 47 monitored
    SYNCED: 41  |  HIGH priority: 2
```

## Commands

| Command | Where to run | What it does |
|---|---|---|
| `rgw-monitor init` | Primary | Validates `radosgw-admin` access, generates `config.yaml` |
| `rgw-monitor start` | Primary | Starts dashboard + collector on port 5000 |
| `rgw-monitor agent -u URL` | Secondary | Pushes sync status to primary every 60s |
| `rgw-monitor test` | Primary | Runs all validation checks (topology, CLI, REST, parsers) |
| `rgw-monitor status` | Anywhere | Checks health of a running instance |

### Common options

```bash
# Custom config file
rgw-monitor start -c /etc/rgw-monitor/config.yaml

# Custom port
rgw-monitor start -p 8080

# Debug logging (shows every CLI command + parser output)
rgw-monitor start -d

# Agent: custom interval, explicit zone name
rgw-monitor agent -u http://primary:5000 -i 120 -z us-west-2

# Agent: single run for testing
rgw-monitor agent -u http://primary:5000 --once --verbose

# Agent: dry-run (prints payload, doesn't push)
rgw-monitor agent --dry-run
```

## Configuration

**No configuration is required for basic operation.** The tool auto-discovers everything from the cluster via `radosgw-admin`.

`rgw-monitor init` generates a `config.yaml` with sensible defaults. All fields are optional:

```yaml
# Collection interval (seconds)
collection_interval: 60

# REST API for secondary zone bucket stats (when CLI --rgw-zone doesn't work)
use_rest_for_bucket_stats: false
# access_key: "YOUR_ACCESS_KEY"
# secret_key: "YOUR_SECRET_KEY"

# SSL verification for REST API
verify_ssl: false

# API server bind settings
api_host: "0.0.0.0"
api_port: 5000
```

Config file is searched in this order:
1. `$RGW_MONITOR_CONFIG` environment variable
2. `./config.yaml` (current directory)
3. `~/.config/rgw-monitor/config.yaml`
4. `/etc/rgw-monitor/config.yaml`

## Running as systemd services

### Primary site

```ini
# /etc/systemd/system/rgw-monitor.service
[Unit]
Description=RGW Multisite Sync Monitor
After=ceph-radosgw.target network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/rgw-monitor start -c /etc/rgw-monitor/config.yaml
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### Secondary site

```ini
# /etc/systemd/system/rgw-monitor-agent.service
[Unit]
Description=RGW Multisite Zone Agent
After=ceph-radosgw.target network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/rgw-monitor agent -u http://primary-node:5000
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now rgw-monitor          # primary
sudo systemctl enable --now rgw-monitor-agent     # secondary
```

## Architecture

```
Secondary Zone                          Primary Zone
┌──────────────────────┐                ┌──────────────────────────────┐
│ rgw-monitor agent    │   POST /api/   │ rgw-monitor start            │
│                      │──────────────→ │                              │
│ • sync status        │  zone-agent/   │ • Dashboard (port 5000)      │
│ • sync error list    │    push        │ • Collector (bucket stats)   │
│ • bucket sync status │                │ • /metrics (Prometheus)      │
│   (per-bucket shards)│                │ • Zone agent receiver        │
└──────────────────────┘                └──────────────────────────────┘
```

**Why two components?** `sync status` and `sync error list` are only meaningful on the secondary zone (where pull-based replication runs). The primary only knows "I'm the master." The agent runs locally on each secondary, parses the output, and pushes structured JSON to the primary dashboard.

## Dashboard

Three tabs:

- **Overview** — zone cards with master stats, secondary zone sync progress (from both collector and agent), agent connection status
- **Bucket Sync** — per-bucket cards with object-count sync bars, shard-level sync from agent, search/filter/sort
- **Errors** — merged errors from primary + all zone agents, grouped by bucket and error code

Agent connection monitoring: when an agent stops pushing, the dashboard shows a red "AGENT LOST" badge and dims all stale agent data across every tab.

## API Reference

| Endpoint | Method | Description |
|---|---|---|
| `/api/health` | GET | Health check |
| `/api/dashboard` | GET | Full dashboard data |
| `/api/buckets` | GET | Bucket list sorted by sync progress |
| `/api/buckets/<name>` | GET | Bucket detail with history |
| `/api/errors` | GET | Global sync error list |
| `/api/zone-agent/push` | POST | Receive data from zone agent |
| `/api/zone-agents` | GET | Status of all connected agents |
| `/metrics` | GET | Prometheus metrics |

## Prometheus Metrics

The `/metrics` endpoint exposes per-bucket gauges:

```
rgw_multisite_bucket_sync_progress{bucket="my-bucket"} 94.2
rgw_multisite_bucket_delta_objects{bucket="my-bucket"} 1523
rgw_multisite_bucket_delta_bytes{bucket="my-bucket"} 6291456
rgw_multisite_bucket_errors{bucket="my-bucket"} 0
rgw_multisite_global_errors 3
```

Example Prometheus alert rule:

```yaml
groups:
  - name: rgw_multisite
    rules:
      - alert: RGWSyncLagging
        expr: rgw_multisite_bucket_sync_progress < 85
        for: 10m
        labels:
          severity: warning
```

## Troubleshooting

### rgw-monitor init says "radosgw-admin NOT found"

Install ceph-common: `yum install ceph-common` (RHEL) or `apt install ceph-common` (Debian).

### Agent shows "AGENT LOST" on dashboard

Check the agent is running and can reach the primary: `curl http://primary-node:5000/api/health` from the secondary node. If firewalled, open port 5000.

### Bucket sync shows "unknown" status

Ensure the agent runs with `--rgw-zone` (handled automatically by `rgw-monitor agent`). Verify manually: `radosgw-admin bucket sync status --bucket <name> --rgw-zone <zone>`.

### All buckets show 0% sync

The secondary zone may not be syncing. Check: `radosgw-admin sync status` on the secondary node. Common cause: RGW gateway not running on the secondary.
