#!/usr/bin/env python3
"""
Ceph RGW Multisite Monitor — API Server
========================================
Flask-based REST API that serves collected sync data to the dashboard.
Also provides SSE (Server-Sent Events) for real-time updates and
a Prometheus-compatible /metrics endpoint.
"""

import os
import sys
import json
import time
import yaml
import signal
import logging
import threading
from datetime import datetime, timezone

from flask import Flask, jsonify, request, Response, send_from_directory
from flask_cors import CORS

from collector import SyncCollector, SyncDataStore, CephAccessError, validate_ceph_access

# ------------------------------------------------------------------ #
#  Logging
# ------------------------------------------------------------------ #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("rgw-monitor")

# ------------------------------------------------------------------ #
#  Paths
# ------------------------------------------------------------------ #
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

# Dashboard dir: try ../dashboard first (dev layout), then ./dashboard (container)
DASHBOARD_DIR = os.path.join(PROJECT_DIR, "dashboard")
if not os.path.isdir(DASHBOARD_DIR):
    DASHBOARD_DIR = os.path.join(SCRIPT_DIR, "dashboard")
if not os.path.isdir(DASHBOARD_DIR):
    DASHBOARD_DIR = SCRIPT_DIR  # fallback: look for index.html alongside api_server.py

# ------------------------------------------------------------------ #
#  Flask App
# ------------------------------------------------------------------ #
app = Flask(__name__)
CORS(app)

store = SyncDataStore(max_snapshots=30)
collector = None
collector_thread = None


def load_config(path: str = None) -> dict:
    """
    Load configuration from YAML file or environment variables.

    The collector runs via radosgw-admin CLI on the local host — no
    endpoint/credentials are required for the core functionality.

    Optional config:
      - use_rest_for_bucket_stats: true  (query zone endpoints for bucket stats)
      - access_key / secret_key          (needed only if REST mode is enabled)
      - collection_interval              (seconds between collection cycles)
      - verify_ssl                       (for REST API calls)
    """
    config = {}

    # Try loading from file first
    config_path = path or os.environ.get("RGW_MONITOR_CONFIG", "config.yaml")
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            config = yaml.safe_load(f) or {}
        logger.info("Loaded config from %s", config_path)

    # Environment overrides
    env_map = {
        "RGW_ACCESS_KEY": "access_key",
        "RGW_SECRET_KEY": "secret_key",
        "RGW_USE_REST_BUCKET_STATS": "use_rest_for_bucket_stats",
        "RGW_VERIFY_SSL": "verify_ssl",
        "RGW_COLLECTION_INTERVAL": "collection_interval",
    }
    for env_key, config_key in env_map.items():
        val = os.environ.get(env_key)
        if val is not None:
            if config_key in ("use_rest_for_bucket_stats", "verify_ssl"):
                config[config_key] = val.lower() in ("true", "1", "yes")
            elif config_key == "collection_interval":
                config[config_key] = int(val)
            else:
                config[config_key] = val

    return config


# ================================================================== #
#  Dashboard (serves the UI)
# ================================================================== #

@app.route("/")
def serve_dashboard():
    """Serve the single-page dashboard HTML."""
    index_path = os.path.join(DASHBOARD_DIR, "index.html")
    if os.path.exists(index_path):
        logger.info("Serving dashboard from: %s", DASHBOARD_DIR)
        return send_from_directory(DASHBOARD_DIR, "index.html")
    else:
        return (
            "<h2>Dashboard not built yet</h2>"
            "<p>Run <code>python3 dashboard/build_html.py</code> first, "
            "or use <code>./setup.sh</code> which does it automatically.</p>"
            "<p>Looked in: <code>{}</code></p>"
            "<hr><p>API is running — try <a href='/api/health'>/api/health</a></p>"
            .format(DASHBOARD_DIR)
        ), 404


# ================================================================== #
#  REST API Endpoints
# ================================================================== #

@app.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint — includes Ceph access status."""
    ceph_ok = False
    ceph_error = None
    try:
        validate_ceph_access()
        ceph_ok = True
    except CephAccessError as exc:
        ceph_error = str(exc)

    return jsonify({
        "status": "ok" if ceph_ok else "degraded",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "collector_running": collector_thread is not None and collector_thread.is_alive(),
        "ceph_access": ceph_ok,
        "ceph_error": ceph_error,
    })


@app.route("/api/topology", methods=["GET"])
def topology():
    """Return discovered multisite topology."""
    data = store.get_dashboard_data()
    return jsonify(data.get("topology", {}))


@app.route("/api/dashboard", methods=["GET"])
def dashboard():
    """
    Main dashboard endpoint — returns all data needed to render the UI.
    Supports optional ?last_n=N to limit history snapshots returned.
    """
    data = store.get_dashboard_data()
    last_n = request.args.get("last_n", type=int, default=0)

    if last_n > 0:
        for bucket_name in data.get("buckets", {}):
            history = data["buckets"][bucket_name].get("history", [])
            data["buckets"][bucket_name]["history"] = history[-last_n:]
        data["global_sync"] = data.get("global_sync", [])[-last_n:]

    return jsonify(data)


@app.route("/api/buckets", methods=["GET"])
def bucket_list():
    """List all monitored buckets with their latest status."""
    data = store.get_dashboard_data()
    buckets = []
    for name, info in data.get("buckets", {}).items():
        history = info.get("history", [])
        latest = history[-1] if history else {}
        errors = info.get("errors", [])

        buckets.append({
            "name": name,
            "sync_progress_pct": latest.get("sync_progress_pct", 0),
            "delta_objects": latest.get("delta_objects", 0),
            "delta_size": latest.get("delta_size", 0),
            "error_count": len(errors),
            "snapshot_count": len(history),
            "last_update": latest.get("timestamp", ""),
        })

    # Sort by sync progress (worst first)
    buckets.sort(key=lambda b: b["sync_progress_pct"])
    return jsonify(buckets)


@app.route("/api/buckets/<bucket_name>", methods=["GET"])
def bucket_detail(bucket_name: str):
    """Get detailed history and errors for a specific bucket."""
    data = store.get_dashboard_data()
    bucket = data.get("buckets", {}).get(bucket_name)
    if not bucket:
        return jsonify({"error": f"Bucket '{bucket_name}' not found"}), 404
    return jsonify({
        "name": bucket_name,
        "history": bucket.get("history", []),
        "errors": bucket.get("errors", []),
    })


@app.route("/api/errors", methods=["GET"])
def global_errors():
    """Get global sync error list."""
    data = store.get_dashboard_data()
    return jsonify({
        "errors": data.get("global_errors", []),
        "total": len(data.get("global_errors", [])),
    })


@app.route("/api/errors/<bucket_name>", methods=["GET"])
def bucket_errors(bucket_name: str):
    """Get sync errors for a specific bucket."""
    data = store.get_dashboard_data()
    bucket = data.get("buckets", {}).get(bucket_name)
    if not bucket:
        return jsonify({"error": f"Bucket '{bucket_name}' not found"}), 404
    return jsonify({
        "bucket": bucket_name,
        "errors": bucket.get("errors", []),
    })


@app.route("/api/sync-status", methods=["GET"])
def sync_status():
    """Get global sync status history."""
    data = store.get_dashboard_data()
    return jsonify(data.get("global_sync", []))


# ================================================================== #
#  Zone Agent API — receives data from secondary zone agents
# ================================================================== #

@app.route("/api/zone-agent/push", methods=["POST"])
def zone_agent_push():
    """
    Receive processed sync data from a secondary zone agent.

    Expected payload:
    {
      "zone_name": "us-west-2",
      "timestamp": "2025-...",
      "agent_version": "1.0",
      "sync_status": { ... parsed sync status ... },
      "sync_errors": [ ... parsed error list ... ],
      "bucket_sync_status": { "bucket1": { ... }, ... }
    }
    """
    payload = request.get_json()
    if not payload:
        return jsonify({"error": "Invalid JSON payload"}), 400

    zone_name = payload.get("zone_name", "")
    if not zone_name:
        return jsonify({"error": "zone_name is required"}), 400

    # Validate zone exists in topology (if topology is available)
    topo = store.topology
    if topo and topo.get("zones"):
        known_zones = [z.get("name") for z in topo["zones"]]
        if zone_name not in known_zones:
            logger.warning("Zone agent push from unknown zone '%s' — "
                           "known zones: %s", zone_name, known_zones)
            # Accept anyway — zone might not be in topology yet
            # but log a warning

    # Validate payload structure
    if not payload.get("sync_status") and not payload.get("sync_errors"):
        return jsonify({"error": "Payload must contain sync_status or sync_errors"}), 400

    # Store the data
    store.update_zone_agent(zone_name, payload)

    # Merge agent sync errors into global errors (append zone context)
    agent_errors = payload.get("sync_errors", [])
    if agent_errors:
        # Tag errors with the agent zone as source
        for err in agent_errors:
            err["_agent_zone"] = zone_name
            err["_source"] = "zone_agent"

    logger.info("Zone agent push from '%s': sync_status=%s, errors=%d, bucket_sync=%d",
                zone_name,
                payload.get("sync_status", {}).get("status", "?"),
                len(agent_errors),
                len(payload.get("bucket_sync_status", {})))

    return jsonify({
        "status": "ok",
        "zone_name": zone_name,
        "received_at": datetime.now(timezone.utc).isoformat(),
        "errors_count": len(agent_errors),
        "bucket_sync_count": len(payload.get("bucket_sync_status", {})),
    })


@app.route("/api/zone-agents", methods=["GET"])
def zone_agent_status():
    """Get status of all connected zone agents."""
    data = store.get_dashboard_data()
    agents = data.get("zone_agents", {})
    result = {}
    now = datetime.now(timezone.utc)
    for zone_name, info in agents.items():
        ts = info.get("timestamp", "")
        age_seconds = None
        if ts:
            try:
                agent_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                age_seconds = (now - agent_time).total_seconds()
            except (ValueError, TypeError):
                pass
        result[zone_name] = {
            "last_push": ts,
            "age_seconds": age_seconds,
            "stale": age_seconds is not None and age_seconds > 300,
            "error_count": len(info.get("sync_errors", [])),
            "bucket_count": len(info.get("bucket_sync_status", {})),
            "agent_version": info.get("agent_version", ""),
        }
    return jsonify(result)


# ================================================================== #
#  Server-Sent Events for real-time updates
# ================================================================== #

@app.route("/api/events", methods=["GET"])
def sse_events():
    """SSE endpoint for real-time dashboard updates."""
    def generate():
        last_ts = ""
        while True:
            data = store.get_dashboard_data()
            current_ts = data.get("last_update", "")
            if current_ts != last_ts:
                yield f"data: {json.dumps(data)}\n\n"
                last_ts = current_ts
            time.sleep(5)

    return Response(
        generate(),
        content_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )


# ================================================================== #
#  Prometheus Metrics
# ================================================================== #

@app.route("/metrics", methods=["GET"])
def prometheus_metrics():
    """Prometheus-compatible metrics endpoint."""
    data = store.get_dashboard_data()
    lines = []
    lines.append("# HELP rgw_multisite_bucket_sync_progress Sync progress percentage per bucket")
    lines.append("# TYPE rgw_multisite_bucket_sync_progress gauge")

    lines.append("# HELP rgw_multisite_bucket_delta_objects Object count delta per bucket")
    lines.append("# TYPE rgw_multisite_bucket_delta_objects gauge")

    lines.append("# HELP rgw_multisite_bucket_delta_bytes Size delta in bytes per bucket")
    lines.append("# TYPE rgw_multisite_bucket_delta_bytes gauge")

    lines.append("# HELP rgw_multisite_bucket_errors Active error count per bucket")
    lines.append("# TYPE rgw_multisite_bucket_errors gauge")

    lines.append("# HELP rgw_multisite_global_errors Total global sync errors")
    lines.append("# TYPE rgw_multisite_global_errors gauge")

    for bucket_name, info in data.get("buckets", {}).items():
        history = info.get("history", [])
        latest = history[-1] if history else {}
        errors = info.get("errors", [])

        progress = latest.get("sync_progress_pct", 0)
        delta_obj = latest.get("delta_objects", 0)
        delta_size = latest.get("delta_size", 0)

        safe_name = bucket_name.replace('"', '\\"')
        lines.append(f'rgw_multisite_bucket_sync_progress{{bucket="{safe_name}"}} {progress}')
        lines.append(f'rgw_multisite_bucket_delta_objects{{bucket="{safe_name}"}} {delta_obj}')
        lines.append(f'rgw_multisite_bucket_delta_bytes{{bucket="{safe_name}"}} {delta_size}')
        lines.append(f'rgw_multisite_bucket_errors{{bucket="{safe_name}"}} {len(errors)}')

    global_err_count = len(data.get("global_errors", []))
    lines.append(f"rgw_multisite_global_errors {global_err_count}")

    return Response("\n".join(lines) + "\n", content_type="text/plain; charset=utf-8")


# ================================================================== #
#  Configuration API (for initial setup from dashboard)
# ================================================================== #

@app.route("/api/config", methods=["POST"])
def update_config():
    """
    Accept configuration and (re)start collector.

    No fields are strictly required since the collector uses local
    radosgw-admin CLI. Optional fields:
      - use_rest_for_bucket_stats: bool
      - access_key / secret_key: for REST bucket stats
      - collection_interval: int (seconds)
      - verify_ssl: bool
    """
    global collector, collector_thread

    payload = request.json
    if not payload:
        return jsonify({"error": "No configuration provided"}), 400

    # If REST mode requested, access_key and secret_key are required
    if payload.get("use_rest_for_bucket_stats"):
        missing = [k for k in ("access_key", "secret_key") if not payload.get(k)]
        if missing:
            return jsonify({
                "error": f"REST bucket stats mode requires: {missing}"
            }), 400

    # Stop existing collector if running
    if collector:
        collector.stop()
        if collector_thread:
            collector_thread.join(timeout=10)

    # Save config
    config_path = os.environ.get("RGW_MONITOR_CONFIG", "config.yaml")
    with open(config_path, "w") as f:
        yaml.dump(payload, f, default_flow_style=False)
    logger.info("Configuration saved to %s", config_path)

    # Start new collector
    try:
        collector = SyncCollector(payload, store)
        collector.initialize()
        interval = payload.get("collection_interval", 60)
        collector_thread = threading.Thread(
            target=collector.run, args=(interval,), daemon=True
        )
        collector_thread.start()
        return jsonify({
            "status": "ok",
            "message": "Collector started successfully",
            "topology": store.get_dashboard_data().get("topology", {}),
        })
    except CephAccessError as exc:
        logger.error("Ceph access check failed: %s", exc)
        return jsonify({"error": str(exc)}), 500
    except Exception as exc:
        logger.exception("Failed to start collector")
        return jsonify({"error": str(exc)}), 500


@app.route("/api/config", methods=["GET"])
def get_config():
    """Return current config (with secrets masked)."""
    config = load_config()
    if config.get("secret_key"):
        config["secret_key"] = "***" + config["secret_key"][-4:]
    if config.get("access_key"):
        config["access_key"] = config["access_key"][:4] + "***"
    config["cli_mode"] = True  # Always true — CLI is the default
    config["use_rest_for_bucket_stats"] = config.get("use_rest_for_bucket_stats", False)
    return jsonify(config)


# ================================================================== #
#  Manual collection trigger
# ================================================================== #

@app.route("/api/collect", methods=["POST"])
def trigger_collection():
    """Manually trigger a collection cycle."""
    if collector:
        threading.Thread(target=collector.collect_once, daemon=True).start()
        return jsonify({"status": "ok", "message": "Collection triggered"})
    return jsonify({"error": "Collector not initialized"}), 503


# ================================================================== #
#  Startup
# ================================================================== #

def start_collector_from_config():
    """
    Try to start collector at startup.
    The collector uses local radosgw-admin CLI, so it will validate
    Ceph access and fail fast if the binary is missing or the cluster
    is unreachable.
    """
    global collector, collector_thread

    config = load_config()

    try:
        collector = SyncCollector(config, store)
        collector.initialize()
        interval = config.get("collection_interval", 60)
        collector_thread = threading.Thread(
            target=collector.run, args=(interval,), daemon=True
        )
        collector_thread.start()
        logger.info("Collector started successfully.")
    except CephAccessError as exc:
        logger.error("=" * 60)
        logger.error("CEPH ACCESS CHECK FAILED — COLLECTOR NOT STARTED")
        logger.error("=" * 60)
        logger.error("%s", exc)
        logger.error("=" * 60)
        logger.error("The API server will still run (serving the dashboard), "
                     "but no data will be collected until Ceph access is fixed.")
        logger.error("You can also POST to /api/config to retry after fixing "
                     "the issue.")
    except Exception:
        logger.exception("Failed to start collector. The API server will "
                         "still run — fix the issue and POST to /api/config.")


def shutdown_handler(signum, frame):
    logger.info("Shutting down...")
    if collector:
        collector.stop()
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="RGW Multisite Monitor API Server")
    parser.add_argument("--config", "-c", default=None,
                        help="Path to config.yaml (default: auto-detect)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable verbose logging (INFO level for all components)")
    parser.add_argument("--debug", "-d", action="store_true",
                        help="Enable debug logging (DEBUG level — shows CLI commands, "
                             "parser output, method selection decisions)")
    parser.add_argument("--port", "-p", type=int, default=None,
                        help="API server port (default: 5000 or from config)")
    parser.add_argument("--host", default=None,
                        help="API server bind host (default: 0.0.0.0)")
    args = parser.parse_args()

    # Set log level based on flags
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        # Also set collector logger explicitly
        logging.getLogger("collector").setLevel(logging.DEBUG)
        logger.info("Debug logging enabled — all CLI commands and parser results will be shown")
    elif args.verbose:
        logging.getLogger().setLevel(logging.INFO)
        logger.info("Verbose logging enabled")

    # Load config with optional path override
    if args.config:
        os.environ["RGW_MONITOR_CONFIG"] = args.config

    start_collector_from_config()

    port = args.port or int(os.environ.get("RGW_MONITOR_PORT", 5500))
    host = args.host or os.environ.get("RGW_MONITOR_HOST", "0.0.0.0")

    logger.info("Starting API server on %s:%d", host, port)
    app.run(host=host, port=port, debug=False, threaded=True)