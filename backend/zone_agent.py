#!/usr/bin/env python3
"""
RGW Multisite Monitor — Secondary Zone Agent
==============================================
Lightweight agent that runs on a secondary (non-master) RGW zone.

Collects sync data that is ONLY meaningful from the secondary's perspective:
  - radosgw-admin sync status          → parsed global sync state
  - radosgw-admin sync error list      → parsed error list
  - radosgw-admin bucket sync status   → parsed per-bucket shard-level sync

Pushes processed (not raw) data to the primary site's dashboard API.

Usage:
  python3 zone_agent.py --primary-url http://primary-node:5000

  python3 zone_agent.py \\
    --primary-url http://primary-node:5000 \\
    --interval 60 \\
    --zone my-zone-name \\
    --debug

  python3 zone_agent.py --config agent.yaml

Config file (agent.yaml):
  primary_url: http://primary-node:5000
  push_interval: 60
  zone_name: ""        # empty = auto-detect from sync status
  max_buckets: 500     # max buckets to collect sync status for

Requirements:
  - radosgw-admin on PATH
  - Ceph cluster access from this node
  - Network access to the primary dashboard API
  - Python 3.6+ (no extra pip packages needed)
"""

import argparse
import json
import logging
import os
import re
import signal
import subprocess
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ------------------------------------------------------------------ #
#  Logging
# ------------------------------------------------------------------ #
logger = logging.getLogger("zone-agent")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S"
))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


# ------------------------------------------------------------------ #
#  CLI Runners (self-contained — no dependency on collector.py)
# ------------------------------------------------------------------ #

def run_cli_json(args, timeout=60):
    """Run radosgw-admin with JSON output. Returns parsed dict/list or error dict."""
    cmd = ["radosgw-admin"] + args + ["--format=json"]
    logger.debug("CLI-JSON: %s", " ".join(cmd))
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"_error": True, "error": "timed out", "cmd": " ".join(args)}
    except Exception as exc:
        return {"_error": True, "error": str(exc), "cmd": " ".join(args)}

    if proc.returncode != 0:
        return {"_error": True, "error": proc.stderr.strip(), "rc": proc.returncode}

    output = proc.stdout.strip()
    json_start = -1
    for i, ch in enumerate(output):
        if ch in ('{', '['):
            json_start = i
            break
    if json_start == -1:
        return {"_error": True, "error": "no JSON in output", "raw": output[:500]}
    try:
        return json.loads(output[json_start:])
    except json.JSONDecodeError as exc:
        return {"_error": True, "error": f"JSON parse: {exc}", "raw": output[:500]}


def run_cli_raw(args, timeout=60):
    """Run radosgw-admin for TEXT output. Returns dict with text or error."""
    cmd = ["radosgw-admin"] + args
    logger.debug("CLI-RAW: %s", " ".join(cmd))
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"_error": True, "error": "timed out"}
    except Exception as exc:
        return {"_error": True, "error": str(exc)}

    if proc.returncode != 0:
        return {"_error": True, "error": proc.stderr.strip(), "stdout": proc.stdout.strip()}

    return {"_raw": True, "text": proc.stdout.strip()}


def is_error(result):
    return isinstance(result, dict) and result.get("_error", False)


# ------------------------------------------------------------------ #
#  Parsers (self-contained copies — kept in sync with collector.py)
# ------------------------------------------------------------------ #

def parse_sync_status_text(text):
    """Parse 'radosgw-admin sync status' TEXT output."""
    result = {"realm": "", "zonegroup": "", "zone": "",
              "metadata_sync": {}, "data_sync": []}
    for line in text.splitlines():
        s = line.strip()
        m = re.match(r'^realm\s+\S+\s+\((.+?)\)', s)
        if m: result["realm"] = m.group(1); continue
        m = re.match(r'^zonegroup\s+\S+\s+\((.+?)\)', s)
        if m: result["zonegroup"] = m.group(1); continue
        m = re.match(r'^zone\s+\S+\s+\((.+?)\)', s)
        if m: result["zone"] = m.group(1); continue

    # Metadata sync block
    meta_block = _extract_block(text, r'metadata sync')
    if meta_block:
        result["metadata_sync"] = _parse_sync_block(meta_block)

    # Data sync blocks
    for source_zone, block_text in _extract_data_sync_blocks(text):
        parsed = _parse_sync_block(block_text)
        parsed["source_zone"] = source_zone
        result["data_sync"].append(parsed)

    return result


def parse_bucket_sync_status_text(text):
    """Parse 'radosgw-admin bucket sync status --bucket X' TEXT output."""
    result = {"realm": "", "zonegroup": "", "zone": "", "bucket": "",
              "current_time": "", "sync_disabled": False, "sources": []}
    for line in text.splitlines():
        s = line.strip()
        m = re.match(r'^realm\s+\S+\s+\((.+?)\)', s)
        if m: result["realm"] = m.group(1); continue
        m = re.match(r'^zonegroup\s+\S+\s+\((.+?)\)', s)
        if m: result["zonegroup"] = m.group(1); continue
        m = re.match(r'^zone\s+\S+\s+\((.+?)\)', s)
        if m: result["zone"] = m.group(1); continue
        m = re.match(r'^bucket\s+:?(\S+?)[\[\(]', s)
        if m: result["bucket"] = m.group(1); continue
        m = re.match(r'^current time\s+([\dT:Z\.\-]+)', s)
        if m: result["current_time"] = m.group(1); continue
        if "sync is disabled" in s.lower() or "no sync sources" in s.lower():
            result["sync_disabled"] = True; continue

    # Parse per-source-zone blocks
    blocks = []
    lines = text.splitlines()
    current_zone = None; current_lines = []
    for line in lines:
        m = re.search(r'source zone\s+\S+\s+\((.+?)\)', line)
        if m:
            if current_zone is not None:
                blocks.append((current_zone, "\n".join(current_lines)))
            current_zone = m.group(1); current_lines = [line]; continue
        if current_zone is not None:
            current_lines.append(line)
    if current_zone is not None:
        blocks.append((current_zone, "\n".join(current_lines)))

    for source_name, block_text in blocks:
        parsed = _parse_bucket_source_block(block_text)
        parsed["source_zone"] = source_name
        result["sources"].append(parsed)

    return result


def _extract_block(text, header_pattern):
    lines = text.splitlines(); block_lines = []; capturing = False
    for line in lines:
        if re.search(header_pattern, line, re.IGNORECASE):
            capturing = True; block_lines.append(line); continue
        if capturing:
            if line.strip() and not line.startswith(' ' * 8) and not line.startswith('\t'):
                if block_lines and not line.strip().startswith(('full', 'incremental', 'metadata', 'data', 'shard')):
                    break
            block_lines.append(line)
    return "\n".join(block_lines)


def _extract_data_sync_blocks(text):
    blocks = []; lines = text.splitlines(); cz = None; cl = []
    for line in lines:
        m = re.search(r'data sync source:\s*\S+\s+\((.+?)\)', line)
        if m:
            if cz is not None: blocks.append((cz, "\n".join(cl)))
            cz = m.group(1); cl = [line]; continue
        if cz is not None:
            s = line.strip()
            if s and not line.startswith(' '):
                blocks.append((cz, "\n".join(cl))); cz = None; cl = []
            else: cl.append(line)
    if cz is not None: blocks.append((cz, "\n".join(cl)))
    return blocks


def _parse_sync_block(block):
    result = {"status": "unknown", "full_sync_done": 0, "full_sync_total": 0,
              "incremental_sync_done": 0, "incremental_sync_total": 0}
    for line in block.splitlines():
        s = line.strip()
        if "caught up" in s.lower(): result["status"] = "caught up"
        elif "syncing" in s.lower() and "sync:" not in s.lower(): result["status"] = "syncing"
        elif "behind" in s.lower(): result["status"] = "behind"
        m = re.search(r'full sync:\s*(\d+)/(\d+)\s*shards?', s)
        if m: result["full_sync_done"] = int(m.group(1)); result["full_sync_total"] = int(m.group(2))
        m = re.search(r'incremental sync:\s*(\d+)/(\d+)\s*shards?', s)
        if m: result["incremental_sync_done"] = int(m.group(1)); result["incremental_sync_total"] = int(m.group(2))
    return result


def _parse_bucket_source_block(block):
    result = {"status": "unknown", "full_sync_done": 0, "full_sync_total": 0,
              "incremental_sync_done": 0, "incremental_sync_total": 0, "shard_details": []}
    for line in block.splitlines():
        s = line.strip()
        if "caught up" in s.lower(): result["status"] = "caught up"
        elif "syncing" in s.lower() and "sync:" not in s.lower(): result["status"] = "syncing"
        elif "behind" in s.lower() and "sync:" not in s.lower(): result["status"] = "behind"
        m = re.search(r'full sync:\s*(\d+)/(\d+)\s*shards?', s)
        if m: result["full_sync_done"] = int(m.group(1)); result["full_sync_total"] = int(m.group(2))
        m = re.search(r'incremental sync:\s*(\d+)/(\d+)\s*shards?', s)
        if m: result["incremental_sync_done"] = int(m.group(1)); result["incremental_sync_total"] = int(m.group(2))
        m = re.match(r'bucket shard\s+(\d+):\s*(.*)', s)
        if m: result["shard_details"].append({"shard_id": int(m.group(1)), "status": m.group(2).strip()})
    # Infer status from shard counts if still unknown
    if result["status"] == "unknown":
        ft, fd = result["full_sync_total"], result["full_sync_done"]
        it, idn = result["incremental_sync_total"], result["incremental_sync_done"]
        if it > 0 and idn >= it and fd >= ft:
            result["status"] = "caught up"
        elif it > 0 or ft > 0:
            result["status"] = "syncing"
    return result


# ------------------------------------------------------------------ #
#  Sync Error Parser
# ------------------------------------------------------------------ #

def collect_sync_errors():
    """Collect and parse sync errors from radosgw-admin sync error list."""
    result = run_cli_json(["sync", "error", "list"])
    errors = []

    if is_error(result):
        logger.debug("sync error list returned error: %s", result.get("error"))
        return errors

    shards = result if isinstance(result, list) else result.get("shards", result.get("entries", []))

    for shard in shards:
        if not isinstance(shard, dict):
            continue
        shard_id = shard.get("shard_id", "")
        for entry in shard.get("entries", []):
            if not isinstance(entry, dict):
                continue
            raw_name = entry.get("name", "")
            bucket_name = raw_name.split(":", 1)[0] if raw_name else ""
            info = entry.get("info", {})
            errors.append({
                "shard_id": shard_id,
                "entry_id": entry.get("id", ""),
                "section": entry.get("section", ""),
                "raw_name": raw_name,
                "timestamp": entry.get("timestamp", ""),
                "bucket": bucket_name,
                "source_zone": info.get("source_zone", ""),
                "error_code": info.get("error_code", "unknown"),
                "message": info.get("message", ""),
            })

    return errors


# ------------------------------------------------------------------ #
#  Bucket List Discovery
# ------------------------------------------------------------------ #

def get_bucket_list():
    """Get list of bucket names from this zone."""
    result = run_cli_json(["bucket", "stats"])
    if is_error(result):
        logger.warning("Failed to get bucket list: %s", result.get("error"))
        return []
    if isinstance(result, dict):
        result = [result]
    return [b.get("bucket", "") for b in result if isinstance(b, dict) and b.get("bucket")]


# ------------------------------------------------------------------ #
#  Zone Name Auto-Detection
# ------------------------------------------------------------------ #

def detect_zone_name():
    """Auto-detect this zone's name from radosgw-admin sync status."""
    result = run_cli_raw(["sync", "status"], timeout=15)
    if is_error(result):
        logger.warning("Could not auto-detect zone name: %s", result.get("error"))
        return None
    parsed = parse_sync_status_text(result["text"])
    zone = parsed.get("zone", "")
    if zone:
        logger.info("Auto-detected zone name: %s", zone)
    return zone or None


# ------------------------------------------------------------------ #
#  Collection Cycle
# ------------------------------------------------------------------ #

def collect_all(zone_name, max_buckets=500):
    """
    Run one full collection cycle. Returns the payload to push to primary.

    Collects:
      1. sync status (global — from this zone's perspective)
      2. sync error list (errors recorded on this zone)
      3. bucket sync status (per-bucket shard-level detail)
    """
    ts = datetime.now(timezone.utc).isoformat()
    logger.info("Collection cycle at %s (zone: %s)", ts, zone_name)

    payload = {
        "zone_name": zone_name,
        "timestamp": ts,
        "agent_version": "1.0",
        "sync_status": None,
        "sync_errors": [],
        "bucket_sync_status": {},
    }

    # 1. Global sync status
    logger.info("Collecting sync status...")
    result = run_cli_raw(["sync", "status"], timeout=30)
    if is_error(result):
        logger.warning("sync status failed: %s", result.get("error"))
        payload["sync_status"] = {
            "status": "error", "error": result.get("error", "unknown"),
        }
    else:
        parsed = parse_sync_status_text(result["text"])
        parsed["status"] = "ok"
        payload["sync_status"] = parsed
        logger.info("  Metadata: %s | Data sources: %d",
                     parsed.get("metadata_sync", {}).get("status", "?"),
                     len(parsed.get("data_sync", [])))
        for ds in parsed.get("data_sync", []):
            logger.info("  Data from '%s': %s (full=%d/%d, incr=%d/%d)",
                        ds.get("source_zone", "?"), ds.get("status", "?"),
                        ds.get("full_sync_done", 0), ds.get("full_sync_total", 0),
                        ds.get("incremental_sync_done", 0), ds.get("incremental_sync_total", 0))

    # 2. Sync errors
    logger.info("Collecting sync errors...")
    errors = collect_sync_errors()
    payload["sync_errors"] = errors
    logger.info("  Found %d error(s)", len(errors))

    # 3. Per-bucket sync status
    buckets = get_bucket_list()
    if len(buckets) > max_buckets:
        logger.warning("Found %d buckets, limiting to %d", len(buckets), max_buckets)
        buckets = buckets[:max_buckets]

    logger.info("Collecting bucket sync status for %d bucket(s)...", len(buckets))
    for bucket in buckets:
        result = run_cli_raw(["bucket", "sync", "status", "--bucket", bucket,
                              "--rgw-zone", zone_name], timeout=30)
        if is_error(result):
            logger.debug("  bucket sync status failed for '%s': %s", bucket, result.get("error"))
            continue
        parsed = parse_bucket_sync_status_text(result["text"])
        # Strip shard_details to reduce payload size (keep summary only)
        for src in parsed.get("sources", []):
            shard_count = len(src.get("shard_details", []))
            src["shard_count"] = shard_count
            # Keep shard details only if there are problems
            behind_shards = [sd for sd in src.get("shard_details", [])
                            if "behind" in sd.get("status", "").lower()
                            or "error" in sd.get("status", "").lower()]
            src["problem_shards"] = behind_shards
            del src["shard_details"]

        payload["bucket_sync_status"][bucket] = parsed

    logger.info("  Collected sync status for %d/%d bucket(s)",
                len(payload["bucket_sync_status"]), len(buckets))

    return payload


# ------------------------------------------------------------------ #
#  Push to Primary
# ------------------------------------------------------------------ #

def push_to_primary(primary_url, payload):
    """POST the collected payload to the primary site's API."""
    url = primary_url.rstrip("/") + "/api/zone-agent/push"
    data = json.dumps(payload).encode("utf-8")

    logger.info("Pushing %d bytes to %s ...", len(data), url)
    logger.debug("Payload: sync_status=%s, errors=%d, buckets=%d",
                 payload["sync_status"].get("status", "?") if payload["sync_status"] else "null",
                 len(payload["sync_errors"]),
                 len(payload["bucket_sync_status"]))

    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8")
            status = resp.status
            if status == 200:
                logger.info("  Push OK (HTTP %d)", status)
                return True
            else:
                logger.warning("  Push returned HTTP %d: %s", status, body[:200])
                return False
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:300]
        logger.error("  Push failed: HTTP %d — %s", exc.code, body)
        return False
    except urllib.error.URLError as exc:
        logger.error("  Push failed: %s", exc.reason)
        return False
    except Exception as exc:
        logger.error("  Push failed: %s", exc)
        return False


# ------------------------------------------------------------------ #
#  Config Loading
# ------------------------------------------------------------------ #

def load_agent_config(path):
    """Load agent config from YAML or simple key:value file."""
    config = {}
    if not path or not os.path.exists(path):
        return config
    try:
        import yaml
        with open(path, "r") as f:
            config = yaml.safe_load(f) or {}
    except ImportError:
        # Fallback: simple key: value parser
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if ":" in line:
                    k, v = line.split(":", 1)
                    k, v = k.strip(), v.strip().strip('"').strip("'")
                    if v.lower() in ("true", "yes"):
                        config[k] = True
                    elif v.lower() in ("false", "no"):
                        config[k] = False
                    elif v.isdigit():
                        config[k] = int(v)
                    else:
                        config[k] = v
    return config


# ------------------------------------------------------------------ #
#  Pre-flight Checks
# ------------------------------------------------------------------ #

def preflight_check(primary_url):
    """Validate radosgw-admin, cluster access, and primary API reachability."""
    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║   RGW Multisite Monitor — Zone Agent Pre-flight     ║")
    print("╚══════════════════════════════════════════════════════╝")
    print()

    # 1. radosgw-admin binary
    import shutil
    if not shutil.which("radosgw-admin"):
        print("  ✗ radosgw-admin not found on PATH")
        return False
    print("  ✓ radosgw-admin found")

    # 2. Cluster access
    result = run_cli_json(["realm", "get"], timeout=10)
    if is_error(result):
        # Try without realm (might have no realm configured)
        result2 = run_cli_raw(["sync", "status"], timeout=10)
        if is_error(result2):
            print(f"  ✗ Cannot access cluster: {result.get('error', '?')}")
            return False
    print("  ✓ Cluster access verified")

    # 3. Primary API reachable
    url = primary_url.rstrip("/") + "/api/health"
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            print(f"  ✓ Primary API reachable at {primary_url}")
            print(f"    Status: {body.get('status', '?')}, "
                  f"Collector: {'running' if body.get('collector_running') else 'stopped'}")
    except Exception as exc:
        print(f"  ✗ Cannot reach primary API at {url}: {exc}")
        print(f"    Ensure the primary dashboard is running and accessible.")
        return False

    print()
    return True


# ------------------------------------------------------------------ #
#  Main Loop
# ------------------------------------------------------------------ #

def main():
    parser = argparse.ArgumentParser(
        description="RGW Multisite Monitor — Secondary Zone Agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --primary-url http://primary-node:5000
  %(prog)s --primary-url http://primary-node:5000 --interval 120 --debug
  %(prog)s --config agent.yaml
        """,
    )
    parser.add_argument("--primary-url", "-u", type=str, default=None,
                        help="URL of the primary site dashboard API (e.g. http://host:5000)")
    parser.add_argument("--interval", "-i", type=int, default=None,
                        help="Push interval in seconds (default: 60)")
    parser.add_argument("--zone", "-z", type=str, default=None,
                        help="Zone name (default: auto-detect)")
    parser.add_argument("--max-buckets", type=int, default=None,
                        help="Max buckets to collect sync status for (default: 500)")
    parser.add_argument("--config", "-c", type=str, default=None,
                        help="Path to agent config YAML file")
    parser.add_argument("--once", action="store_true",
                        help="Run one collection cycle and exit (for testing/cron)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Collect and print payload without pushing to primary")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Verbose logging")
    parser.add_argument("--debug", "-d", action="store_true",
                        help="Debug logging (shows CLI commands)")
    args = parser.parse_args()

    # Log level
    if args.debug:
        logger.setLevel(logging.DEBUG)
    elif args.verbose:
        logger.setLevel(logging.INFO)

    # Load config file if provided
    file_config = load_agent_config(args.config)

    # Merge: CLI args > config file > defaults
    primary_url = args.primary_url or file_config.get("primary_url", "")
    interval = args.interval or file_config.get("push_interval", 60)
    zone_name = args.zone or file_config.get("zone_name", "")
    max_buckets = args.max_buckets or file_config.get("max_buckets", 500)

    if not primary_url and not args.dry_run:
        print("ERROR: --primary-url is required (or set primary_url in config file)")
        print("  Example: python3 zone_agent.py --primary-url http://primary-node:5000")
        sys.exit(1)

    # Pre-flight
    if not args.dry_run:
        if not preflight_check(primary_url):
            sys.exit(1)

    # Auto-detect zone name
    if not zone_name:
        zone_name = detect_zone_name()
        if not zone_name:
            print("ERROR: Could not auto-detect zone name.")
            print("  Specify with --zone <name> or set zone_name in config file.")
            sys.exit(1)

    print(f"  Zone:     {zone_name}")
    print(f"  Primary:  {primary_url or '(dry-run)'}")
    print(f"  Interval: {interval}s")
    print(f"  Buckets:  max {max_buckets}")
    print()

    # Graceful shutdown
    running = True
    def handle_signal(signum, frame):
        nonlocal running
        logger.info("Received signal %d — shutting down...", signum)
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Main loop
    consecutive_failures = 0
    while running:
        try:
            payload = collect_all(zone_name, max_buckets=max_buckets)

            if args.dry_run:
                print(json.dumps(payload, indent=2, default=str))
                if args.once:
                    break
            else:
                success = push_to_primary(primary_url, payload)
                if success:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    if consecutive_failures >= 5:
                        logger.warning("5 consecutive push failures — "
                                      "check primary API connectivity")

                if args.once:
                    sys.exit(0 if success else 1)

        except Exception:
            logger.exception("Unexpected error in collection cycle")
            consecutive_failures += 1

        if args.once:
            break

        # Sleep with interruptibility
        for _ in range(interval):
            if not running:
                break
            time.sleep(1)

    logger.info("Agent stopped.")


if __name__ == "__main__":
    main()