#!/usr/bin/env python3
"""
Ceph RGW Multisite Sync Collector
=================================
Collects sync status, bucket stats, and error data from RGW multisite
deployments using radosgw-admin CLI commands.

MUST run on a Ceph admin node where `radosgw-admin` has cluster access.

IMPORTANT — Two categories of radosgw-admin commands:

  Commands that produce JSON (with --format=json):
    - realm get / realm list
    - period get
    - zonegroup get
    - bucket stats
    - sync error list

  Commands that produce PLAIN TEXT (ignore --format=json):
    - sync status
    - bucket sync status --bucket <name>

This module has separate runners and parsers for each category.
"""

import json
import re
import subprocess
import logging
import shutil
import threading
from datetime import datetime, timezone
from collections import defaultdict

logger = logging.getLogger(__name__)


# ================================================================== #
#  Pre-flight Validation
# ================================================================== #

class CephAccessError(Exception):
    """Raised when the host does not have working Ceph/RGW CLI access."""
    pass


def validate_ceph_access():
    """
    Validate that this host has a working radosgw-admin binary and can
    reach the Ceph cluster. Fails fast with a clear error if not.

    Checks:
      1. radosgw-admin binary exists on PATH
      2. radosgw-admin can reach the cluster (realm get)
    """
    rgw_path = shutil.which("radosgw-admin")
    if rgw_path is None:
        raise CephAccessError(
            "FATAL: 'radosgw-admin' not found on PATH.\n"
            "This tool must run on a Ceph admin/mon node.\n"
            "Install:  yum install ceph-radosgw  (RHEL)\n"
            "          apt install radosgw        (Debian)"
        )
    logger.info("Found radosgw-admin at: %s", rgw_path)

    try:
        result = subprocess.run(
            ["radosgw-admin", "realm", "get", "--format=json"],
            capture_output=True, text=True, timeout=15
        )
    except subprocess.TimeoutExpired:
        raise CephAccessError(
            "FATAL: 'radosgw-admin realm get' timed out.\n"
            "MON cluster may be unreachable. Verify with: ceph status"
        )
    except FileNotFoundError:
        raise CephAccessError("FATAL: radosgw-admin binary not found.")

    if result.returncode != 0:
        stderr = result.stderr.strip()
        if "could not init" in stderr.lower() or "error connecting" in stderr.lower():
            raise CephAccessError(
                f"FATAL: radosgw-admin cannot connect to the cluster.\n"
                f"stderr: {stderr}\n"
                f"Verify: ceph status\n"
                f"Ensure /etc/ceph/ceph.conf and keyrings are present."
            )
        elif "no realm" in stderr.lower():
            logger.warning("Cluster reachable but no realm found — "
                           "multisite may not be configured. stderr: %s", stderr)
            return
        else:
            raise CephAccessError(
                f"FATAL: radosgw-admin error.\n"
                f"rc={result.returncode}\nstderr: {stderr}"
            )

    logger.info("Ceph cluster access verified")


# ================================================================== #
#  CLI Runners — JSON and Raw Text
# ================================================================== #

def run_cli_json(args: list, timeout: int = 60):
    """
    Run a radosgw-admin command that produces JSON output.
    Appends --format=json automatically.

    Returns:
      On success: parsed dict or list
      On failure: dict with _error=True
    """
    cmd = ["radosgw-admin"] + args + ["--format=json"]
    logger.debug("CLI-JSON exec: %s", " ".join(cmd))

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.debug("CLI-JSON timed out after %ds: %s", timeout, " ".join(cmd))
        return {"_error": True, "error": "timed out", "cmd": " ".join(args)}
    except Exception as exc:
        logger.debug("CLI-JSON exception: %s — %s", " ".join(cmd), exc)
        return {"_error": True, "error": str(exc), "cmd": " ".join(args)}

    logger.debug("CLI-JSON rc=%d stdout=%d bytes stderr=%d bytes",
                 proc.returncode, len(proc.stdout), len(proc.stderr))

    if proc.returncode != 0:
        logger.debug("CLI-JSON failed: rc=%d stderr=%s", proc.returncode, proc.stderr.strip()[:200])
        return {
            "_error": True,
            "error": proc.stderr.strip(),
            "rc": proc.returncode,
            "cmd": " ".join(args),
        }

    # Find the JSON portion — radosgw-admin sometimes emits preamble text
    output = proc.stdout.strip()
    json_start = -1
    for i, ch in enumerate(output):
        if ch in ('{', '['):
            json_start = i
            break

    if json_start == -1:
        logger.debug("CLI-JSON no JSON found in output (first 200 chars): %s", output[:200])
        return {"_error": True, "error": "no JSON in output", "raw": output[:500]}

    if json_start > 0:
        logger.debug("CLI-JSON skipped %d bytes of preamble before JSON", json_start)

    try:
        parsed = json.loads(output[json_start:])
        logger.debug("CLI-JSON parsed OK — type=%s", type(parsed).__name__)
        return parsed
    except json.JSONDecodeError as exc:
        logger.debug("CLI-JSON parse failed: %s — raw: %s", exc, output[:200])
        return {"_error": True, "error": f"JSON parse: {exc}", "raw": output[:500]}


def run_cli_raw(args: list, timeout: int = 60):
    """
    Run a radosgw-admin command that produces PLAIN TEXT output.
    Does NOT append --format=json (it's ignored by these commands anyway).

    Returns:
      On success: dict with _raw=True, text=<raw stdout>
      On failure: dict with _error=True
    """
    cmd = ["radosgw-admin"] + args
    logger.debug("CLI-RAW exec: %s", " ".join(cmd))

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.debug("CLI-RAW timed out after %ds: %s", timeout, " ".join(cmd))
        return {"_error": True, "error": "timed out", "cmd": " ".join(args)}
    except Exception as exc:
        logger.debug("CLI-RAW exception: %s — %s", " ".join(cmd), exc)
        return {"_error": True, "error": str(exc), "cmd": " ".join(args)}

    logger.debug("CLI-RAW rc=%d stdout=%d bytes stderr=%d bytes",
                 proc.returncode, len(proc.stdout), len(proc.stderr))

    if proc.returncode != 0:
        logger.debug("CLI-RAW failed: rc=%d stderr=%s", proc.returncode, proc.stderr.strip()[:200])
        return {
            "_error": True,
            "error": proc.stderr.strip(),
            "stdout": proc.stdout.strip(),
            "rc": proc.returncode,
            "cmd": " ".join(args),
        }

    logger.debug("CLI-RAW output (%d lines): %s...",
                 proc.stdout.count('\n'), proc.stdout.strip()[:120])
    return {"_raw": True, "text": proc.stdout.strip()}


def is_error(result) -> bool:
    """Check if a CLI result is an error."""
    return isinstance(result, dict) and result.get("_error", False)


# ================================================================== #
#  Text Parsers — for commands that don't output JSON
# ================================================================== #

def parse_sync_status_text(text: str) -> dict:
    """
    Parse the plain-text output of `radosgw-admin sync status`.

    Example output:
              realm 4a15f6c4-... (test_realm)
          zonegroup 442935dd-... (default)
               zone 15cea747-... (test_zone)
      metadata sync syncing
                    full sync: 0/64 shards
                    incremental sync: 64/64 shards
                    metadata is caught up with master
      data sync source: abc123-... (zone2)
                        syncing
                        full sync: 0/128 shards
                        incremental sync: 128/128 shards
                        data is caught up with source

    Returns a structured dict.
    """
    result = {
        "realm": "", "zonegroup": "", "zone": "",
        "metadata_sync": {}, "data_sync": [],
    }

    lines = text.splitlines()

    for line in lines:
        stripped = line.strip()

        # Header fields: "realm <id> (<name>)"
        m = re.match(r'^realm\s+\S+\s+\((.+?)\)', stripped)
        if m:
            result["realm"] = m.group(1)
            continue

        m = re.match(r'^zonegroup\s+\S+\s+\((.+?)\)', stripped)
        if m:
            result["zonegroup"] = m.group(1)
            continue

        m = re.match(r'^zone\s+\S+\s+\((.+?)\)', stripped)
        if m:
            result["zone"] = m.group(1)
            continue

    # Parse metadata sync block
    meta_block = _extract_block(text, r'metadata sync')
    if meta_block:
        result["metadata_sync"] = _parse_sync_block(meta_block)

    # Parse data sync blocks (one per source zone)
    data_blocks = _extract_data_sync_blocks(text)
    for source_zone, block_text in data_blocks:
        parsed = _parse_sync_block(block_text)
        parsed["source_zone"] = source_zone
        result["data_sync"].append(parsed)

    return result


def _extract_block(text: str, header_pattern: str) -> str:
    """Extract a contiguous block starting with header_pattern."""
    lines = text.splitlines()
    block_lines = []
    capturing = False

    for line in lines:
        if re.search(header_pattern, line, re.IGNORECASE):
            capturing = True
            block_lines.append(line)
            continue

        if capturing:
            # Stop at next top-level section or empty line after content
            if line.strip() and not line.startswith(' ' * 8) and not line.startswith('\t'):
                # New section (less indented) — stop
                if block_lines and not line.strip().startswith(('full', 'incremental',
                                                                 'metadata', 'data', 'shard')):
                    break
            block_lines.append(line)

    return "\n".join(block_lines)


def _extract_data_sync_blocks(text: str) -> list:
    """
    Extract each 'data sync source: <id> (<name>)' block.
    Returns [(source_zone_name, block_text), ...]
    """
    blocks = []
    lines = text.splitlines()
    current_zone = None
    current_lines = []

    for line in lines:
        m = re.search(r'data sync source:\s*\S+\s+\((.+?)\)', line)
        if m:
            # Save previous block
            if current_zone is not None:
                blocks.append((current_zone, "\n".join(current_lines)))
            current_zone = m.group(1)
            current_lines = [line]
            continue

        if current_zone is not None:
            # Check if we've hit a new top-level section
            stripped = line.strip()
            if stripped and not line.startswith(' '):
                # Non-indented non-empty line — end of block
                blocks.append((current_zone, "\n".join(current_lines)))
                current_zone = None
                current_lines = []
            else:
                current_lines.append(line)

    if current_zone is not None:
        blocks.append((current_zone, "\n".join(current_lines)))

    return blocks


def _parse_sync_block(block: str) -> dict:
    """
    Parse common sync block fields:
      full sync: X/Y shards
      incremental sync: X/Y shards
      status text like "caught up" or "behind"
    """
    result = {
        "status": "unknown",
        "full_sync_done": 0, "full_sync_total": 0,
        "incremental_sync_done": 0, "incremental_sync_total": 0,
        "behind_shards": [],
        "raw": block.strip(),
    }

    for line in block.splitlines():
        stripped = line.strip()

        # Status keyword
        if "syncing" in stripped.lower() and "sync:" not in stripped.lower():
            result["status"] = "syncing"
        if "caught up" in stripped.lower():
            result["status"] = "caught up"

        # full sync: N/M shards
        m = re.search(r'full sync:\s*(\d+)/(\d+)\s*shards?', stripped)
        if m:
            result["full_sync_done"] = int(m.group(1))
            result["full_sync_total"] = int(m.group(2))

        # incremental sync: N/M shards
        m = re.search(r'incremental sync:\s*(\d+)/(\d+)\s*shards?', stripped)
        if m:
            result["incremental_sync_done"] = int(m.group(1))
            result["incremental_sync_total"] = int(m.group(2))

        # Shard-level detail: "shard N: behind by X seconds"
        m = re.search(r'shard\s+(\d+).*behind', stripped, re.IGNORECASE)
        if m:
            result["behind_shards"].append({
                "shard_id": int(m.group(1)),
                "detail": stripped,
            })

    return result


def parse_bucket_sync_status_text(text: str) -> dict:
    """
    Parse the plain-text output of
    `radosgw-admin bucket sync status --bucket <name>`.

    Example outputs:

    1) Sync disabled:
              realm 4a15f6c4-... (test_realm)
          zonegroup 442935dd-... (default)
               zone 15cea747-... (test_zone)
             bucket :b11[...])
       current time 2026-02-10T09:22:13Z

       Sync is disabled for bucket b11 or bucket has no sync sources

    2) Sync active (shard-level detail):
              realm <id> (<name>)
          zonegroup <id> (<name>)
               zone <id> (<name>)
             bucket :<name>[<id>])
       current time <timestamp>

        source zone <id> (<name>)
                     full sync: 0/8 shards
                     incremental sync: 8/8 shards
                     bucket shard 0: ...
                     bucket shard 1: ...
    """
    result = {
        "realm": "", "zonegroup": "", "zone": "",
        "bucket": "", "current_time": "",
        "sync_disabled": False,
        "sources": [],
        "raw": text.strip(),
    }

    lines = text.splitlines()

    for line in lines:
        stripped = line.strip()

        m = re.match(r'^realm\s+\S+\s+\((.+?)\)', stripped)
        if m:
            result["realm"] = m.group(1)
            continue

        m = re.match(r'^zonegroup\s+\S+\s+\((.+?)\)', stripped)
        if m:
            result["zonegroup"] = m.group(1)
            continue

        m = re.match(r'^zone\s+\S+\s+\((.+?)\)', stripped)
        if m:
            result["zone"] = m.group(1)
            continue

        m = re.match(r'^bucket\s+:?(\S+?)[\[\(]', stripped)
        if m:
            result["bucket"] = m.group(1)
            continue

        m = re.match(r'^current time\s+([\dT:Z\.\-]+)', stripped)
        if m:
            result["current_time"] = m.group(1)
            continue

        # Check for "sync is disabled"
        if "sync is disabled" in stripped.lower() or "no sync sources" in stripped.lower():
            result["sync_disabled"] = True
            continue

    # Parse per-source-zone blocks
    source_blocks = _extract_source_zone_blocks(text)
    for source_name, block_text in source_blocks:
        parsed = _parse_bucket_source_block(block_text)
        parsed["source_zone"] = source_name
        result["sources"].append(parsed)

    return result


def _extract_source_zone_blocks(text: str) -> list:
    """
    Extract 'source zone <id> (<name>)' blocks from bucket sync status.
    Returns [(zone_name, block_text), ...]
    """
    blocks = []
    lines = text.splitlines()
    current_zone = None
    current_lines = []

    for line in lines:
        m = re.search(r'source zone\s+\S+\s+\((.+?)\)', line)
        if m:
            if current_zone is not None:
                blocks.append((current_zone, "\n".join(current_lines)))
            current_zone = m.group(1)
            current_lines = [line]
            continue

        if current_zone is not None:
            current_lines.append(line)

    if current_zone is not None:
        blocks.append((current_zone, "\n".join(current_lines)))

    return blocks


def _parse_bucket_source_block(block: str) -> dict:
    """Parse a single source-zone block within bucket sync status."""
    result = {
        "status": "unknown",
        "full_sync_done": 0, "full_sync_total": 0,
        "incremental_sync_done": 0, "incremental_sync_total": 0,
        "shard_details": [],
    }

    for line in block.splitlines():
        stripped = line.strip()

        if "caught up" in stripped.lower():
            result["status"] = "caught up"
        elif "syncing" in stripped.lower() and "sync:" not in stripped.lower():
            result["status"] = "syncing"
        elif "behind" in stripped.lower() and "sync:" not in stripped.lower():
            result["status"] = "behind"

        m = re.search(r'full sync:\s*(\d+)/(\d+)\s*shards?', stripped)
        if m:
            result["full_sync_done"] = int(m.group(1))
            result["full_sync_total"] = int(m.group(2))

        m = re.search(r'incremental sync:\s*(\d+)/(\d+)\s*shards?', stripped)
        if m:
            result["incremental_sync_done"] = int(m.group(1))
            result["incremental_sync_total"] = int(m.group(2))

        # bucket shard N: <status detail>
        m = re.match(r'bucket shard\s+(\d+):\s*(.*)', stripped)
        if m:
            result["shard_details"].append({
                "shard_id": int(m.group(1)),
                "status": m.group(2).strip(),
            })

    # Infer status from shard counts if still unknown
    if result["status"] == "unknown":
        ft = result["full_sync_total"]
        fd = result["full_sync_done"]
        it = result["incremental_sync_total"]
        idn = result["incremental_sync_done"]
        if it > 0 and idn >= it and fd >= ft:
            result["status"] = "caught up"
        elif it > 0 or ft > 0:
            result["status"] = "syncing"

    return result


# ================================================================== #
#  Optional: RGW Admin REST API for Bucket Stats
# ================================================================== #

class RGWRestAPI:
    """
    Optional REST API client for querying bucket stats from zone endpoints.
    Only used when `use_rest_for_bucket_stats` is True.
    """

    def __init__(self, endpoint: str, access_key: str, secret_key: str,
                 verify_ssl: bool = False):
        self.endpoint = endpoint.rstrip("/")
        self.access_key = access_key
        self.secret_key = secret_key
        self.verify_ssl = verify_ssl
        self._session = None
        self._auth = None

    @property
    def session(self):
        if self._session is None:
            import requests
            self._session = requests.Session()
            self._session.verify = self.verify_ssl
        return self._session

    @property
    def auth(self):
        if self._auth is None:
            try:
                from requests_aws4auth import AWS4Auth
                self._auth = AWS4Auth(self.access_key, self.secret_key, "", "s3")
            except ImportError:
                self._auth = "query_string"  # fallback: pass keys as query params
        return self._auth

    def validate_access(self) -> dict:
        """
        Pre-flight check: verify we can reach the endpoint and authenticate.
        Calls GET /admin/bucket?stats=True with a limit of 1.
        Returns:
          On success: {"ok": True, "endpoint": ..., "status": 200}
          On failure: {"ok": False, "endpoint": ..., "error": ..., "status": ...}
        """
        url = f"{self.endpoint}/admin/bucket"
        params = {"format": "json", "stats": "True", "max-entries": "1"}

        logger.debug("REST validate_access: GET %s", url)
        try:
            if self.auth == "query_string":
                params["access_key"] = self.access_key
                params["key"] = self.secret_key
                resp = self.session.get(url, params=params, timeout=10)
            else:
                resp = self.session.get(url, params=params, auth=self.auth, timeout=10)
        except Exception as exc:
            return {
                "ok": False, "endpoint": self.endpoint,
                "error": f"Connection failed: {exc}", "status": 0,
            }

        if resp.status_code == 200:
            return {"ok": True, "endpoint": self.endpoint, "status": 200}
        else:
            # Parse common RGW error responses
            error_msg = resp.text[:300]
            if "SignatureDoesNotMatch" in error_msg:
                error_msg = "SignatureMismatch — access_key or secret_key is incorrect"
            elif "AccessDenied" in error_msg:
                error_msg = "AccessDenied — user may lack admin caps (buckets=read)"
            elif "InvalidAccessKeyId" in error_msg:
                error_msg = "InvalidAccessKeyId — access_key not found on this zone"
            return {
                "ok": False, "endpoint": self.endpoint,
                "error": error_msg, "status": resp.status_code,
            }

    def get_bucket_stats(self, bucket: str = None):
        """Fetch bucket stats via GET /admin/bucket?stats=True"""
        url = f"{self.endpoint}/admin/bucket"
        params = {"format": "json", "stats": "True"}
        if bucket:
            params["bucket"] = bucket

        try:
            if self.auth == "query_string":
                params["access_key"] = self.access_key
                params["key"] = self.secret_key
                resp = self.session.get(url, params=params, timeout=30)
            else:
                resp = self.session.get(url, params=params, auth=self.auth, timeout=30)
        except Exception as exc:
            return {"_error": True, "error": str(exc)}

        if resp.status_code == 200:
            return resp.json()
        else:
            return {"_error": True, "error": resp.text[:300], "status": resp.status_code}


# ================================================================== #
#  Topology Discovery (always via CLI JSON)
# ================================================================== #

class MultisiteTopology:
    """
    Auto-discovers multisite topology from the local cluster.
    realm → period → zonegroups → zones with endpoints.
    """

    def __init__(self):
        self.realm = {}
        self.period = {}
        self.zones = []
        self.zonegroups = []
        self.master_zone = None
        self.secondary_zones = []

    def discover(self):
        """Run discovery via radosgw-admin CLI (JSON commands)."""
        logger.info("Discovering multisite topology...")

        # Realm
        self.realm = run_cli_json(["realm", "get"])
        if is_error(self.realm):
            logger.warning("realm get failed: %s — trying realm list",
                           self.realm.get("error"))
            realm_list = run_cli_json(["realm", "list"])
            if not is_error(realm_list):
                realms = realm_list.get("realms", [])
                if realms:
                    self.realm = run_cli_json(["realm", "get", "--rgw-realm", realms[0]])

        realm_name = self.realm.get("name", "unknown") if not is_error(self.realm) else "unknown"
        logger.info("Realm: %s", realm_name)

        # Period (contains the full zone map)
        self.period = run_cli_json(["period", "get"])
        if is_error(self.period):
            logger.warning("period get failed — trying zonegroup get")
            zg = run_cli_json(["zonegroup", "get"])
            if not is_error(zg):
                self._parse_zonegroup(zg)
            else:
                raise CephAccessError(
                    "Cannot discover topology. Both 'period get' and "
                    "'zonegroup get' failed. Is multisite configured?"
                )
        else:
            self._parse_period(self.period)

        logger.info("Discovered %d zone(s) across %d zonegroup(s)",
                     len(self.zones), len(self.zonegroups))
        for z in self.zones:
            role = "MASTER" if z["is_master"] else "SECONDARY"
            logger.info("  Zone: %-20s [%s]  endpoints: %s",
                        z["name"], role, z["endpoints"])

        self.master_zone = next((z for z in self.zones if z["is_master"]), None)
        self.secondary_zones = [z for z in self.zones if not z["is_master"]]

        if not self.master_zone and self.zones:
            logger.warning("No master zone identified — using first zone")
            self.zones[0]["is_master"] = True
            self.master_zone = self.zones[0]
            self.secondary_zones = self.zones[1:]

    def _parse_period(self, period: dict):
        epoch = period.get("period_map", period)
        for zg in epoch.get("zonegroups", []):
            self._parse_zonegroup(zg)

    def _parse_zonegroup(self, zg: dict):
        zg_name = zg.get("name", "default")
        master_zone_id = zg.get("master_zone", "")
        self.zonegroups.append(zg_name)

        for zone in zg.get("zones", []):
            self.zones.append({
                "name": zone.get("name", ""),
                "id": zone.get("id", ""),
                "endpoints": zone.get("endpoints", []),
                "is_master": zone.get("id") == master_zone_id,
                "zonegroup": zg_name,
            })

    def to_dict(self) -> dict:
        return {
            "realm": self.realm.get("name", "unknown") if not is_error(self.realm) else "unknown",
            "zonegroups": list(self.zonegroups),
            "zones": [
                {"name": z["name"], "endpoints": z["endpoints"],
                 "is_master": z["is_master"], "zonegroup": z["zonegroup"]}
                for z in self.zones
            ],
            "master_zone": self.master_zone["name"] if self.master_zone else "unknown",
            "is_single_zone": len(self.secondary_zones) == 0,
        }


# ================================================================== #
#  In-Memory Data Store
# ================================================================== #

class SyncDataStore:
    """In-memory store with per-bucket snapshot history and zone agent data."""

    def __init__(self, max_snapshots: int = 20):
        self.max_snapshots = max_snapshots
        self.lock = threading.Lock()
        self.bucket_history = defaultdict(list)
        self.global_history = []
        self.bucket_errors = defaultdict(list)
        self.global_errors = []
        self.topology = {}
        # Zone agent data: keyed by zone name
        self.zone_agent_data = {}       # {zone_name: latest_payload}
        self.zone_agent_history = defaultdict(list)  # {zone_name: [sync_status_snapshots]}

    def add_bucket_snapshot(self, bucket: str, snapshot: dict):
        with self.lock:
            h = self.bucket_history[bucket]
            h.append(snapshot)
            if len(h) > self.max_snapshots:
                h.pop(0)

    def add_global_snapshot(self, snapshot: dict):
        with self.lock:
            self.global_history.append(snapshot)
            if len(self.global_history) > self.max_snapshots:
                self.global_history.pop(0)

    def set_bucket_errors(self, bucket: str, errors: list):
        with self.lock:
            self.bucket_errors[bucket] = errors

    def set_global_errors(self, errors: list):
        with self.lock:
            self.global_errors = errors

    def set_topology(self, topo: dict):
        with self.lock:
            self.topology = topo

    def update_zone_agent(self, zone_name: str, payload: dict):
        """
        Store data pushed by a secondary zone agent.

        Payload structure:
          zone_name, timestamp, sync_status, sync_errors, bucket_sync_status
        """
        with self.lock:
            self.zone_agent_data[zone_name] = payload

            # Keep sync status history for this zone
            if payload.get("sync_status"):
                entry = dict(payload["sync_status"])
                entry["timestamp"] = payload.get("timestamp", "")
                entry["source"] = "zone_agent"
                entry["zone_name"] = zone_name
                h = self.zone_agent_history[zone_name]
                h.append(entry)
                if len(h) > self.max_snapshots:
                    h.pop(0)

            logger.info("Zone agent data updated for '%s' — errors=%d, bucket_sync=%d",
                        zone_name,
                        len(payload.get("sync_errors", [])),
                        len(payload.get("bucket_sync_status", {})))

    def get_dashboard_data(self) -> dict:
        with self.lock:
            buckets = {}
            for name, history in self.bucket_history.items():
                buckets[name] = {
                    "history": list(history),
                    "errors": list(self.bucket_errors.get(name, [])),
                }
            # Build zone agent summary for dashboard
            zone_agents = {}
            for zone_name, payload in self.zone_agent_data.items():
                zone_agents[zone_name] = {
                    "timestamp": payload.get("timestamp", ""),
                    "sync_status": payload.get("sync_status"),
                    "sync_errors": payload.get("sync_errors", []),
                    "bucket_sync_status": payload.get("bucket_sync_status", {}),
                    "agent_version": payload.get("agent_version", ""),
                }
            zone_agent_sync_history = {}
            for zone_name, history in self.zone_agent_history.items():
                zone_agent_sync_history[zone_name] = list(history)

            return {
                "topology": dict(self.topology),
                "buckets": buckets,
                "global_sync": list(self.global_history),
                "global_errors": list(self.global_errors),
                "zone_agents": zone_agents,
                "zone_agent_sync_history": zone_agent_sync_history,
                "last_update": datetime.now(timezone.utc).isoformat(),
            }


# ================================================================== #
#  Sync Collector
# ================================================================== #

class SyncCollector:
    """
    Periodically collects sync data using radosgw-admin CLI.

    Uses:
      - run_cli_json()  for: realm, period, bucket stats, sync error list
      - run_cli_raw()   for: sync status, bucket sync status
    """

    def __init__(self, config: dict, store: SyncDataStore):
        self.config = config
        self.store = store
        self.topology = None
        self.zone_rest_apis = {}
        self._stop = threading.Event()

    def initialize(self):
        """Pre-flight + topology discovery + optional REST setup."""
        logger.info("Initializing collector...")
        logger.info("Config: collection_interval=%s, use_rest_for_bucket_stats=%s, verify_ssl=%s",
                     self.config.get("collection_interval", 60),
                     self.config.get("use_rest_for_bucket_stats", False),
                     self.config.get("verify_ssl", False))

        if self.config.get("use_rest_for_bucket_stats"):
            ak = self.config.get("access_key", "")
            sk = self.config.get("secret_key", "")
            if ak and sk:
                logger.info("REST mode: access_key=%s... (configured)", ak[:8])
            else:
                logger.warning("REST mode enabled but access_key/secret_key missing — will fall back to CLI")

        validate_ceph_access()

        self.topology = MultisiteTopology()
        self.topology.discover()
        self.store.set_topology(self.topology.to_dict())

        # Optional REST API for bucket stats — with pre-flight validation
        if self.config.get("use_rest_for_bucket_stats", False):
            ak = self.config.get("access_key", "")
            sk = self.config.get("secret_key", "")
            ssl = self.config.get("verify_ssl", False)
            if ak and sk:
                for zone in self.topology.zones:
                    eps = zone.get("endpoints", [])
                    if not eps:
                        logger.warning("Zone '%s' has no endpoints — cannot set up REST API", zone["name"])
                        continue
                    api = RGWRestAPI(eps[0], ak, sk, ssl)
                    logger.info("Validating REST access for zone '%s' → %s ...", zone["name"], eps[0])
                    check = api.validate_access()
                    if check["ok"]:
                        logger.info("  ✓ REST access OK for zone '%s'", zone["name"])
                        self.zone_rest_apis[zone["name"]] = api
                    else:
                        logger.warning("  ✗ REST access FAILED for zone '%s': %s (HTTP %s)",
                                       zone["name"], check["error"], check.get("status", "?"))
                        logger.warning("    → Will fall back to CLI for this zone")

                if self.zone_rest_apis:
                    logger.info("REST bucket stats validated for zones: %s",
                                list(self.zone_rest_apis.keys()))
                else:
                    logger.warning("REST validation failed for ALL zones — using CLI for everything")
            else:
                logger.warning("use_rest_for_bucket_stats=true but no credentials — using CLI for all zones")
        else:
            logger.info("Bucket stats method: CLI (radosgw-admin) for all zones")

        # Determine effective secondary data availability
        # A secondary zone is "reachable" if we have either:
        #   - A validated REST API for it, OR
        #   - CLI access (always available on local node)
        self._secondary_data_available = len(self.topology.secondary_zones) > 0

        logger.info("Collector initialized — %d zone(s), master=%s, secondaries=%s, "
                     "secondary_data_available=%s",
                     len(self.topology.zones),
                     self.topology.master_zone["name"] if self.topology.master_zone else "none",
                     [z["name"] for z in self.topology.secondary_zones],
                     self._secondary_data_available)

        # Store the data-availability flag in topology for the dashboard
        topo_dict = self.topology.to_dict()
        topo_dict["secondary_data_available"] = self._secondary_data_available
        topo_dict["rest_validated_zones"] = list(self.zone_rest_apis.keys())
        self.store.set_topology(topo_dict)

    # ------------------------------------------------------------------ #
    #  Main collection cycle
    # ------------------------------------------------------------------ #

    def collect_once(self):
        ts = datetime.now(timezone.utc).isoformat()
        logger.info("Collection cycle at %s", ts)
        try:
            self._collect_bucket_stats(ts)
            self._collect_sync_status(ts)
            self._collect_sync_errors(ts)
        except Exception:
            logger.exception("Error during collection cycle")

    # ------------------------------------------------------------------ #
    #  Bucket Stats (JSON command)
    # ------------------------------------------------------------------ #

    def _get_bucket_stats_cli(self, zone_name: str = None) -> dict:
        """Get bucket stats via CLI. Uses --rgw-zone for non-local zones."""
        cmd = ["bucket", "stats"]
        if zone_name:
            cmd += ["--rgw-zone", zone_name]

        logger.debug("Fetching bucket stats via CLI%s",
                      f" for zone '{zone_name}'" if zone_name else " (local/default zone)")
        result = run_cli_json(cmd)
        if is_error(result):
            logger.warning("Bucket stats CLI failed%s: %s",
                           f" for zone '{zone_name}'" if zone_name else "",
                           result.get("error"))
            return {}

        if isinstance(result, dict):
            result = [result]
        parsed = self._parse_bucket_stats(result)
        logger.debug("Bucket stats CLI%s: got %d bucket(s)",
                      f" [{zone_name}]" if zone_name else "", len(parsed))
        return parsed

    def _get_bucket_stats_rest(self, zone_name: str) -> dict:
        """Get bucket stats via REST API for a specific zone."""
        api = self.zone_rest_apis.get(zone_name)
        if not api:
            logger.warning("No REST API configured for zone '%s' — skipping", zone_name)
            return {}

        logger.debug("Fetching bucket stats via REST API for zone '%s' → %s",
                      zone_name, api.endpoint)
        result = api.get_bucket_stats()
        if isinstance(result, dict) and result.get("_error"):
            logger.warning("Bucket stats REST failed for zone '%s': %s",
                           zone_name, result.get("error", "unknown"))
            return {}
        if isinstance(result, dict):
            result = [result]
        parsed = self._parse_bucket_stats(result)
        logger.debug("Bucket stats REST [%s]: got %d bucket(s)", zone_name, len(parsed))
        return parsed

    def _collect_bucket_stats(self, ts: str):
        master_name = self.topology.master_zone["name"] if self.topology.master_zone else "primary"
        has_secondaries = self._secondary_data_available

        logger.info("Collecting bucket stats — master=%s, secondaries=%d, rest_apis=%d",
                     master_name, len(self.topology.secondary_zones), len(self.zone_rest_apis))

        zone_stats = {}

        # Master zone — always CLI
        logger.debug("Master zone '%s': using CLI (always)", master_name)
        zone_stats[master_name] = self._get_bucket_stats_cli()

        # Secondary zones — CLI or REST based on config
        # Track which secondaries actually returned data
        zones_with_data = []
        for zone in self.topology.secondary_zones:
            zn = zone["name"]
            if self.zone_rest_apis.get(zn):
                logger.info("Zone '%s': using REST API (use_rest_for_bucket_stats=true)", zn)
                zone_stats[zn] = self._get_bucket_stats_rest(zn)
            else:
                logger.info("Zone '%s': using CLI (--rgw-zone %s)", zn, zn)
                zone_stats[zn] = self._get_bucket_stats_cli(zone_name=zn)

            if zone_stats[zn]:
                zones_with_data.append(zn)
            else:
                logger.warning("Zone '%s': no bucket stats returned — data unavailable", zn)

        # Effective: do we have any secondary data to compare?
        has_comparison_data = len(zones_with_data) > 0
        if has_secondaries and not has_comparison_data:
            logger.warning("Secondary zones exist but none returned data — "
                           "treating as no-secondary-data for sync calculations")

        primary_stats = zone_stats.get(master_name, {})
        if not primary_stats:
            logger.warning("No primary bucket stats — skipping")
            return

        for bucket_name, primary in primary_stats.items():
            snapshot = {
                "timestamp": ts, "primary_zone": master_name,
                "primary": primary, "replicas": {},
                # no_secondary_data if: single zone OR secondaries exist but returned nothing
                "no_secondary_data": not has_comparison_data,
                "single_zone": len(self.topology.secondary_zones) == 0,
                "sync_progress_pct": None if not has_comparison_data else 100.0,
                "delta_objects": 0, "delta_size": 0,
            }

            if not has_comparison_data:
                self.store.add_bucket_snapshot(bucket_name, snapshot)
                continue

            worst_pct = 100.0
            total_delta_obj = 0
            total_delta_size = 0

            for zone in self.topology.secondary_zones:
                sec = zone_stats.get(zone["name"], {}).get(bucket_name, {
                    "num_objects": 0, "size_kb": 0, "num_shards": 0, "size_actual": 0,
                })
                d_obj = primary["num_objects"] - sec["num_objects"]
                d_size = primary["size_actual"] - sec["size_actual"]

                pct = 0.0
                if primary["num_objects"] > 0:
                    pct = min((sec["num_objects"] / primary["num_objects"]) * 100, 100.0)
                elif primary["num_objects"] == 0 and sec["num_objects"] == 0:
                    pct = 100.0

                snapshot["replicas"][zone["name"]] = {
                    "stats": sec,
                    "delta_objects": max(d_obj, 0),
                    "delta_size": max(d_size, 0),
                    "sync_progress_pct": round(pct, 2),
                }
                total_delta_obj += max(d_obj, 0)
                total_delta_size += max(d_size, 0)
                worst_pct = min(worst_pct, pct)

            snapshot["sync_progress_pct"] = round(worst_pct, 2)
            snapshot["delta_objects"] = total_delta_obj
            snapshot["delta_size"] = total_delta_size

            self.store.add_bucket_snapshot(bucket_name, snapshot)

        # Bucket sync status (TEXT command) for each bucket
        for bucket_name in primary_stats:
            self._collect_bucket_sync_status(bucket_name)

    # ------------------------------------------------------------------ #
    #  Bucket Sync Status (TEXT command — NOT JSON)
    # ------------------------------------------------------------------ #

    def _collect_bucket_sync_status(self, bucket: str):
        """
        radosgw-admin bucket sync status --bucket <name>
        Outputs PLAIN TEXT — parsed with parse_bucket_sync_status_text().
        """
        logger.debug("Collecting bucket sync status for '%s' (TEXT command)", bucket)
        result = run_cli_raw(
            ["bucket", "sync", "status", "--bucket", bucket],
            timeout=30
        )
        if is_error(result):
            logger.debug("Bucket sync status failed for '%s': %s",
                         bucket, result.get("error"))
            return

        parsed = parse_bucket_sync_status_text(result["text"])
        if parsed.get("sync_disabled"):
            logger.debug("Bucket '%s': sync disabled or no sync sources", bucket)
        else:
            sources = parsed.get("sources", [])
            logger.debug("Bucket '%s': %d sync source(s)", bucket, len(sources))
            for src in sources:
                logger.debug("  Source '%s': %s (full=%d/%d, incr=%d/%d)",
                             src.get("source_zone", "?"), src.get("status", "?"),
                             src.get("full_sync_done", 0), src.get("full_sync_total", 0),
                             src.get("incremental_sync_done", 0), src.get("incremental_sync_total", 0))

        # Attach to the latest snapshot for this bucket
        history = self.store.bucket_history.get(bucket, [])
        if history:
            history[-1]["sync_status"] = parsed

    # ------------------------------------------------------------------ #
    #  Global Sync Status (TEXT command — NOT JSON)
    # ------------------------------------------------------------------ #

    def _collect_sync_status(self, ts: str):
        """
        radosgw-admin sync status
        Outputs PLAIN TEXT — parsed with parse_sync_status_text().
        """
        result = run_cli_raw(["sync", "status"], timeout=30)

        if is_error(result):
            logger.warning("Global sync status failed: %s", result.get("error", "unknown"))
            self.store.add_global_snapshot({
                "timestamp": ts,
                "status": "error",
                "error": result.get("error", "unknown"),
                "raw_text": result.get("stdout", ""),
            })
            return

        parsed = parse_sync_status_text(result["text"])
        parsed["timestamp"] = ts
        parsed["status"] = "ok"
        parsed["raw_text"] = result["text"]

        logger.info("Global sync: realm=%s, zone=%s, metadata=%s, data_sources=%d",
                     parsed.get("realm", "?"), parsed.get("zone", "?"),
                     parsed.get("metadata_sync", {}).get("status", "?"),
                     len(parsed.get("data_sync", [])))
        for ds in parsed.get("data_sync", []):
            logger.debug("  Data sync from '%s': %s (full=%d/%d, incr=%d/%d)",
                         ds.get("source_zone", "?"), ds.get("status", "?"),
                         ds.get("full_sync_done", 0), ds.get("full_sync_total", 0),
                         ds.get("incremental_sync_done", 0), ds.get("incremental_sync_total", 0))

        self.store.add_global_snapshot(parsed)

    # ------------------------------------------------------------------ #
    #  Sync Errors (JSON command)
    # ------------------------------------------------------------------ #

    def _collect_sync_errors(self, ts: str):
        """
        Collect sync errors from: radosgw-admin sync error list --format=json

        Actual output structure:
        [
          {
            "shard_id": 0,
            "entries": [
              {
                "id": "...",
                "section": "data",
                "name": "bucket_name:zone_id.xxxxx:shard[N]",
                "timestamp": "2025-09-25T06:09:39.488676Z",
                "info": {
                  "source_zone": "8b96aea5-...",
                  "error_code": 13,
                  "message": "failed to sync bucket instance: (13) Permission denied"
                }
              }
            ]
          },
          { "shard_id": 1, "entries": [] },
          ...
        ]
        """
        result = run_cli_json(["sync", "error", "list"])

        global_errors = []
        bucket_errors = defaultdict(list)

        if is_error(result):
            logger.debug("sync error list returned error: %s", result.get("error"))
            self.store.set_global_errors([])
            return

        # Top-level is a list of shards
        shards = []
        if isinstance(result, list):
            shards = result
        elif isinstance(result, dict):
            # Some versions might wrap in a dict
            shards = result.get("shards", result.get("entries", []))

        for shard in shards:
            if not isinstance(shard, dict):
                continue

            shard_id = shard.get("shard_id", "")
            entries = shard.get("entries", [])

            for entry in entries:
                if not isinstance(entry, dict):
                    continue

                # Extract bucket name from "name" field
                # Format: "bucket_name:zone_id.xxxxx:shard[N]"
                raw_name = entry.get("name", "")
                bucket_name = ""
                if raw_name:
                    # Split on first ':' — everything before it is the bucket
                    parts = raw_name.split(":", 1)
                    bucket_name = parts[0] if parts else ""

                # Error details are inside "info" sub-dict
                info = entry.get("info", {})

                error = {
                    "shard_id": shard_id,
                    "entry_id": entry.get("id", ""),
                    "section": entry.get("section", ""),
                    "raw_name": raw_name,
                    "timestamp": entry.get("timestamp", ts),
                    "bucket": bucket_name,
                    "source_zone": info.get("source_zone", ""),
                    "error_code": info.get("error_code", "unknown"),
                    "message": info.get("message", ""),
                }
                global_errors.append(error)
                if bucket_name:
                    bucket_errors[bucket_name].append(error)

        self.store.set_global_errors(global_errors)
        for bkt, errs in bucket_errors.items():
            self.store.set_bucket_errors(bkt, errs)

        logger.info("Collected %d sync error(s) across %d bucket(s)",
                     len(global_errors), len(bucket_errors))

    # ------------------------------------------------------------------ #
    #  Utilities
    # ------------------------------------------------------------------ #

    @staticmethod
    def _parse_bucket_stats(stats_list: list) -> dict:
        parsed = {}
        for item in stats_list:
            if not isinstance(item, dict):
                continue
            name = item.get("bucket", "")
            if not name:
                continue
            usage = item.get("usage", {})
            rgw_main = usage.get("rgw.main", {})

            parsed[name] = {
                "num_objects": rgw_main.get("num_objects", 0),
                "size_kb": rgw_main.get("size_kb", 0),
                "size_actual": rgw_main.get("size_kb_actual", 0) * 1024
                if "size_kb_actual" in rgw_main
                else rgw_main.get("size", 0),
                "num_shards": item.get("num_shards", 0),
                "bucket_quota": item.get("bucket_quota", {}),
                "zonegroup": item.get("zonegroup", ""),
                "placement_rule": item.get("placement_rule", ""),
                "marker": item.get("marker", ""),
                "id": item.get("id", ""),
            }
        return parsed

    # ------------------------------------------------------------------ #
    #  Run loop
    # ------------------------------------------------------------------ #

    def run(self, interval: int = 60):
        logger.info("Starting collection loop (%ds interval)", interval)
        while not self._stop.is_set():
            self.collect_once()
            self._stop.wait(interval)

    def stop(self):
        self._stop.set()