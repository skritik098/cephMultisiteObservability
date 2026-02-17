#!/usr/bin/env python3
"""
Step-by-Step Component Tester
==============================
Tests each piece of the collector individually on a real Ceph cluster.
Reads config.yaml to test CLI vs REST modes exactly as the main server does.

Usage:
  python3 test_steps.py                    # Run all steps interactively
  python3 test_steps.py --step 3           # Run only step 3
  python3 test_steps.py --step 1-4         # Run steps 1 through 4
  python3 test_steps.py --list             # List all steps
  python3 test_steps.py --auto             # Run all steps non-interactively
  python3 test_steps.py --verbose          # Show INFO-level collector logs
  python3 test_steps.py --debug            # Show DEBUG logs (CLI commands, parser output)
  python3 test_steps.py --config /path/to/config.yaml  # Use specific config file

Steps:
  1. Validate radosgw-admin binary exists
  2. Validate cluster access (realm get)
  3. Topology discovery (realm → period → zones)
  4. Bucket stats (JSON) — primary zone
  5. Bucket stats — secondary zone (CLI or REST per config.yaml)
  6. Global sync status (TEXT) — raw output + parser test
  7. Bucket sync status (TEXT) — raw output + parser test
  8. Sync error list (JSON)
  9. Full collection cycle (uses config.yaml for all settings)
"""

import sys
import os
import json
import shutil
import argparse
import subprocess
import textwrap
from datetime import datetime

# Ensure the backend module is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def load_test_config(config_path=None):
    """
    Load config.yaml for test steps. Searches in order:
      1. Explicit --config path
      2. ../config.yaml (project root)
      3. ./config.yaml (current dir)
    Returns dict with config values + _config_path metadata.
    """
    search_paths = []
    if config_path:
        search_paths.append(config_path)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    search_paths.extend([
        os.path.join(project_dir, "config.yaml"),
        os.path.join(script_dir, "config.yaml"),
        "config.yaml",
    ])

    for path in search_paths:
        if os.path.exists(path):
            try:
                import yaml
                with open(path, "r") as f:
                    config = yaml.safe_load(f) or {}
                config["_config_path"] = os.path.abspath(path)
                return config
            except ImportError:
                # No PyYAML — try basic parsing
                config = _parse_yaml_basic(path)
                config["_config_path"] = os.path.abspath(path)
                return config
            except Exception as e:
                print(f"  Warning: Failed to load {path}: {e}")

    return {"_config_path": "not found (using defaults)"}


def _parse_yaml_basic(path):
    """Fallback YAML parser for simple key: value files (no PyYAML needed)."""
    config = {}
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" in line:
                key, val = line.split(":", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if val.lower() in ("true", "yes"):
                    config[key] = True
                elif val.lower() in ("false", "no"):
                    config[key] = False
                elif val.isdigit():
                    config[key] = int(val)
                else:
                    config[key] = val
    return config

# ================================================================== #
#  Formatting Helpers
# ================================================================== #

RED = "\033[0;31m"
GREEN = "\033[0;32m"
YELLOW = "\033[1;33m"
CYAN = "\033[0;36m"
BOLD = "\033[1m"
DIM = "\033[2m"
NC = "\033[0m"


def banner(step_num, title):
    print()
    print(f"{CYAN}{'═' * 60}{NC}")
    print(f"{CYAN}  STEP {step_num}: {BOLD}{title}{NC}")
    print(f"{CYAN}{'═' * 60}{NC}")
    print()


def ok(msg):
    print(f"  {GREEN}✓{NC} {msg}")


def fail(msg):
    print(f"  {RED}✗{NC} {msg}")


def warn(msg):
    print(f"  {YELLOW}⚠{NC} {msg}")


def info(msg):
    print(f"  {DIM}ℹ{NC} {msg}")


def show_raw(label, text, max_lines=30):
    """Display raw output in a bordered box."""
    print()
    print(f"  {DIM}┌── {label} ──{'─' * max(0, 40 - len(label))}┐{NC}")
    lines = text.splitlines()
    for line in lines[:max_lines]:
        print(f"  {DIM}│{NC} {line}")
    if len(lines) > max_lines:
        print(f"  {DIM}│ ... ({len(lines) - max_lines} more lines){NC}")
    print(f"  {DIM}└{'─' * 50}┘{NC}")
    print()


def show_json(label, data, indent=2):
    """Display parsed JSON/dict nicely."""
    print()
    print(f"  {DIM}┌── {label} ──{'─' * max(0, 40 - len(label))}┐{NC}")
    formatted = json.dumps(data, indent=indent, default=str)
    for line in formatted.splitlines()[:40]:
        print(f"  {DIM}│{NC} {line}")
    print(f"  {DIM}└{'─' * 50}┘{NC}")
    print()


def ask_continue():
    """Ask user to continue or abort."""
    try:
        resp = input(f"  {CYAN}Press Enter to continue, 'q' to quit → {NC}")
        if resp.strip().lower() in ('q', 'quit', 'exit'):
            print(f"\n  {YELLOW}Aborted by user.{NC}\n")
            sys.exit(0)
    except (EOFError, KeyboardInterrupt):
        print(f"\n  {YELLOW}Aborted.{NC}\n")
        sys.exit(0)


# ================================================================== #
#  Step Implementations
# ================================================================== #

def step_1_binary_check(auto=False, config=None):
    """Step 1: Check radosgw-admin binary exists on PATH."""
    banner(1, "Verify radosgw-admin Binary")

    info("Checking if 'radosgw-admin' is on PATH...")

    path = shutil.which("radosgw-admin")
    if path:
        ok(f"Found at: {path}")
        # Show version
        try:
            ver = subprocess.run(
                ["radosgw-admin", "--version"],
                capture_output=True, text=True, timeout=5
            )
            if ver.stdout.strip():
                ok(f"Version: {ver.stdout.strip()}")
            elif ver.stderr.strip():
                ok(f"Version: {ver.stderr.strip()}")
        except Exception:
            warn("Could not get version")
        return True
    else:
        fail("'radosgw-admin' NOT found on PATH")
        fail("This tool must run on a Ceph admin/mon node.")
        return False


def step_2_cluster_access(auto=False, config=None):
    """Step 2: Test actual cluster connectivity."""
    banner(2, "Verify Cluster Access")

    info("Running: radosgw-admin realm get --format=json")
    info("This tests both RADOS connectivity and multisite config...")

    try:
        proc = subprocess.run(
            ["radosgw-admin", "realm", "get", "--format=json"],
            capture_output=True, text=True, timeout=15
        )
    except subprocess.TimeoutExpired:
        fail("Command timed out after 15s — MONs may be unreachable")
        return False

    if proc.returncode == 0:
        ok("Command succeeded (rc=0)")
        show_raw("stdout (first 500 chars)", proc.stdout[:500])

        # Try parsing
        try:
            data = json.loads(proc.stdout.strip())
            ok(f"JSON parsed successfully — realm name: {data.get('name', '?')}")
            return True
        except json.JSONDecodeError as e:
            warn(f"stdout is not valid JSON: {e}")
            warn("Cluster is reachable but output unexpected")
            return True  # Still reachable
    else:
        stderr = proc.stderr.strip()
        if "no realm" in stderr.lower():
            warn("Cluster reachable but NO REALM configured")
            warn(f"stderr: {stderr}")
            info("Multisite may not be set up. Some tests will fail.")
            return True  # Cluster accessible, just no multisite
        else:
            fail(f"Command failed (rc={proc.returncode})")
            show_raw("stderr", stderr)
            return False


def step_3_topology_discovery(auto=False, config=None):
    """Step 3: Full topology discovery."""
    banner(3, "Topology Discovery (realm → period → zones)")

    from collector import run_cli_json, is_error, MultisiteTopology

    # 3a: realm get
    info("3a) radosgw-admin realm get")
    realm = run_cli_json(["realm", "get"])
    if is_error(realm):
        warn(f"realm get failed: {realm.get('error')}")
    else:
        ok(f"Realm: {realm.get('name', '?')}")
        show_json("realm get output", realm)

    # 3b: period get
    info("3b) radosgw-admin period get")
    period = run_cli_json(["period", "get"])
    if is_error(period):
        warn(f"period get failed: {period.get('error')}")
        info("Trying zonegroup get as fallback...")
        zg = run_cli_json(["zonegroup", "get"])
        if is_error(zg):
            fail(f"zonegroup get also failed: {zg.get('error')}")
            return False
        else:
            show_json("zonegroup get output (keys only)", {k: "..." for k in zg.keys()})
    else:
        ok("Period fetched successfully")
        # Show zone summary
        period_map = period.get("period_map", period)
        zg_count = len(period_map.get("zonegroups", []))
        zone_count = sum(
            len(zg.get("zones", []))
            for zg in period_map.get("zonegroups", [])
        )
        ok(f"Found {zg_count} zonegroup(s) with {zone_count} zone(s)")

    # 3c: Full discovery via MultisiteTopology
    info("3c) Running full MultisiteTopology.discover()...")
    try:
        topo = MultisiteTopology()
        topo.discover()
        ok("Topology discovery succeeded")
        show_json("Discovered topology", topo.to_dict())
        return True
    except Exception as e:
        fail(f"Discovery failed: {e}")
        return False


def step_4_bucket_stats(auto=False, config=None):
    """Step 4: Bucket stats (JSON command)."""
    banner(4, "Bucket Stats — Primary Zone (JSON)")

    from collector import run_cli_json, is_error, SyncCollector

    info("Running: radosgw-admin bucket stats --format=json")
    result = run_cli_json(["bucket", "stats"])

    if is_error(result):
        fail(f"bucket stats failed: {result.get('error')}")
        return False

    # Normalize
    if isinstance(result, dict):
        result = [result]

    ok(f"Got stats for {len(result)} bucket(s)")

    if result:
        # Show first bucket raw
        show_json(f"First bucket raw", result[0])

        # Test parser
        info("Testing _parse_bucket_stats()...")
        parsed = SyncCollector._parse_bucket_stats(result)
        ok(f"Parsed {len(parsed)} bucket(s)")
        for name, stats in list(parsed.items())[:3]:
            print(f"    {BOLD}{name}{NC}: "
                  f"{stats['num_objects']} objects, "
                  f"{stats['size_kb']} KB, "
                  f"{stats['num_shards']} shards")
        if len(parsed) > 3:
            info(f"... and {len(parsed) - 3} more")

    return True


def step_5_bucket_stats_secondary(auto=False, config=None):
    """Step 5: Bucket stats for secondary zone — method chosen by config."""
    config = config or {}
    use_rest = config.get("use_rest_for_bucket_stats", False)

    if use_rest:
        banner(5, "Bucket Stats — Secondary Zone (REST API — from config)")
    else:
        banner(5, "Bucket Stats — Secondary Zone (CLI --rgw-zone — from config)")

    from collector import run_cli_json, is_error, MultisiteTopology, RGWRestAPI, SyncCollector

    # Show what config says
    info(f"Config: use_rest_for_bucket_stats = {use_rest}")
    if use_rest:
        ak = config.get("access_key", "")
        sk = config.get("secret_key", "")
        info(f"Config: access_key = {ak[:8]}..." if ak else "Config: access_key = NOT SET")
        info(f"Config: secret_key = {'***' if sk else 'NOT SET'}")

    # Discover zones
    info("Discovering zones to find a secondary...")
    try:
        topo = MultisiteTopology()
        topo.discover()
    except Exception as e:
        warn(f"Topology discovery failed: {e}")
        info("Skipping this step — no secondary zones discoverable")
        return True

    if not topo.secondary_zones:
        warn("No secondary zones found — nothing to test")
        info("This is expected for single-zone setups")
        return True

    sec = topo.secondary_zones[0]
    info(f"Secondary zone: {sec['name']}")
    info(f"Endpoints: {sec.get('endpoints', [])}")

    # --- Test A: Always test CLI method first ---
    info("")
    info(f"5a) Testing CLI method: radosgw-admin bucket stats --rgw-zone {sec['name']}")
    cli_result = run_cli_json(["bucket", "stats", "--rgw-zone", sec["name"]])
    cli_ok = not is_error(cli_result)

    if cli_ok:
        if isinstance(cli_result, dict):
            cli_result = [cli_result]
        parsed = SyncCollector._parse_bucket_stats(cli_result)
        ok(f"CLI: got {len(parsed)} bucket(s) from zone '{sec['name']}'")
        for name, stats in list(parsed.items())[:3]:
            print(f"      {name}: {stats['num_objects']} objects, {stats['size_kb']} KB")
    else:
        warn(f"CLI: --rgw-zone {sec['name']} failed: {cli_result.get('error')}")
        if use_rest:
            info("Will fall back to REST API as configured")
        else:
            warn("Consider enabling use_rest_for_bucket_stats in config.yaml")

    # --- Test B: Test REST method if configured ---
    if use_rest:
        info("")
        info(f"5b) Testing REST API access (pre-flight validation)")
        ak = config.get("access_key", "")
        sk = config.get("secret_key", "")

        if not ak or not sk:
            fail("REST mode enabled but access_key/secret_key not set in config.yaml")
            return False

        # Test ALL zones with endpoints
        all_zones = [sec] + topo.secondary_zones[1:]  # secondary first, then others
        # Also include master zone if it has endpoints (useful to validate creds)
        if topo.master_zone and topo.master_zone.get("endpoints"):
            all_zones.append(topo.master_zone)

        rest_ok_count = 0
        for z in all_zones:
            eps = z.get("endpoints", [])
            if not eps:
                warn(f"Zone '{z['name']}': no endpoints — skipping REST test")
                continue

            endpoint = eps[0]
            info(f"  Validating: {z['name']} → {endpoint}")
            info(f"  access_key: {ak[:8]}...")

            api = RGWRestAPI(endpoint, ak, sk, config.get("verify_ssl", False))
            check = api.validate_access()

            if check["ok"]:
                ok(f"  ✓ Zone '{z['name']}': REST access validated (HTTP 200)")
                rest_ok_count += 1
            else:
                fail(f"  ✗ Zone '{z['name']}': {check['error']} (HTTP {check.get('status', '?')})")
                if "SignatureMismatch" in str(check.get("error", "")):
                    info("")
                    info("  DIAGNOSIS: SignatureMismatch means the access_key or secret_key")
                    info("  in config.yaml does not match what the RGW zone expects.")
                    info("  Fix: verify with: radosgw-admin user info --uid=<sync-user>")
                    info("  and update config.yaml with the correct keys.")
                elif "AccessDenied" in str(check.get("error", "")):
                    info("")
                    info("  DIAGNOSIS: User exists but lacks admin caps.")
                    info("  Fix: radosgw-admin caps add --uid=<user> --caps='buckets=read;metadata=read'")

        if rest_ok_count == 0:
            fail("REST validation failed for ALL zones")
            info("The collector will fall back to CLI for all zones.")
            info("Fix the credentials or endpoints above, or set use_rest_for_bucket_stats: false")
            return False

        # Now fetch actual data from the validated endpoint
        info("")
        info(f"5c) Fetching bucket stats via REST API for zone '{sec['name']}'")
        api = RGWRestAPI(sec["endpoints"][0], ak, sk, config.get("verify_ssl", False))
        rest_result = api.get_bucket_stats()

        if isinstance(rest_result, dict) and rest_result.get("_error"):
            fail(f"REST data fetch failed: {rest_result.get('error', 'unknown')}")
            return False

        if isinstance(rest_result, dict):
            rest_result = [rest_result]

        parsed = SyncCollector._parse_bucket_stats(rest_result)
        ok(f"REST: got {len(parsed)} bucket(s) from zone '{sec['name']}'")
        for name, stats in list(parsed.items())[:3]:
            print(f"      {name}: {stats['num_objects']} objects, {stats['size_kb']} KB")
    elif not cli_ok:
        info("")
        info("TIP: If CLI --rgw-zone doesn't work for your setup, enable REST mode:")
        info("  In config.yaml:")
        info("    use_rest_for_bucket_stats: true")
        info("    access_key: \"YOUR_KEY\"")
        info("    secret_key: \"YOUR_SECRET\"")

    return True


def step_6_global_sync_status(auto=False, config=None):
    """Step 6: Global sync status (TEXT — not JSON)."""
    banner(6, "Global Sync Status (TEXT output)")

    from collector import run_cli_raw, is_error, parse_sync_status_text

    info("Running: radosgw-admin sync status")
    info("NOTE: This command outputs PLAIN TEXT, not JSON!")
    info("      --format=json is intentionally NOT used.")

    result = run_cli_raw(["sync", "status"], timeout=30)

    if is_error(result):
        fail(f"sync status failed: {result.get('error')}")
        if result.get("stdout"):
            show_raw("stdout (even though rc!=0)", result["stdout"])
        return False

    raw_text = result["text"]
    ok("Command succeeded")
    show_raw("Raw text output", raw_text)

    # Test parser
    info("Testing parse_sync_status_text()...")
    parsed = parse_sync_status_text(raw_text)
    show_json("Parsed result", parsed)

    ok(f"Realm: {parsed.get('realm', '?')}")
    ok(f"Zone: {parsed.get('zone', '?')}")

    meta = parsed.get("metadata_sync", {})
    if meta:
        ok(f"Metadata sync status: {meta.get('status', '?')}")
        ok(f"  Full: {meta.get('full_sync_done')}/{meta.get('full_sync_total')} shards")
        ok(f"  Incremental: {meta.get('incremental_sync_done')}/{meta.get('incremental_sync_total')} shards")

    for ds in parsed.get("data_sync", []):
        ok(f"Data sync from '{ds.get('source_zone', '?')}': {ds.get('status', '?')}")
        ok(f"  Full: {ds.get('full_sync_done')}/{ds.get('full_sync_total')} shards")
        ok(f"  Incremental: {ds.get('incremental_sync_done')}/{ds.get('incremental_sync_total')} shards")

    if not parsed.get("metadata_sync") and not parsed.get("data_sync"):
        warn("No metadata_sync or data_sync blocks found in output.")
        warn("Review the raw text above — parser may need adjustment for your format.")

    return True


def step_7_bucket_sync_status(auto=False, config=None):
    """Step 7: Bucket sync status (TEXT — not JSON)."""
    banner(7, "Bucket Sync Status — Per Bucket (TEXT output)")

    from collector import run_cli_json, run_cli_raw, is_error, parse_bucket_sync_status_text

    # First get a bucket name
    info("Getting a bucket name from 'bucket stats'...")
    stats = run_cli_json(["bucket", "stats"])
    if is_error(stats):
        fail(f"Cannot get bucket list: {stats.get('error')}")
        return False

    if isinstance(stats, dict):
        stats = [stats]

    if not stats:
        warn("No buckets found")
        return True

    bucket_name = stats[0].get("bucket", "")
    if not bucket_name:
        fail("First bucket has no name")
        return False

    info(f"Testing with bucket: {bucket_name}")
    info(f"Running: radosgw-admin bucket sync status --bucket {bucket_name}")
    info("NOTE: This outputs PLAIN TEXT, not JSON!")

    result = run_cli_raw(
        ["bucket", "sync", "status", "--bucket", bucket_name],
        timeout=30
    )

    if is_error(result):
        # Some versions return rc!=0 for disabled sync but still have stdout
        stdout = result.get("stdout", "")
        if stdout and ("sync is disabled" in stdout.lower() or "no sync sources" in stdout.lower()):
            warn("Command returned non-zero but has useful output")
            show_raw("stdout", stdout)
            parsed = parse_bucket_sync_status_text(stdout)
            show_json("Parsed result", parsed)
            if parsed.get("sync_disabled"):
                ok(f"Parser correctly detected: sync disabled for '{bucket_name}'")
            return True
        fail(f"bucket sync status failed: {result.get('error')}")
        return False

    raw_text = result["text"]
    ok("Command succeeded")
    show_raw("Raw text output", raw_text)

    # Test parser
    info("Testing parse_bucket_sync_status_text()...")
    parsed = parse_bucket_sync_status_text(raw_text)
    show_json("Parsed result", parsed)

    if parsed.get("sync_disabled"):
        ok(f"Sync is DISABLED for bucket '{bucket_name}'")
    else:
        ok(f"Bucket: {parsed.get('bucket', '?')}")
        for src in parsed.get("sources", []):
            ok(f"  Source zone: {src.get('source_zone', '?')} — {src.get('status', '?')}")
            ok(f"    Full: {src.get('full_sync_done')}/{src.get('full_sync_total')} shards")
            ok(f"    Incremental: {src.get('incremental_sync_done')}/{src.get('incremental_sync_total')} shards")
            if src.get("shard_details"):
                for sd in src["shard_details"][:5]:
                    info(f"    Shard {sd['shard_id']}: {sd['status']}")

    # Test with more buckets if available
    if len(stats) > 1 and not auto:
        info(f"\nYou have {len(stats)} buckets. Want to test another?")
        for i, s in enumerate(stats[:8]):
            print(f"    [{i}] {s.get('bucket', '?')}")
        try:
            choice = input(f"  {CYAN}Enter number (or Enter to skip) → {NC}")
            if choice.strip().isdigit():
                idx = int(choice.strip())
                if 0 <= idx < len(stats):
                    extra_bucket = stats[idx].get("bucket", "")
                    info(f"Testing: {extra_bucket}")
                    r2 = run_cli_raw(
                        ["bucket", "sync", "status", "--bucket", extra_bucket],
                        timeout=30
                    )
                    if not is_error(r2):
                        show_raw(f"Raw output for {extra_bucket}", r2["text"])
                        p2 = parse_bucket_sync_status_text(r2["text"])
                        show_json("Parsed", p2)
                    else:
                        stdout = r2.get("stdout", "")
                        if stdout:
                            show_raw("stdout", stdout)
                            p2 = parse_bucket_sync_status_text(stdout)
                            show_json("Parsed", p2)
                        else:
                            warn(f"Failed: {r2.get('error')}")
        except (EOFError, KeyboardInterrupt):
            pass

    return True


def step_8_sync_errors(auto=False, config=None):
    """Step 8: Sync error list (JSON) — shard→entries→info structure."""
    banner(8, "Sync Error List (JSON)")

    from collector import run_cli_json, is_error

    info("Running: radosgw-admin sync error list --format=json")
    result = run_cli_json(["sync", "error", "list"])

    if is_error(result):
        stderr = result.get("error", "")
        if "doesn't exist" in stderr.lower() or not stderr:
            ok("No sync errors (error list empty or doesn't exist)")
            return True
        fail(f"sync error list failed: {stderr}")
        return False

    # Validate structure: should be a list of shards
    if not isinstance(result, list):
        warn(f"Expected a list of shards, got: {type(result).__name__}")
        show_json("Raw result (first 500 chars)", str(result)[:500])
        return False

    ok(f"Got {len(result)} shard(s) in output")

    # Count actual errors across all shards
    total_errors = 0
    errors_by_bucket = {}
    sample_entry = None

    for shard in result:
        if not isinstance(shard, dict):
            continue
        shard_id = shard.get("shard_id", "?")
        entries = shard.get("entries", [])
        if entries:
            info(f"  Shard {shard_id}: {len(entries)} error(s)")
        for entry in entries:
            total_errors += 1
            if sample_entry is None:
                sample_entry = entry

            # Extract bucket from "name" field (format: bucket:zone_id:shard[N])
            raw_name = entry.get("name", "")
            bucket = raw_name.split(":", 1)[0] if raw_name else "_unknown"
            errors_by_bucket.setdefault(bucket, 0)
            errors_by_bucket[bucket] += 1

    ok(f"Total errors across all shards: {total_errors}")

    if sample_entry:
        show_json("Sample error entry", sample_entry)

        # Validate expected fields
        has_info = "info" in sample_entry
        has_name = "name" in sample_entry
        has_ts = "timestamp" in sample_entry

        if has_info:
            ok("Entry has 'info' sub-dict with error details")
            entry_info = sample_entry["info"]
            ok(f"  source_zone: {entry_info.get('source_zone', '?')}")
            ok(f"  error_code: {entry_info.get('error_code', '?')}")
            ok(f"  message: {entry_info.get('message', '?')[:80]}")
        else:
            warn("Entry does NOT have 'info' key — structure may differ from expected")

        if has_name:
            raw_name = sample_entry["name"]
            bucket = raw_name.split(":", 1)[0]
            ok(f"  Bucket extracted from 'name' field: '{bucket}'")
            ok(f"  Raw name: {raw_name[:80]}")
        else:
            warn("Entry does NOT have 'name' key")

    if errors_by_bucket:
        info("\nErrors by bucket:")
        for b, count in sorted(errors_by_bucket.items(), key=lambda x: -x[1]):
            print(f"    {b}: {count} error(s)")
    else:
        info("No errors found — cluster is clean")

    return True


def step_9_full_cycle(auto=False, config=None):
    """Step 9: Full collection cycle end-to-end."""
    banner(9, "Full Collection Cycle (All Together)")

    from collector import SyncCollector, SyncDataStore

    config = config or {}
    # Remove internal metadata key before passing to collector
    collector_config = {k: v for k, v in config.items() if not k.startswith("_")}

    info(f"Initializing collector with config:")
    info(f"  use_rest_for_bucket_stats: {collector_config.get('use_rest_for_bucket_stats', False)}")
    info(f"  collection_interval: {collector_config.get('collection_interval', 60)}s")
    if collector_config.get("use_rest_for_bucket_stats"):
        ak = collector_config.get("access_key", "")
        info(f"  access_key: {ak[:8]}..." if ak else "  access_key: NOT SET")
        info(f"  secret_key: {'***' if collector_config.get('secret_key') else 'NOT SET'}")

    store = SyncDataStore(max_snapshots=5)
    collector = SyncCollector(config=collector_config, store=store)

    try:
        collector.initialize()
        ok("Initialization succeeded")
    except Exception as e:
        fail(f"Initialization failed: {e}")
        return False

    topo = store.get_dashboard_data().get("topology", {})
    ok(f"Topology: {topo.get('realm', '?')} — "
       f"{len(topo.get('zones', []))} zone(s)")

    # Show which method is being used for each zone
    for zone in topo.get("zones", []):
        zn = zone.get("name", "?")
        if zone.get("is_master"):
            info(f"  Zone '{zn}' [MASTER]: bucket stats via CLI (always)")
        elif collector.zone_rest_apis.get(zn):
            info(f"  Zone '{zn}' [SECONDARY]: bucket stats via REST API → {zone.get('endpoints', ['?'])[0]}")
        else:
            info(f"  Zone '{zn}' [SECONDARY]: bucket stats via CLI (--rgw-zone {zn})")

    info("Running one full collection cycle...")
    collector.collect_once()

    data = store.get_dashboard_data()
    buckets = data.get("buckets", {})
    g_errors = data.get("global_errors", [])
    g_sync = data.get("global_sync", [])

    ok(f"Collected data for {len(buckets)} bucket(s)")
    ok(f"Global sync snapshots: {len(g_sync)}")
    ok(f"Global errors: {len(g_errors)}")

    is_single_zone = topo.get("is_single_zone", False)
    if is_single_zone:
        warn("Single-zone setup — sync progress will show N/A")

    if buckets:
        info("\nBucket summary:")
        print(f"    {'Bucket':<30s} {'Sync %':>8s} {'ΔObj':>8s} {'ΔSize':>10s} {'Errors':>6s}")
        print(f"    {'─' * 30} {'─' * 8} {'─' * 8} {'─' * 10} {'─' * 6}")
        for name, bdata in sorted(buckets.items()):
            hist = bdata.get("history", [])
            errs = bdata.get("errors", [])
            if hist:
                latest = hist[-1]
                pct = latest.get("sync_progress_pct")
                d_obj = latest.get("delta_objects", 0)
                d_size = latest.get("delta_size", 0)
                # Format size
                if d_size > 1024 * 1024 * 1024:
                    size_str = f"{d_size / 1024 / 1024 / 1024:.1f} GB"
                elif d_size > 1024 * 1024:
                    size_str = f"{d_size / 1024 / 1024:.1f} MB"
                elif d_size > 1024:
                    size_str = f"{d_size / 1024:.1f} KB"
                else:
                    size_str = f"{d_size} B"
                pct_str = f"{pct:>7.1f}%" if pct is not None else "    N/A"
                print(f"    {name:<30s} {pct_str} {d_obj:>8d} {size_str:>10s} {len(errs):>6d}")
            else:
                print(f"    {name:<30s} {'no data':>8s}")

    # Show a sample of the dashboard JSON
    info("\nSample dashboard API response (truncated):")
    sample = {
        "topology": data.get("topology"),
        "bucket_count": len(buckets),
        "global_sync_count": len(g_sync),
        "global_error_count": len(g_errors),
        "is_single_zone": is_single_zone,
        "last_update": data.get("last_update"),
    }
    show_json("Dashboard summary", sample)

    return True


# ================================================================== #
#  Main
# ================================================================== #

ALL_STEPS = [
    (1, "Verify radosgw-admin binary", step_1_binary_check),
    (2, "Verify cluster access", step_2_cluster_access),
    (3, "Topology discovery", step_3_topology_discovery),
    (4, "Bucket stats (primary zone, CLI)", step_4_bucket_stats),
    (5, "Bucket stats (secondary zone, per config)", step_5_bucket_stats_secondary),
    (6, "Global sync status (text parser)", step_6_global_sync_status),
    (7, "Bucket sync status (text parser)", step_7_bucket_sync_status),
    (8, "Sync error list (JSON)", step_8_sync_errors),
    (9, "Full collection cycle (uses config)", step_9_full_cycle),
]


def main():
    parser = argparse.ArgumentParser(
        description="Step-by-step component tester for RGW Multisite Monitor"
    )
    parser.add_argument("--step", type=str, default=None,
                        help="Run specific step(s): '3', '1-5', '6,7,8'")
    parser.add_argument("--list", action="store_true",
                        help="List all steps")
    parser.add_argument("--auto", action="store_true",
                        help="Non-interactive mode (no prompts)")
    parser.add_argument("--config", "-c", type=str, default=None,
                        help="Path to config.yaml (default: ../config.yaml)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show INFO-level logs from collector module")
    parser.add_argument("--debug", "-d", action="store_true",
                        help="Show DEBUG-level logs (CLI commands, parser output, decisions)")
    args = parser.parse_args()

    # --- Set up logging ---
    import logging
    log_level = logging.WARNING  # default: quiet
    if args.verbose:
        log_level = logging.INFO
    if args.debug:
        log_level = logging.DEBUG
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S"
    )

    if args.list:
        print(f"\n{BOLD}Available test steps:{NC}\n")
        for num, desc, _ in ALL_STEPS:
            print(f"  {CYAN}{num}{NC}. {desc}")
        print()
        return

    # --- Load configuration ---
    config = load_test_config(args.config)

    # Parse step selection
    steps_to_run = list(range(1, len(ALL_STEPS) + 1))
    if args.step:
        steps_to_run = []
        for part in args.step.split(","):
            if "-" in part:
                a, b = part.split("-", 1)
                steps_to_run.extend(range(int(a), int(b) + 1))
            else:
                steps_to_run.append(int(part))

    print()
    print(f"{BOLD}╔══════════════════════════════════════════════════════╗{NC}")
    print(f"{BOLD}║   RGW Multisite Monitor — Component Test Suite      ║{NC}")
    print(f"{BOLD}╚══════════════════════════════════════════════════════╝{NC}")
    print()
    print(f"  Running steps: {steps_to_run}")
    print(f"  Mode: {'automatic' if args.auto else 'interactive'}")
    print(f"  Log level: {'DEBUG' if args.debug else 'VERBOSE' if args.verbose else 'QUIET'}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Show loaded config
    print(f"  {DIM}┌── Configuration ──────────────────────────────────┐{NC}")
    print(f"  {DIM}│{NC} config file:              {config.get('_config_path', 'not found')}")
    print(f"  {DIM}│{NC} use_rest_for_bucket_stats: {config.get('use_rest_for_bucket_stats', False)}")
    print(f"  {DIM}│{NC} collection_interval:       {config.get('collection_interval', 60)}s")
    print(f"  {DIM}│{NC} verify_ssl:                {config.get('verify_ssl', False)}")
    if config.get("use_rest_for_bucket_stats"):
        ak = config.get("access_key", "")
        print(f"  {DIM}│{NC} access_key:               {ak[:8]}..." if ak else f"  {DIM}│{NC} access_key:               {RED}NOT SET{NC}")
        print(f"  {DIM}│{NC} secret_key:               {'***' if config.get('secret_key') else f'{RED}NOT SET{NC}'}")
    print(f"  {DIM}└───────────────────────────────────────────────────┘{NC}")
    print()

    results = {}
    for num, desc, func in ALL_STEPS:
        if num not in steps_to_run:
            continue

        passed = func(auto=args.auto, config=config)
        results[num] = passed

        if passed:
            print(f"\n  {GREEN}━━━ STEP {num} PASSED ━━━{NC}")
        else:
            print(f"\n  {RED}━━━ STEP {num} FAILED ━━━{NC}")

        if not args.auto:
            if num < max(steps_to_run):
                ask_continue()

    # Summary
    print()
    print(f"{BOLD}{'═' * 60}{NC}")
    print(f"{BOLD}  SUMMARY{NC}")
    print(f"{'═' * 60}")
    for num, desc, _ in ALL_STEPS:
        if num in results:
            status = f"{GREEN}PASS{NC}" if results[num] else f"{RED}FAIL{NC}"
            print(f"  Step {num}: [{status}] {desc}")
    print()

    failed = [n for n, p in results.items() if not p]
    if failed:
        print(f"  {RED}{len(failed)} step(s) failed: {failed}{NC}")
        print(f"  Fix the issues above before running the full monitor.")
    else:
        print(f"  {GREEN}All steps passed!{NC} The monitor should work correctly.")
        print(f"  Start it with: python3 api_server.py")
    print()


if __name__ == "__main__":
    main()