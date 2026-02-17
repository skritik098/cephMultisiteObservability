# RGW Multisite Lab Setup — 2 Zones on a Single Cluster

## Overview

This guide creates a **two-zone multisite RGW deployment** on your existing
single 3-node Ceph cluster. Both zones share the same RADOS cluster but run
independent RGW daemons with different zone configurations — replicating
exactly how production multisite sync works.

```
┌───────────────────────────────────────────────────────┐
│              Single Ceph Cluster (3 nodes)             │
│                                                        │
│   ┌─────────────────┐      ┌──────────────────┐       │
│   │  Zone: zone1     │      │  Zone: zone2      │      │
│   │  (MASTER)        │      │  (SECONDARY)      │      │
│   │                  │      │                   │      │
│   │  RGW on node1    │ ───► │  RGW on node2     │      │
│   │  port 8001       │ sync │  port 8002        │      │
│   │                  │ ◄─── │                   │      │
│   └─────────────────┘      └──────────────────┘       │
│                                                        │
│   Realm: test_realm                                    │
│   Zonegroup: test_zg                                   │
│   Pools: zone1.rgw.* , zone2.rgw.*                     │
└───────────────────────────────────────────────────────┘
```

## Prerequisites

- 3-node Ceph cluster running (Reef or later)
- `cephadm` or direct `radosgw-admin` access
- Admin keyring on at least one node
- RGW package installed (`ceph-radosgw`)

## Variables — Set These First

```bash
# Adjust to match your environment
NODE1=ceph-node1    # hostname or IP — will run zone1 RGW (master)
NODE2=ceph-node2    # hostname or IP — will run zone2 RGW (secondary)
NODE3=ceph-node3    # hostname or IP — available for monitoring / extra RGW

REALM=test_realm
ZONEGROUP=test_zg
MASTER_ZONE=zone1
SECONDARY_ZONE=zone2

# RGW ports (different ports since same cluster)
MASTER_PORT=8001
SECONDARY_PORT=8002

# These get created in step 3
MASTER_ACCESS_KEY=""
MASTER_SECRET_KEY=""
```

---

## Step 1 — Create the Realm

The realm is the top-level container for multisite.

```bash
radosgw-admin realm create --rgw-realm=${REALM} --default
```

Verify:
```bash
radosgw-admin realm get --rgw-realm=${REALM}
# Should show realm name and id
```

## Step 2 — Create the Master Zonegroup and Zone

```bash
# Create the master zonegroup
radosgw-admin zonegroup create \
  --rgw-zonegroup=${ZONEGROUP} \
  --rgw-realm=${REALM} \
  --master --default \
  --endpoints=http://${NODE1}:${MASTER_PORT}

# Create the master zone inside it
radosgw-admin zone create \
  --rgw-zone=${MASTER_ZONE} \
  --rgw-zonegroup=${ZONEGROUP} \
  --rgw-realm=${REALM} \
  --master --default \
  --endpoints=http://${NODE1}:${MASTER_PORT}
```

## Step 3 — Create the Sync User

This system user handles zone-to-zone replication authentication.

```bash
radosgw-admin user create \
  --uid=sync-user \
  --display-name="Zone Sync User" \
  --system \
  --rgw-zone=${MASTER_ZONE} \
  --rgw-realm=${REALM}
```

**Capture the keys** from the output:
```bash
# Extract and save them
MASTER_ACCESS_KEY=$(radosgw-admin user info --uid=sync-user | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['keys'][0]['access_key'])")
MASTER_SECRET_KEY=$(radosgw-admin user info --uid=sync-user | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['keys'][0]['secret_key'])")

echo "Access Key: $MASTER_ACCESS_KEY"
echo "Secret Key: $MASTER_SECRET_KEY"
```

Now set these keys on the master zone:
```bash
radosgw-admin zone modify \
  --rgw-zone=${MASTER_ZONE} \
  --rgw-zonegroup=${ZONEGROUP} \
  --rgw-realm=${REALM} \
  --access-key=${MASTER_ACCESS_KEY} \
  --secret=${MASTER_SECRET_KEY}
```

## Step 4 — Create the Secondary Zone

```bash
radosgw-admin zone create \
  --rgw-zone=${SECONDARY_ZONE} \
  --rgw-zonegroup=${ZONEGROUP} \
  --rgw-realm=${REALM} \
  --endpoints=http://${NODE2}:${SECONDARY_PORT} \
  --access-key=${MASTER_ACCESS_KEY} \
  --secret=${MASTER_SECRET_KEY}
```

## Step 5 — Commit the Period

After any realm/zonegroup/zone changes you must commit the period:

```bash
radosgw-admin period update --commit \
  --rgw-realm=${REALM}
```

Verify the topology:
```bash
radosgw-admin period get --rgw-realm=${REALM} --format=json | \
  python3 -c "
import sys, json
p = json.load(sys.stdin)
pm = p.get('period_map', p)
for zg in pm.get('zonegroups', []):
    print(f\"Zonegroup: {zg['name']}\")
    mz = zg.get('master_zone', '')
    for z in zg.get('zones', []):
        role = 'MASTER' if z['id'] == mz else 'SECONDARY'
        print(f\"  Zone: {z['name']:20s} [{role}]  endpoints: {z.get('endpoints', [])}\")
"
```

You should see:
```
Zonegroup: test_zg
  Zone: zone1                [MASTER]     endpoints: ['http://ceph-node1:8001']
  Zone: zone2                [SECONDARY]  endpoints: ['http://ceph-node2:8002']
```

## Step 6 — Deploy RGW Daemons

### Option A: Using cephadm (recommended)

```bash
# Deploy zone1 RGW on node1
ceph orch apply rgw ${REALM}.${MASTER_ZONE} \
  --realm=${REALM} \
  --zone=${MASTER_ZONE} \
  --placement="${NODE1}" \
  --port=${MASTER_PORT}

# Deploy zone2 RGW on node2
ceph orch apply rgw ${REALM}.${SECONDARY_ZONE} \
  --realm=${REALM} \
  --zone=${SECONDARY_ZONE} \
  --placement="${NODE2}" \
  --port=${SECONDARY_PORT}
```

### Option B: Manual (non-cephadm)

If you're not using cephadm, add RGW config to `ceph.conf` and start manually:

```ini
# /etc/ceph/ceph.conf — add these sections

[client.rgw.zone1]
rgw_realm = test_realm
rgw_zonegroup = test_zg
rgw_zone = zone1
rgw_frontends = "beast port=8001"
host = ceph-node1

[client.rgw.zone2]
rgw_realm = test_realm
rgw_zonegroup = test_zg
rgw_zone = zone2
rgw_frontends = "beast port=8002"
host = ceph-node2
```

Then start:
```bash
# On node1:
radosgw -n client.rgw.zone1 --cluster=ceph

# On node2:
radosgw -n client.rgw.zone2 --cluster=ceph
```

## Step 7 — Verify RGW Daemons Are Running

```bash
# Check daemons
ceph orch ps --daemon-type rgw

# Or manually:
curl -s http://${NODE1}:${MASTER_PORT}  | head -5
curl -s http://${NODE2}:${SECONDARY_PORT} | head -5
# Both should return XML (anonymous S3 ListBucket error = good, means RGW is alive)
```

## Step 8 — Verify Sync Status

```bash
# On node running zone2 RGW (or with --rgw-zone=zone2):
radosgw-admin sync status --rgw-zone=${SECONDARY_ZONE}
```

Expected output:
```
        realm <id> (test_realm)
    zonegroup <id> (test_zg)
         zone <id> (zone2)
metadata sync syncing
              full sync: 0/64 shards
              incremental sync: 64/64 shards
              metadata is caught up with master
data sync source: <id> (zone1)
                  syncing
                  full sync: 0/128 shards
                  incremental sync: 128/128 shards
                  data is caught up with source
```

If you see "caught up" — sync is working.

## Step 9 — Create Test Buckets and Objects

```bash
# Install s3cmd or aws-cli if not already present
pip install awscli --break-system-packages 2>/dev/null || yum install -y awscli

# Configure for zone1 (master)
aws configure set aws_access_key_id ${MASTER_ACCESS_KEY}
aws configure set aws_secret_access_key ${MASTER_SECRET_KEY}

# Create test buckets on master zone
for i in 1 2 3 4 5; do
  aws --endpoint-url=http://${NODE1}:${MASTER_PORT} s3 mb s3://test-bucket-${i}
done

# Upload some test objects
for i in 1 2 3 4 5; do
  dd if=/dev/urandom bs=1K count=100 2>/dev/null | \
    aws --endpoint-url=http://${NODE1}:${MASTER_PORT} \
    s3 cp - s3://test-bucket-${i}/testfile-${i}.dat
done

echo "Waiting 10s for sync..."
sleep 10

# Verify on secondary zone
for i in 1 2 3 4 5; do
  echo -n "test-bucket-${i} on zone2: "
  aws --endpoint-url=http://${NODE2}:${SECONDARY_PORT} \
    s3 ls s3://test-bucket-${i}/ 2>/dev/null | wc -l
done
```

## Step 10 — Run the Monitor

Now that you have a working 2-zone setup:

```bash
cd rgw-multisite-monitor

# Test each component step by step
python3 backend/test_steps.py

# If all steps pass, start the monitor
./setup.sh
# Open http://<node>:5000 in browser
```

---

## Useful Commands for Testing

### Check what pools were created
```bash
ceph osd pool ls | grep rgw
# Should show: zone1.rgw.* and zone2.rgw.* pools
```

### Simulate sync lag (for testing the dashboard)
```bash
# Upload a large batch to master — secondary will take time to catch up
for i in $(seq 1 50); do
  dd if=/dev/urandom bs=10K count=1 2>/dev/null | \
    aws --endpoint-url=http://${NODE1}:${MASTER_PORT} \
    s3 cp - s3://test-bucket-1/bulk-${i}.dat
done
# Watch the dashboard — you should see sync % dip temporarily
```

### Force sync errors (for testing error collection)
```bash
# Disable sync on a specific bucket, then re-enable
radosgw-admin bucket sync disable --bucket=test-bucket-3
# Upload objects while sync is disabled
aws --endpoint-url=http://${NODE1}:${MASTER_PORT} \
  s3 cp /etc/hostname s3://test-bucket-3/while-disabled.txt
# Re-enable (may generate catch-up errors)
radosgw-admin bucket sync enable --bucket=test-bucket-3
```

### Monitor sync in real-time
```bash
watch -n 5 'radosgw-admin sync status --rgw-zone=zone2'
```

### Check bucket-level sync status
```bash
radosgw-admin bucket sync status --bucket=test-bucket-1
```

---

## Cleanup (when done testing)

```bash
# Remove RGW daemons
ceph orch rm rgw.${REALM}.${MASTER_ZONE}
ceph orch rm rgw.${REALM}.${SECONDARY_ZONE}

# Delete zones
radosgw-admin zone delete --rgw-zone=${SECONDARY_ZONE} --rgw-zonegroup=${ZONEGROUP} --rgw-realm=${REALM}
radosgw-admin zone delete --rgw-zone=${MASTER_ZONE} --rgw-zonegroup=${ZONEGROUP} --rgw-realm=${REALM}

# Delete zonegroup and realm
radosgw-admin zonegroup delete --rgw-zonegroup=${ZONEGROUP} --rgw-realm=${REALM}
radosgw-admin realm delete --rgw-realm=${REALM}

# Commit
radosgw-admin period update --commit 2>/dev/null

# Delete RGW pools (DESTRUCTIVE — only in lab)
for pool in $(ceph osd pool ls | grep -E '^(zone1|zone2)\.rgw'); do
  ceph osd pool rm $pool $pool --yes-i-really-really-mean-it
done
```

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `sync status` shows no sources | Period not committed | `radosgw-admin period update --commit` |
| `bucket sync status` says "sync disabled" | Bucket-level sync not enabled | `radosgw-admin bucket sync enable --bucket=X` |
| zone2 RGW won't start | Wrong zone config in ceph.conf | Verify `rgw_zone = zone2` in daemon config |
| "failed to sync: Permission denied" | Wrong access/secret keys | Re-set keys on zone with `zone modify` |
| Pools not created | RGW daemon hasn't started yet | Start RGW, it auto-creates pools on first run |
| `period get` fails | Realm not set as default | `radosgw-admin realm default --rgw-realm=test_realm` |