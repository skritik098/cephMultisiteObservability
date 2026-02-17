#!/bin/bash
# =====================================================
# Ceph RGW Multisite Monitor — Quick Setup
# =====================================================
# This tool MUST run on a Ceph admin/mon node where
# radosgw-admin is available and can reach the cluster.
# =====================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "╔══════════════════════════════════════════════════╗"
echo "║   Ceph RGW Multisite Monitor — Setup            ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ---- Check 1: radosgw-admin binary ----
echo "Checking prerequisites..."

if ! command -v radosgw-admin &>/dev/null; then
    echo -e "${RED}✗ FATAL: 'radosgw-admin' not found on PATH.${NC}"
    echo ""
    echo "  This tool must run on a Ceph admin/mon node where"
    echo "  radosgw-admin is installed."
    echo ""
    echo "  Install with:"
    echo "    RHEL/CentOS:  yum install ceph-radosgw"
    echo "    Debian/Ubuntu: apt install radosgw"
    echo ""
    exit 1
fi
echo -e "${GREEN}✓${NC} radosgw-admin found at: $(which radosgw-admin)"

# ---- Check 2: Ceph cluster access ----
echo -n "  Verifying Ceph cluster access... "
if ! radosgw-admin realm get --format=json &>/dev/null 2>&1; then
    # realm get failed — try to distinguish "no realm" from "no cluster"
    STDERR=$(radosgw-admin realm get --format=json 2>&1 || true)

    if echo "$STDERR" | grep -qi "could not init\|error connecting\|no such file\|keyring"; then
        echo -e "${RED}FAILED${NC}"
        echo ""
        echo -e "${RED}✗ FATAL: radosgw-admin cannot connect to the Ceph cluster.${NC}"
        echo ""
        echo "  Error: $STDERR"
        echo ""
        echo "  Verify with:  ceph status"
        echo "  Ensure /etc/ceph/ceph.conf and keyrings are present."
        echo ""
        exit 1
    else
        echo -e "${YELLOW}WARNING${NC}"
        echo -e "  ${YELLOW}⚠ realm get returned a non-zero exit code.${NC}"
        echo "    This might mean multisite is not fully configured."
        echo "    Error: $STDERR"
        echo ""
        echo "    Proceeding anyway — the collector will retry..."
        echo ""
    fi
else
    echo -e "${GREEN}OK${NC}"
fi

# ---- Check 3: Python ----
if ! command -v python3 &>/dev/null; then
    echo -e "${RED}✗ FATAL: python3 not found.${NC}"
    exit 1
fi
echo -e "${GREEN}✓${NC} python3 found"

# ---- Install Python dependencies ----
echo ""
echo "Installing Python dependencies..."
pip3 install -r backend/requirements.txt --quiet 2>/dev/null || \
pip install -r backend/requirements.txt --quiet 2>/dev/null || \
pip3 install -r backend/requirements.txt --break-system-packages --quiet 2>/dev/null

echo -e "${GREEN}✓${NC} Dependencies installed"

# ---- Build dashboard HTML ----
echo ""
echo "Building dashboard..."
python3 dashboard/build_html.py
echo -e "${GREEN}✓${NC} Dashboard built"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Multisite Topology Preview"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

REALM=$(radosgw-admin realm get --format=json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('name','?'))" 2>/dev/null || echo "unavailable")
echo "  Realm: $REALM"

# Show zones from period
radosgw-admin period get --format=json 2>/dev/null | python3 -c "
import sys, json
try:
    p = json.load(sys.stdin)
    pm = p.get('period_map', p)
    for zg in pm.get('zonegroups', []):
        master = zg.get('master_zone', '')
        for z in zg.get('zones', []):
            role = 'MASTER' if z.get('id') == master else 'SECONDARY'
            eps = ', '.join(z.get('endpoints', [])) or 'no endpoints'
            print(f\"  Zone: {z.get('name', '?'):20s} [{role:9s}]  {eps}\")
except:
    print('  (could not parse period — will retry at runtime)')
" 2>/dev/null || echo "  (period get not available — will discover at runtime)"

echo ""

# ---- Start ----
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Starting API Server"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "  Dashboard: http://localhost:5000          ← open this in your browser"
echo "  API:       http://localhost:5000/api/health"
echo "  Data API:  http://localhost:5000/api/dashboard"
echo "  Metrics:   http://localhost:5000/metrics"
echo ""
echo "  Config:    $SCRIPT_DIR/config.yaml"
echo "  Mode:      CLI (radosgw-admin)"
echo ""
echo "Press Ctrl+C to stop"
echo ""

cd "$SCRIPT_DIR"
python3 backend/api_server.py