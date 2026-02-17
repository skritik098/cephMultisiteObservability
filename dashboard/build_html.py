#!/usr/bin/env python3
"""
Build a self-contained index.html from the React JSX component.

Creates a single HTML file with:
  - React 18 + ReactDOM 18 (CDN)
  - Babel Standalone (CDN, for in-browser JSX)
  - Feather Icons (CDN, replaces lucide-react)
  - Full dashboard code inline

Flask serves this at /
"""
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
JSX_PATH = os.path.join(SCRIPT_DIR, "RGWMultisiteMonitor.jsx")
OUT_PATH = os.path.join(SCRIPT_DIR, "index.html")


def build():
    with open(JSX_PATH, "r") as f:
        jsx_lines = f.readlines()

    # Strip all import statements (including multi-line ones)
    # and fix the "export default" on the App function
    body_lines = []
    in_import = False
    past_imports = False

    for line in jsx_lines:
        stripped = line.strip()

        # Once we're past the import block, include everything
        if past_imports:
            if line.startswith("export default function"):
                line = line.replace("export default function", "function", 1)
            body_lines.append(line)
            continue

        # Detect start of import statement
        if stripped.startswith("import "):
            in_import = True
            # Single-line import (has 'from' on same line)
            if " from " in stripped and stripped.endswith(";"):
                in_import = False
            continue

        # Inside a multi-line import
        if in_import:
            if stripped.startswith("} from ") or " from " in stripped:
                in_import = False  # End of this import
            continue

        # Skip blank lines between imports
        if stripped == "" and not body_lines:
            continue

        # First non-import, non-blank line — we're past imports
        past_imports = True
        body_lines.append(line)

    jsx_body = "".join(body_lines)

    # The HTML template
    html = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>RGW Multisite Monitor</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { background: #0c0e14; overflow-x: hidden; }
    @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
    #root { min-height: 100vh; }
  </style>
</head>
<body>
  <div id="root"></div>

  <!-- React 18 UMD -->
  <script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>

  <!-- Babel Standalone — compiles JSX in the browser -->
  <script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>

  <!-- Feather Icons — Lucide is a fork of Feather, icons are visually identical -->
  <script src="https://cdn.jsdelivr.net/npm/feather-icons/dist/feather.min.js"></script>

  <script type="text/babel">
    // ============================================================
    //  React Globals
    // ============================================================
    const { useState, useEffect, useCallback, useRef } = React;

    // ============================================================
    //  Icon Bridge: feather-icons → React components
    //  Feather SVGs use stroke="currentColor", so we set the CSS
    //  `color` property on the wrapper <span> and it cascades.
    // ============================================================
    const _FEATHER_MAP = {
      'RefreshCw':     'refresh-cw',
      'AlertTriangle': 'alert-triangle',
      'CheckCircle':   'check-circle',
      'Activity':      'activity',
      'TrendingUp':    'trending-up',
      'Database':      'database',
      'Server':        'server',
      'Settings':      'settings',
      'ChevronDown':   'chevron-down',
      'ChevronUp':     'chevron-up',
      'X':             'x',
      'Zap':           'zap',
      'Clock':         'clock',
      'Shield':        'shield',
      'Eye':           'eye',
      'BarChart3':     'bar-chart-2',
      'ArrowRight':    'arrow-right',
      'Wifi':          'wifi',
      'WifiOff':       'wifi-off',
      'ArrowUpDown':   'repeat',
      'Filter':        'filter',
      'AlertCircle':   'alert-circle',
      'Globe':         'globe',
      'Layers':        'layers',
      'HardDrive':     'hard-drive',
    };

    function _mkIcon(lucideName) {
      const featherName = _FEATHER_MAP[lucideName];
      if (!featherName) {
        // Fallback: convert CamelCase to kebab-case
        const kebab = lucideName.replace(/([A-Z])/g, '-$1').toLowerCase().replace(/^-/, '');
        return function FallbackIcon({ size = 24, color = 'currentColor', style = {} }) {
          return <span style={{ display: 'inline-flex', alignItems: 'center', width: size, height: size, color, ...style }}>?</span>;
        };
      }
      return function FeatherIcon({ size = 24, color = 'currentColor', style = {}, className, ...rest }) {
        const icon = feather.icons[featherName];
        if (!icon) return null;
        // Generate SVG string (feather sets stroke="currentColor")
        const svgStr = icon.toSvg({ width: size, height: size });
        return (
          <span
            style={{ display: 'inline-flex', alignItems: 'center', lineHeight: 0, color, ...style }}
            dangerouslySetInnerHTML={{ __html: svgStr }}
          />
        );
      };
    }

    const RefreshCw     = _mkIcon('RefreshCw');
    const AlertTriangle = _mkIcon('AlertTriangle');
    const CheckCircle   = _mkIcon('CheckCircle');
    const Activity      = _mkIcon('Activity');
    const TrendingUp    = _mkIcon('TrendingUp');
    const Database      = _mkIcon('Database');
    const Server        = _mkIcon('Server');
    const Settings      = _mkIcon('Settings');
    const ChevronDown   = _mkIcon('ChevronDown');
    const ChevronUp     = _mkIcon('ChevronUp');
    const X             = _mkIcon('X');
    const Zap           = _mkIcon('Zap');
    const Clock         = _mkIcon('Clock');
    const Shield        = _mkIcon('Shield');
    const Eye           = _mkIcon('Eye');
    const BarChart3     = _mkIcon('BarChart3');
    const ArrowRight    = _mkIcon('ArrowRight');
    const Wifi          = _mkIcon('Wifi');
    const WifiOff       = _mkIcon('WifiOff');
    const ArrowUpDown   = _mkIcon('ArrowUpDown');
    const Filter        = _mkIcon('Filter');
    const AlertCircle   = _mkIcon('AlertCircle');
    const Globe         = _mkIcon('Globe');
    const Layers        = _mkIcon('Layers');
    const HardDrive     = _mkIcon('HardDrive');

    // ============================================================
    //  Dashboard Application
    // ============================================================
""" + jsx_body + """
    // ============================================================
    //  Mount
    // ============================================================
    const root = ReactDOM.createRoot(document.getElementById('root'));
    root.render(<App />);
  </script>
</body>
</html>"""

    with open(OUT_PATH, "w") as f:
        f.write(html)

    size_kb = os.path.getsize(OUT_PATH) / 1024
    print(f"Built: {OUT_PATH} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    build()