"""
permitmap-tracker app.py
------------------------
Flask app for email open/click tracking.
Deploy to Render at permitmap-tracking.onrender.com

Persists tracking.csv to GitHub so data survives Render redeploys.

Required env vars on Render:
  GITHUB_TOKEN     — personal access token with repo write access
  GITHUB_REPO      — e.g. anandakeyclub-ops/permitmap-tracker
  GITHUB_BRANCH    — e.g. main
  STRIPE_URL       — default Stripe checkout URL for clicks

Endpoints:
  GET /pixel/<tracking_id>   — 1x1 GIF, logs open
  GET /click/<tracking_id>   — redirect to Stripe, logs click
  GET /stats                 — HTML dashboard
  GET /stats_json            — JSON stats for daily summary script
  GET /healthz               — health check
"""
from __future__ import annotations

import base64
import csv
import io
import json
import os
import threading
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, redirect, request

app = Flask(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
STRIPE_URL    = os.environ.get("STRIPE_URL", "https://buy.stripe.com/14AeVddOnbPx1g23VIdUY04")
GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO   = os.environ.get("GITHUB_REPO", "anandakeyclub-ops/permitmap-tracker")
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")
GITHUB_PATH   = "tracking.csv"

CSV_HEADERS = ["timestamp", "event", "tracking_id", "contractor_id",
               "send_type", "county", "trade", "ip", "user_agent"]

PIXEL_GIF = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00"
    b"!\xf9\x04\x00\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01"
    b"\x00\x00\x02\x02D\x01\x00;"
)

# In-memory buffer — flushed to GitHub every N writes or every M seconds
_buffer: list[dict] = []
_buffer_lock = threading.Lock()
_last_flush  = time.time()
FLUSH_EVERY_N = 5    # flush after this many events
FLUSH_EVERY_S = 120  # flush every 2 minutes regardless


# ── GitHub persistence ────────────────────────────────────────────────────────

def _github_headers() -> dict:
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }


def _get_remote_csv() -> tuple[str, str]:
    """Fetch current tracking.csv from GitHub. Returns (content_str, sha)."""
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_PATH}"
    try:
        r = requests.get(url, headers=_github_headers(),
                         params={"ref": GITHUB_BRANCH}, timeout=10)
        if r.status_code == 200:
            data    = r.json()
            content = base64.b64decode(data["content"]).decode("utf-8")
            return content, data["sha"]
        elif r.status_code == 404:
            # File doesn't exist yet — create with headers
            header_line = ",".join(CSV_HEADERS) + "\n"
            return header_line, ""
    except Exception as e:
        print(f"[tracker] GitHub fetch error: {e}")
    return ",".join(CSV_HEADERS) + "\n", ""


def _push_csv(content: str, sha: str) -> bool:
    """Push updated tracking.csv to GitHub."""
    if not GITHUB_TOKEN:
        print("[tracker] No GITHUB_TOKEN — skipping push")
        return False
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_PATH}"
    payload = {
        "message": f"tracking update {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}",
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "branch":  GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha
    try:
        r = requests.put(url, headers=_github_headers(),
                         json=payload, timeout=15)
        if r.status_code in (200, 201):
            return True
        print(f"[tracker] GitHub push failed: {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"[tracker] GitHub push error: {e}")
    return False


def _flush_buffer():
    """Append buffered rows to GitHub CSV."""
    global _last_flush
    with _buffer_lock:
        if not _buffer:
            _last_flush = time.time()
            return
        rows_to_write = list(_buffer)
        _buffer.clear()

    try:
        remote_csv, sha = _get_remote_csv()
        out = io.StringIO()
        # Preserve existing content, strip trailing newline
        existing = remote_csv.rstrip("\n")
        # If file only has headers or is empty, start fresh
        if not existing or existing == ",".join(CSV_HEADERS):
            out.write(",".join(CSV_HEADERS) + "\n")
        else:
            out.write(existing + "\n")
        writer = csv.DictWriter(out, fieldnames=CSV_HEADERS, extrasaction="ignore")
        for row in rows_to_write:
            writer.writerow(row)
        _push_csv(out.getvalue(), sha)
    except Exception as e:
        print(f"[tracker] Flush error: {e}")
        # Put rows back in buffer so they're not lost
        with _buffer_lock:
            _buffer.extend(rows_to_write)

    _last_flush = time.time()


def _maybe_flush(force: bool = False):
    with _buffer_lock:
        n = len(_buffer)
    elapsed = time.time() - _last_flush
    if force or n >= FLUSH_EVERY_N or elapsed >= FLUSH_EVERY_S:
        t = threading.Thread(target=_flush_buffer, daemon=True)
        t.start()


# ── Logging ───────────────────────────────────────────────────────────────────

def _parse_tracking_id(tracking_id: str) -> dict:
    """
    tracking_id format: {contractor_id}_{send_type}_{county}_{trade}
    e.g. 42_followup_1_palm_beach_roofing
    """
    parts = tracking_id.split("_", 3)
    return {
        "contractor_id": parts[0] if len(parts) > 0 else "",
        "send_type":     parts[1] if len(parts) > 1 else "",
        "county":        parts[2] if len(parts) > 2 else "",
        "trade":         parts[3] if len(parts) > 3 else "",
    }


def _log_event(tracking_id: str, event: str):
    parsed = _parse_tracking_id(tracking_id)
    row = {
        "timestamp":     datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "event":         event,
        "tracking_id":   tracking_id,
        "contractor_id": parsed["contractor_id"],
        "send_type":     parsed["send_type"],
        "county":        parsed["county"],
        "trade":         parsed["trade"],
        "ip":            request.headers.get("X-Forwarded-For", request.remote_addr or ""),
        "user_agent":    request.headers.get("User-Agent", ""),
    }
    with _buffer_lock:
        _buffer.append(row)
    _maybe_flush()


# ── Stats ─────────────────────────────────────────────────────────────────────

def _load_all_rows() -> list[dict]:
    """Load all rows from GitHub CSV."""
    _flush_buffer()  # make sure buffer is flushed first
    content, _ = _get_remote_csv()
    rows = []
    try:
        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)
    except Exception:
        pass
    return rows


def _build_stats(rows: list[dict]) -> dict:
    today_str = date.today().isoformat()
    by_send_type = defaultdict(lambda: {"opens": 0, "clicks": 0})
    by_county    = defaultdict(lambda: {"opens": 0, "clicks": 0})
    by_trade     = defaultdict(lambda: {"opens": 0, "clicks": 0})
    opens_today = clicks_today = total_opens = total_clicks = 0

    for row in rows:
        evt  = row.get("event", "")
        st   = row.get("send_type", "unknown") or "unknown"
        co   = row.get("county", "") or ""
        tr   = row.get("trade", "") or ""
        ts   = row.get("timestamp", "")
        is_today = ts.startswith(today_str)

        if evt == "open":
            total_opens += 1
            by_send_type[st]["opens"] += 1
            by_county[co]["opens"]    += 1
            by_trade[tr]["opens"]     += 1
            if is_today:
                opens_today += 1
        elif evt == "click":
            total_clicks += 1
            by_send_type[st]["clicks"] += 1
            by_county[co]["clicks"]    += 1
            by_trade[tr]["clicks"]     += 1
            if is_today:
                clicks_today += 1

    return {
        "today_opens":   opens_today,
        "today_clicks":  clicks_today,
        "total_opens":   total_opens,
        "total_clicks":  total_clicks,
        "total_events":  len(rows),
        "by_send_type":  dict(by_send_type),
        "by_county":     dict(by_county),
        "by_trade":      dict(by_trade),
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/pixel/<path:tracking_id>")
def pixel(tracking_id: str):
    _log_event(tracking_id, "open")
    return Response(
        PIXEL_GIF,
        mimetype="image/gif",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"},
    )


@app.route("/click/<path:tracking_id>")
def click(tracking_id: str):
    _log_event(tracking_id, "click")
    # Force flush so click is persisted immediately
    _flush_buffer()
    return redirect(STRIPE_URL)


@app.route("/stats_json")
def stats_json():
    rows  = _load_all_rows()
    stats = _build_stats(rows)
    return jsonify(stats)


@app.route("/stats")
def stats():
    rows  = _load_all_rows()
    data  = _build_stats(rows)
    today = date.today().strftime("%B %d, %Y")

    stage_rows = ""
    for stage, counts in sorted(data["by_send_type"].items()):
        total = counts["opens"] + counts["clicks"]
        if total:
            stage_rows += (
                f"<tr><td>{stage}</td>"
                f"<td style='text-align:right'>{counts['opens']}</td>"
                f"<td style='text-align:right'>{counts['clicks']}</td></tr>"
            )

    county_rows = ""
    for county, counts in sorted(data["by_county"].items(), key=lambda x: -x[1]["opens"]):
        if counts["opens"] or counts["clicks"]:
            county_rows += (
                f"<tr><td>{county.replace('_',' ').title()}</td>"
                f"<td style='text-align:right'>{counts['opens']}</td>"
                f"<td style='text-align:right'>{counts['clicks']}</td></tr>"
            )

    html = f"""<!DOCTYPE html>
<html><head><title>PermitMap Tracker</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  body{{font-family:Arial,sans-serif;max-width:720px;margin:32px auto;padding:0 20px;background:#f3f4f6;}}
  h1{{color:#1e3a5f;margin-bottom:4px;}}
  .sub{{color:#6b7280;font-size:13px;margin-bottom:24px;}}
  .cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px;}}
  .card{{background:#fff;border-radius:8px;padding:16px;border-left:4px solid #2563eb;box-shadow:0 1px 3px rgba(0,0,0,.07);}}
  .card.green{{border-color:#059669;}}
  .card.purple{{border-color:#7c3aed;}}
  .card.amber{{border-color:#d97706;}}
  .num{{font-size:28px;font-weight:700;color:#111;}}
  .lbl{{font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:.05em;margin-top:4px;}}
  table{{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;
         box-shadow:0 1px 3px rgba(0,0,0,.07);margin-bottom:20px;}}
  th{{background:#f9fafb;padding:8px 12px;text-align:left;font-size:11px;color:#6b7280;
      border-bottom:1px solid #e5e7eb;font-weight:600;text-transform:uppercase;letter-spacing:.05em;}}
  td{{padding:8px 12px;border-bottom:1px solid #f3f4f6;font-size:13px;}}
  h2{{font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;
      color:#374151;margin:20px 0 8px;}}
  a{{color:#2563eb;font-size:12px;}}
</style></head>
<body>
<h1>PermitMap Tracker</h1>
<div class="sub">Live email tracking — {today} &nbsp;·&nbsp; <a href="/stats_json">JSON API</a></div>

<div class="cards">
  <div class="card"><div class="num">{data['today_opens']}</div><div class="lbl">Opens Today</div></div>
  <div class="card green"><div class="num">{data['today_clicks']}</div><div class="lbl">Clicks Today</div></div>
  <div class="card purple"><div class="num">{data['total_opens']}</div><div class="lbl">Total Opens</div></div>
  <div class="card amber"><div class="num">{data['total_clicks']}</div><div class="lbl">Total Clicks</div></div>
</div>

<h2>By Stage</h2>
<table>
  <tr><th>Stage</th><th style="text-align:right">Opens</th><th style="text-align:right">Clicks</th></tr>
  {stage_rows or "<tr><td colspan='3' style='color:#9ca3af'>No data yet</td></tr>"}
</table>

<h2>By County</h2>
<table>
  <tr><th>County</th><th style="text-align:right">Opens</th><th style="text-align:right">Clicks</th></tr>
  {county_rows or "<tr><td colspan='3' style='color:#9ca3af'>No data yet</td></tr>"}
</table>

<p style="color:#9ca3af;font-size:11px;margin-top:24px;">
  Buffer: {len(_buffer)} unsaved events &nbsp;·&nbsp;
  Repo: {GITHUB_REPO}
</p>
</body></html>"""
    return html


@app.route("/flush")
def flush():
    """Manual flush endpoint — call to force persist buffer to GitHub."""
    _flush_buffer()
    return jsonify({"status": "flushed", "timestamp": datetime.utcnow().isoformat()})


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok", "buffer": len(_buffer), "repo": GITHUB_REPO})


@app.route("/")
def index():
    return redirect("/stats")


# ── Background flush thread ───────────────────────────────────────────────────
def _background_flusher():
    while True:
        time.sleep(60)
        _maybe_flush()

threading.Thread(target=_background_flusher, daemon=True).start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
