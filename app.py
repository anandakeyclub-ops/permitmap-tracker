"""
permitmap-tracker app.py
Flask app for email open/click tracking.
Deploy to Render at permitmap-tracker.onrender.com

Endpoints:
  GET  /pixel/<tracking_id>     — 1x1 GIF, logs open
  GET  /click/<tracking_id>     — redirect to Stripe, logs click
  GET  /stats                   — HTML dashboard
  GET  /stats/json              — JSON stats for daily summary
"""
from __future__ import annotations

import csv
import io
import json
import os
from datetime import datetime, date
from pathlib import Path

from flask import Flask, Response, redirect, request, jsonify

app = Flask(__name__)

STRIPE_URL  = os.environ.get("STRIPE_URL", "https://buy.stripe.com/cNi3cvfWv1aT6Am63QdUY00")
LOG_FILE    = Path("tracking_log.csv")
LOG_HEADERS = ["timestamp", "tracking_id", "event", "send_type", "county", "trade", "ip"]

PIXEL_GIF = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00"
    b"!\xf9\x04\x00\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01"
    b"\x00\x00\x02\x02D\x01\x00;"
)


def _ensure_log():
    if not LOG_FILE.exists():
        with open(LOG_FILE, "w", newline="") as f:
            csv.writer(f).writerow(LOG_HEADERS)


def _log(tracking_id: str, event: str):
    _ensure_log()
    parts = tracking_id.split("_", 3)
    send_type = parts[1] if len(parts) > 1 else ""
    county    = parts[2] if len(parts) > 2 else ""
    trade     = parts[3] if len(parts) > 3 else ""
    with open(LOG_FILE, "a", newline="") as f:
        csv.writer(f).writerow([
            datetime.utcnow().isoformat(),
            tracking_id, event, send_type, county, trade,
            request.remote_addr,
        ])


def _load_stats() -> dict:
    """Load all tracking events and return structured stats."""
    _ensure_log()
    rows = []
    try:
        with open(LOG_FILE) as f:
            rows = list(csv.DictReader(f))
    except Exception:
        pass

    today_str = date.today().isoformat()

    # All-time by send_type
    by_stage: dict[str, dict] = {}
    opens_today = clicks_today = 0

    for row in rows:
        st  = row.get("send_type", "unknown") or "unknown"
        evt = row.get("event", "")
        ts  = row.get("timestamp", "")

        if st not in by_stage:
            by_stage[st] = {"opens": 0, "clicks": 0}

        if evt == "open":
            by_stage[st]["opens"] += 1
            if ts.startswith(today_str):
                opens_today += 1
        elif evt == "click":
            by_stage[st]["clicks"] += 1
            if ts.startswith(today_str):
                clicks_today += 1

    return {
        "opens_today":  opens_today,
        "clicks_today": clicks_today,
        "total_events": len(rows),
        "by_stage":     by_stage,
    }


@app.route("/pixel/<tracking_id>")
def pixel(tracking_id: str):
    _log(tracking_id, "open")
    return Response(PIXEL_GIF, mimetype="image/gif",
                    headers={"Cache-Control": "no-cache, no-store"})


@app.route("/click/<tracking_id>")
def click(tracking_id: str):
    _log(tracking_id, "click")
    return redirect(STRIPE_URL)


@app.route("/stats/json")
def stats_json():
    """JSON endpoint for daily summary script."""
    return jsonify(_load_stats())


@app.route("/stats")
def stats():
    """HTML dashboard."""
    data = _load_stats()
    rows_html = ""
    for stage, counts in sorted(data["by_stage"].items()):
        rows_html += (
            f"<tr><td>{stage}</td>"
            f"<td>{counts['opens']}</td>"
            f"<td>{counts['clicks']}</td></tr>"
        )

    html = f"""<!DOCTYPE html>
<html><head><title>PermitMap Tracker</title>
<style>
  body {{ font-family: Arial, sans-serif; max-width: 700px; margin: 40px auto; padding: 0 20px; }}
  h1 {{ color: #1e3a5f; }}
  .cards {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; margin: 20px 0; }}
  .card {{ background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 16px; text-align: center; }}
  .card .num {{ font-size: 32px; font-weight: 700; color: #111; }}
  .card .lbl {{ font-size: 12px; color: #6b7280; margin-top: 4px; }}
  table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
  th {{ background: #f3f4f6; padding: 8px 12px; text-align: left; font-size: 12px; color: #6b7280; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #f3f4f6; font-size: 14px; }}
</style></head>
<body>
<h1>PermitMap Tracker</h1>
<div class="cards">
  <div class="card"><div class="num">{data['opens_today']}</div><div class="lbl">Opens Today</div></div>
  <div class="card"><div class="num">{data['clicks_today']}</div><div class="lbl">Clicks Today</div></div>
  <div class="card"><div class="num">{data['total_events']}</div><div class="lbl">Total Events</div></div>
</div>
<table>
  <tr><th>Stage</th><th>Opens</th><th>Clicks</th></tr>
  {rows_html}
</table>
<p style="color:#9ca3af;font-size:12px;margin-top:20px;">
  <a href="/stats/json">JSON API</a>
</p>
</body></html>"""
    return html


@app.route("/")
def index():
    return redirect("/stats")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
