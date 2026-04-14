"""
permitmap.org -- Email Tracking Server
"""

import csv
import os
import base64
from datetime import datetime
from pathlib import Path
from flask import Flask, redirect, request, Response

app = Flask(__name__)

STRIPE_BASE_URL = os.environ.get("STRIPE_BASE_URL", "https://buy.stripe.com/cNi3cvfWv1aT6Am63QdUY00")
LOG_FILE = Path(__file__).parent / "tracking.csv"
LOG_FIELDS = ["timestamp", "event", "tracking_id", "contractor_id",
              "send_type", "county", "trade", "ip", "user_agent"]

PIXEL_GIF = base64.b64decode(
    "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
)

def parse_tracking_id(tracking_id):
    parts = tracking_id.split("_", 3)
    return {
        "contractor_id": parts[0] if len(parts) > 0 else "",
        "send_type":     parts[1] if len(parts) > 1 else "",
        "county":        parts[2] if len(parts) > 2 else "",
        "trade":         parts[3] if len(parts) > 3 else "",
    }

def log_event(event, tracking_id):
    parsed = parse_tracking_id(tracking_id)
    row = {
        "timestamp":     datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "event":         event,
        "tracking_id":   tracking_id,
        "contractor_id": parsed["contractor_id"],
        "send_type":     parsed["send_type"],
        "county":        parsed["county"],
        "trade":         parsed["trade"],
        "ip":            request.remote_addr or "",
        "user_agent":    request.headers.get("User-Agent", "")[:120],
    }
    file_exists = LOG_FILE.exists()
    with open(LOG_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=LOG_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

@app.route("/pixel/<tracking_id>")
def pixel(tracking_id):
    log_event("open", tracking_id)
    return Response(
        PIXEL_GIF,
        mimetype="image/gif",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        }
    )

@app.route("/click/<tracking_id>")
def click(tracking_id):
    log_event("click", tracking_id)
    parsed = parse_tracking_id(tracking_id)
    county = parsed.get("county", "")
    trade = parsed.get("trade", "")
    tag = f"{county}_{trade}" if county and trade else tracking_id
    url = f"{STRIPE_BASE_URL}?client_reference_id={tag}"
    return redirect(url, code=302)

@app.route("/stats")
def stats():
    if not LOG_FILE.exists():
        return "<h2>No tracking data yet.</h2>"
    from collections import defaultdict
    opens = defaultdict(int)
    clicks = defaultdict(int)
    total = 0
    with open(LOG_FILE) as f:
        for