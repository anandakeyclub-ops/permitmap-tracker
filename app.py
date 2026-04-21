"""
permitmap.org — Email Tracking Server (v2)
Persists tracking.csv to GitHub so data survives Render restarts.
"""
import csv
import os
import base64
import json
import urllib.request
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from flask import Flask, redirect, request, Response

app = Flask(__name__)

STRIPE_BASE_URL = os.environ.get("STRIPE_BASE_URL", "https://buy.stripe.com/14AeVddOnbPx1g23VIdUY04")
LOG_FILE        = Path(__file__).parent / "tracking.csv"
LOG_FIELDS      = ["timestamp", "event", "tracking_id", "contractor_id",
                   "send_type", "county", "trade", "ip", "user_agent"]

GITHUB_TOKEN    = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO     = os.environ.get("GITHUB_REPO", "anandakeyclub-ops/permitmap-tracker")
GITHUB_PATH     = "tracking.csv"
GITHUB_BRANCH   = os.environ.get("GITHUB_BRANCH", "main")

PIXEL_GIF = base64.b64decode(
    "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
)

_github_sha: str | None = None  # cached SHA for updates


def parse_tracking_id(tracking_id):
    parts = tracking_id.split("_", 3)
    return {
        "contractor_id": parts[0] if len(parts) > 0 else "",
        "send_type":     parts[1] if len(parts) > 1 else "",
        "county":        parts[2] if len(parts) > 2 else "",
        "trade":         parts[3] if len(parts) > 3 else "",
    }


def _github_headers() -> dict:
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept":        "application/vnd.github+json",
        "Content-Type":  "application/json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_url() -> str:
    return f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_PATH}"


def pull_from_github() -> None:
    """Pull tracking.csv from GitHub on startup so we have full history."""
    global _github_sha
    if not GITHUB_TOKEN:
        return
    try:
        req = urllib.request.Request(
            _github_url() + f"?ref={GITHUB_BRANCH}",
            headers=_github_headers()
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        _github_sha = data.get("sha", "")
        content = base64.b64decode(data["content"].replace("\n", "")).decode("utf-8")
        LOG_FILE.write_text(content, encoding="utf-8")
        lines = content.count("\n")
        print(f"[tracker] Pulled tracking.csv from GitHub ({lines} lines, sha={_github_sha[:8]})")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print("[tracker] No tracking.csv on GitHub yet — will create on first event")
        else:
            print(f"[tracker] GitHub pull error: {e}")
    except Exception as e:
        print(f"[tracker] GitHub pull error: {e}")


def push_to_github() -> None:
    """Push local tracking.csv to GitHub after each event."""
    global _github_sha
    if not GITHUB_TOKEN:
        return
    if not LOG_FILE.exists():
        return
    try:
        content_b64 = base64.b64encode(
            LOG_FILE.read_bytes()
        ).decode("ascii")

        payload = {
            "message": f"tracking update {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}",
            "content": content_b64,
            "branch":  GITHUB_BRANCH,
        }
        if _github_sha:
            payload["sha"] = _github_sha

        data = json.dumps(payload).encode()
        req  = urllib.request.Request(
            _github_url(),
            data=data,
            headers=_github_headers(),
            method="PUT",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
        _github_sha = result.get("content", {}).get("sha", _github_sha)
    except Exception as e:
        print(f"[tracker] GitHub push error: {e}")


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
    push_to_github()


@app.route("/health")
def health():
    lines = len(LOG_FILE.read_text().splitlines()) - 1 if LOG_FILE.exists() else 0
    return {"status": "ok", "events": lines, "github_sync": bool(GITHUB_TOKEN)}


@app.route("/pixel/<tracking_id>")
def pixel(tracking_id):
    log_event("open", tracking_id)
    return Response(
        PIXEL_GIF,
        mimetype="image/gif",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma":        "no-cache",
            "Expires":       "0",
        }
    )


@app.route("/click/<tracking_id>")
def click(tracking_id):
    log_event("click", tracking_id)
    parsed  = parse_tracking_id(tracking_id)
    county  = parsed.get("county", "")
    trade   = parsed.get("trade", "")
    tag     = county + "_" + trade if county and trade else tracking_id
    url     = STRIPE_BASE_URL + "?client_reference_id=" + tag
    return redirect(url, code=302)


@app.route("/stats")
def stats():
    if not LOG_FILE.exists():
        return "<h2>No tracking data yet.</h2>"

    opens  = defaultdict(int)
    clicks = defaultdict(int)
    today  = datetime.utcnow().strftime("%Y-%m-%d")

    with open(LOG_FILE, newline="", encoding="utf-8", errors="ignore") as f:
        for row in csv.DictReader(f):
            key = row.get("send_type", "unknown")
            ts  = row.get("timestamp", "")
            if row.get("event") == "open":
                opens[key]  += 1
            elif row.get("event") == "click":
                clicks[key] += 1

    total_opens  = sum(opens.values())
    total_clicks = sum(clicks.values())

    rows_html = "".join(
        f"<tr><td>{k}</td><td>{opens.get(k,0)}</td><td>{clicks.get(k,0)}</td></tr>"
        for k in sorted(set(list(opens.keys()) + list(clicks.keys())))
    )

    return f"""
    <html><body style="font-family:monospace;padding:20px;background:#0f172a;color:#e2e8f0">
    <h2 style="color:#3b82f6">PermitMap Tracking Stats</h2>
    <p>Total opens: <b>{total_opens}</b> &nbsp; Total clicks: <b>{total_clicks}</b></p>
    <p>GitHub sync: <b>{'enabled' if GITHUB_TOKEN else 'DISABLED — data will be lost on restart'}</b></p>
    <table border=1 cellpadding=6 style="border-collapse:collapse;color:#e2e8f0">
      <tr><th>Send Type</th><th>Opens</th><th>Clicks</th></tr>
      {rows_html}
    </table>
    </body></html>
    """


# Pull from GitHub on startup so history survives restarts
pull_from_github()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
