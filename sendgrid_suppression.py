from __future__ import annotations
import csv
import hashlib
import io
import os
from datetime import datetime, timezone

SUPPRESSION_PATH = "sendgrid_suppressions.csv"
SUPPRESSION_HEADERS = ["email_hash", "reason", "event", "sg_event_id", "updated_at"]
HARD_EVENTS = {"bounce", "dropped", "spamreport", "unsubscribe", "group_unsubscribe"}


def email_hash(email: str) -> str:
    return hashlib.sha256(str(email or "").strip().lower().encode("utf-8")).hexdigest()


def build_suppression_rows(events: list[dict], existing_csv: str = "") -> str:
    existing = list(csv.DictReader(io.StringIO(existing_csv))) if existing_csv.strip() else []
    by_hash = {r.get("email_hash", ""): r for r in existing if r.get("email_hash")}
    for e in events:
        event = str(e.get("event") or "")
        email = str(e.get("email") or "").strip().lower()
        if event not in HARD_EVENTS or not email:
            continue
        h = email_hash(email)
        reason = str(e.get("reason") or event)
        by_hash[h] = {
            "email_hash": h,
            "reason": reason,
            "event": event,
            "sg_event_id": str(e.get("sg_event_id") or ""),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    out = io.StringIO()
    w = csv.DictWriter(out, fieldnames=SUPPRESSION_HEADERS)
    w.writeheader()
    for row in sorted(by_hash.values(), key=lambda x: x["email_hash"]):
        w.writerow(row)
    return out.getvalue()
