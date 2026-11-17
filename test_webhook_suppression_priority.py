"""Hermetic SendGrid webhook ordering and replay acceptance tests.

No network, provider writes, customer mail, or real billing operations.
"""

import csv
import io

import pytest

import app as tracker
from sendgrid_suppression import (
    SUPPRESSION_HEADERS,
    SUPPRESSION_PATH,
    build_suppression_rows,
    email_hash,
)


def _existing():
    return ",".join(SUPPRESSION_HEADERS) + "\n"


def _post(monkeypatch, event, *, push_ok=True, persist_ok=True):
    calls = []
    current = _existing()
    monkeypatch.setattr(tracker, "_verify_sendgrid_signature", lambda payload: True)
    monkeypatch.setattr(
        tracker, "_get_remote_csv",
        lambda path=None: (current, "synthetic-existing-blob"),
    )

    def push(content, sha, path=None):
        calls.append(("suppression", path, content))
        return push_ok

    def persist(events):
        calls.append(("events", None, events))
        if not persist_ok:
            raise RuntimeError("synthetic event ledger failure")

    monkeypatch.setattr(tracker, "_push_csv", push)
    monkeypatch.setattr(tracker, "_persist_sendgrid_events", persist)
    result = tracker.app.test_client().post(
        "/api/webhooks/sendgrid/events", json=[event]
    )
    return result, calls


def test_suppression_is_durable_before_analytics_acceptance(monkeypatch):
    response, calls = _post(monkeypatch, {
        "event": "unsubscribe", "email": "person@example.test",
        "sg_event_id": "synthetic-unsubscribe-1",
    })
    assert response.status_code == 200
    assert [entry[0] for entry in calls] == ["suppression", "events"]
    assert calls[0][1] == SUPPRESSION_PATH
    rows = list(csv.DictReader(io.StringIO(calls[0][2])))
    assert len(rows) == 1
    assert rows[0]["email_hash"] == email_hash("person@example.test")
    assert rows[0]["sg_event_id"] == "synthetic-unsubscribe-1"


@pytest.mark.parametrize("event", [
    {"event": "unsubscribe", "email": "person@example.test", "sg_event_id": "u-2"},
    {"event": "spamreport", "email": "person@example.test", "sg_event_id": "s-2"},
    {"event": "bounce", "email": "person@example.test", "status": "5.1.1", "sg_event_id": "b-2"},
])
def test_failed_suppression_write_blocks_event_ledger_and_ack(monkeypatch, event):
    response, calls = _post(monkeypatch, event, push_ok=False)
    assert response.status_code == 500
    assert [entry[0] for entry in calls] == ["suppression"]


def test_event_ledger_failure_cannot_undo_first_committed_opt_out(monkeypatch):
    response, calls = _post(monkeypatch, {
        "event": "unsubscribe", "email": "person@example.test",
        "sg_event_id": "u-3",
    }, persist_ok=False)
    assert response.status_code == 500  # Provider retry, not a false 200.
    assert [entry[0] for entry in calls] == ["suppression", "events"]


@pytest.mark.parametrize("event", [
    {"event": "delivered", "email": "person@example.test", "sg_event_id": "d-1"},
    {"event": "deferred", "email": "person@example.test", "sg_event_id": "d-2"},
    {"event": "bounce", "email": "person@example.test", "status": "4.2.0", "type": "soft", "sg_event_id": "b-1"},
    {"event": "click", "email": "person@example.test", "sg_event_id": "c-1"},
])
def test_non_suppression_events_do_not_rewrite_opt_out_ledger(monkeypatch, event):
    response, calls = _post(monkeypatch, event)
    assert response.status_code == 200
    assert [entry[0] for entry in calls] == ["events"]


def test_identical_retried_suppression_preserves_exact_existing_bytes():
    event = {
        "event": "unsubscribe", "email": "person@example.test",
        "sg_event_id": "synthetic-repeat-1",
    }
    original = build_suppression_rows([event], _existing())
    assert original != _existing()
    assert build_suppression_rows([event], original) == original


def test_no_op_preserves_existing_crlf_and_row_order_exactly():
    existing = ",".join(SUPPRESSION_HEADERS) + "\r\n"
    existing += "z,reason,unsubscribe,z-id,2026-11-16T00:00:00Z\r\n"
    unchanged = build_suppression_rows([{
        "event": "delivered", "email": "person@example.test",
    }], existing)
    assert unchanged == existing


def test_new_opt_out_persists_even_when_non_suppression_in_same_batch():
    original = _existing()
    updated = build_suppression_rows([
        {"event": "delivered", "email": "person@example.test"},
        {"event": "unsubscribe", "email": "person@example.test", "sg_event_id": "u-4"},
    ], original)
    rows = list(csv.DictReader(io.StringIO(updated)))
    assert len(rows) == 1 and rows[0]["event"] == "unsubscribe"
