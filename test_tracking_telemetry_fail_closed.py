"""Offline evidence-integrity tests. No real GitHub, Stripe or outbound mail."""
import pytest
import app as tracker


@pytest.mark.parametrize("status", [401, 403, 429, 500])
def test_stats_json_never_reports_fake_zero_when_github_read_fails(monkeypatch, status):
    class Failure:
        status_code = status

    monkeypatch.setattr(tracker, "_flush_buffer", lambda: None)
    monkeypatch.setattr(tracker.requests, "get", lambda *a, **k: Failure())
    response = tracker.app.test_client().get("/stats_json")
    assert response.status_code == 503
    assert response.get_json() == {"error": "tracking_data_unavailable"}
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Retry-After"] == "60"


def test_html_stats_never_shows_zero_cards_when_evidence_is_missing(monkeypatch):
    monkeypatch.setattr(tracker, "_flush_buffer", lambda: None)

    def outage():
        raise tracker.RemoteFetchError("synthetic inaccessible ledger")

    monkeypatch.setattr(tracker, "_get_remote_csv", outage)
    response = tracker.app.test_client().get("/stats")
    assert response.status_code == 503
    assert response.headers["Cache-Control"] == "no-store"
    assert b"temporarily unavailable" in response.data
    assert b"Total Opens" not in response.data


def test_rejected_github_push_requeues_events_in_original_order(monkeypatch):
    original = {"timestamp": "2026-11-17 00:00:00", "event": "click", "tracking_id": "original"}
    new = {"timestamp": "2026-11-17 00:00:01", "event": "open", "tracking_id": "new"}
    monkeypatch.setattr(tracker, "_buffer", [original])
    monkeypatch.setattr(
        tracker, "_get_remote_csv",
        lambda: (",".join(tracker.CSV_HEADERS) + "\n", "a" * 40),
    )
    calls = []

    def rejected_write(*args):
        calls.append(1)
        with tracker._buffer_lock:
            tracker._buffer.append(new)
        return False

    monkeypatch.setattr(tracker, "_push_csv", rejected_write)
    tracker._flush_buffer()
    assert calls == [1]
    assert tracker._buffer == [original, new]


def test_acknowledged_github_push_dequeues_events(monkeypatch):
    original = {"timestamp": "2026-11-17 00:00:00", "event": "click", "tracking_id": "original"}
    monkeypatch.setattr(tracker, "_buffer", [original])
    monkeypatch.setattr(
        tracker, "_get_remote_csv",
        lambda: (",".join(tracker.CSV_HEADERS) + "\n", ""),
    )
    captured = []
    monkeypatch.setattr(tracker, "_push_csv", lambda text, sha: captured.append(text) or True)
    tracker._flush_buffer()
    assert tracker._buffer == []
    assert len(captured) == 1
    assert "original" in captured[0]
