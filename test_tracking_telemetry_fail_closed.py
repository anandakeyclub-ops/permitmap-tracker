"""Offline evidence-integrity tests. No real GitHub, Stripe or outbound mail."""
import threading

import pytest
import app as tracker


@pytest.mark.parametrize("status", [401, 403, 429, 500])
def test_stats_json_never_reports_fake_zero_when_github_read_fails(monkeypatch, status):
    class Failure:
        status_code = status

    monkeypatch.setattr(tracker, "_flush_buffer", lambda: True)
    monkeypatch.setattr(tracker.requests, "get", lambda *a, **k: Failure())
    response = tracker.app.test_client().get("/stats_json")
    assert response.status_code == 503
    assert response.get_json() == {"error": "tracking_data_unavailable"}
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Retry-After"] == "60"


def test_html_stats_never_shows_zero_cards_when_evidence_is_missing(monkeypatch):
    monkeypatch.setattr(tracker, "_flush_buffer", lambda: True)

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
    assert tracker._flush_buffer() is False
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
    assert tracker._flush_buffer() is True
    assert tracker._buffer == []
    assert len(captured) == 1
    assert "original" in captured[0]


def _synthetic_row(identity):
    return {"timestamp": "2026-11-17 00:00:00", "event": "click", "tracking_id": identity}


@pytest.mark.parametrize("path", ["/stats", "/stats_json", "/flush"])
def test_pending_unacknowledged_events_do_not_return_false_success(monkeypatch, path):
    monkeypatch.setattr(tracker, "_buffer", [_synthetic_row("pending")])
    monkeypatch.setattr(
        tracker, "_get_remote_csv",
        lambda: (",".join(tracker.CSV_HEADERS) + "\n", "synthetic"),
    )
    monkeypatch.setattr(tracker, "_push_csv", lambda *args, **kwargs: False)
    response = tracker.app.test_client().get(path)
    assert response.status_code == 503
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Retry-After"] == "60"
    assert tracker._buffer == [_synthetic_row("pending")]


def test_overlapping_flushes_are_serialized_and_preserve_both_events(monkeypatch):
    remote = [",".join(tracker.CSV_HEADERS) + "\n"]
    first_push_started = threading.Event()
    release_first_push = threading.Event()
    writes = []
    results = []
    monkeypatch.setattr(tracker, "_buffer", [_synthetic_row("first")])

    def get_remote():
        return remote[0], "synthetic"

    def push_remote(content, sha):
        writes.append(content)
        if len(writes) == 1:
            first_push_started.set()
            if not release_first_push.wait(timeout=3):
                raise AssertionError("synthetic first push never released")
        remote[0] = content
        return True

    monkeypatch.setattr(tracker, "_get_remote_csv", get_remote)
    monkeypatch.setattr(tracker, "_push_csv", push_remote)
    t1 = threading.Thread(target=lambda: results.append(tracker._flush_buffer()))
    t2 = threading.Thread(target=lambda: results.append(tracker._flush_buffer()))
    try:
        t1.start()
        assert first_push_started.wait(timeout=3), "first flush did not reach push"
        with tracker._buffer_lock:
            tracker._buffer.append(_synthetic_row("second"))
        t2.start()
    finally:
        release_first_push.set()
        t1.join(timeout=3)
        if t2.ident is not None:
            t2.join(timeout=3)

    assert not t1.is_alive() and not t2.is_alive()
    assert results == [True, True]
    assert len(writes) == 2
    assert "first" in remote[0] and "second" in remote[0]
    assert tracker._buffer == []
