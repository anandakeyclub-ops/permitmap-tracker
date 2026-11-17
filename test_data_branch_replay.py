"""Hermetic data-branch cutover and SendGrid replay acceptance.

Tests never reach GitHub, Render, Gmail, Stripe, or customer systems.
The branch option must remain unreleased until a verified snapshot exists.
"""
import base64
import csv
import io

import pytest

import app as tracker


class FakeResponse:
    def __init__(self, status=200, body=None, content=b""):
        self.status_code = status
        self._body = body
        self.content = content
        self.text = ""

    def json(self):
        return self._body


def _set_isolated(monkeypatch, data="ledger-snapshot"):
    monkeypatch.setattr(tracker, "GITHUB_DATA_BRANCH", data)
    monkeypatch.setattr(tracker, "GITHUB_CODE_BRANCH", "main")
    monkeypatch.setattr(tracker, "REQUIRE_SEPARATE_DATA_BRANCH", True)


def test_ledger_get_reads_only_dedicated_branch(monkeypatch):
    _set_isolated(monkeypatch)
    raw = (",".join(tracker.CSV_HEADERS) + "\n").encode()
    calls = []

    def get(url, headers=None, params=None, timeout=None):
        calls.append((url, params))
        return FakeResponse(body={
            "sha": "synthetic-ledger-sha", "size": len(raw),
            "content": base64.b64encode(raw).decode(),
        })

    monkeypatch.setattr(tracker.requests, "get", get)
    content, sha = tracker._get_remote_csv()
    assert (content, sha) == (raw.decode(), "synthetic-ledger-sha")
    assert len(calls) == 1
    assert calls[0][1] == {"ref": "ledger-snapshot"}
    assert calls[0][0].endswith("/contents/tracking.csv")


def test_ledger_put_writes_only_dedicated_branch_with_cas(monkeypatch):
    _set_isolated(monkeypatch)
    monkeypatch.setattr(tracker, "GITHUB_TOKEN", "synthetic-token")
    seen = []

    def put(url, headers=None, json=None, timeout=None):
        seen.append((url, json))
        return FakeResponse()

    monkeypatch.setattr(tracker.requests, "put", put)
    assert tracker._push_csv("header\nrow\n", "synthetic-current-sha")
    assert len(seen) == 1
    assert seen[0][1]["branch"] == "ledger-snapshot"
    assert seen[0][1]["sha"] == "synthetic-current-sha"
    assert base64.b64decode(seen[0][1]["content"]) == b"header\nrow\n"


@pytest.mark.parametrize("data_branch", ["main", "", "  "])
def test_strict_branch_guard_fails_before_any_network(monkeypatch, data_branch):
    _set_isolated(monkeypatch, data=data_branch)
    monkeypatch.setattr(tracker, "GITHUB_TOKEN", "synthetic-token")
    calls = []

    def get(*args, **kwargs):
        calls.append("get")
        raise AssertionError("unexpected network")

    def put(*args, **kwargs):
        calls.append("put")
        raise AssertionError("unexpected network")

    monkeypatch.setattr(tracker.requests, "get", get)
    monkeypatch.setattr(tracker.requests, "put", put)
    with pytest.raises(tracker.RemoteFetchError):
        tracker._get_remote_csv()
    with pytest.raises(tracker.RemoteFetchError):
        tracker._push_csv("new\n", "old-sha")
    assert calls == []


def test_new_branch_without_snapshot_is_not_accepted_as_valid_tracking(monkeypatch):
    _set_isolated(monkeypatch)
    monkeypatch.setattr(tracker.requests, "get", lambda *a, **kw: FakeResponse(404))
    content, sha = tracker._get_remote_csv()
    with pytest.raises(tracker.RemoteFetchError, match="missing"):
        tracker._require_tracking_ledger_header(content, sha)


def _event_csv():
    header = ",".join(tracker.SENDGRID_EVENT_HEADERS) + "\r\n"
    # Actual values in the required 15-column order, with a known event ID.
    known = ["1792000000", "delivered", "synthetic-hash", "known-1", "msg-1"]
    known += [""] * (len(tracker.SENDGRID_EVENT_HEADERS) - len(known))
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\r\n")
    writer.writerow(tracker.SENDGRID_EVENT_HEADERS)
    writer.writerow(known)
    return output.getvalue()


def test_duplicate_only_sendgrid_replay_never_rewrites_ledger(monkeypatch):
    remote = _event_csv()
    monkeypatch.setattr(
        tracker, "_sendgrid_remote_csv",
        lambda: (remote, "synthetic-sg-sha"),
    )
    writes = []
    monkeypatch.setattr(tracker, "_sendgrid_push_csv", lambda *a: writes.append(a))
    with tracker.app.test_request_context("/api/webhooks/sendgrid/events"):
        tracker._persist_sendgrid_events([{
            "event": "delivered", "email": "sample@example.test",
            "sg_event_id": "known-1",
        }])
    assert writes == []  # no Render redeploy from duplicate webhook


def test_mixed_sendgrid_replay_appends_only_new_event(monkeypatch):
    remote = _event_csv()
    monkeypatch.setattr(tracker, "_sendgrid_remote_csv", lambda: (remote, "sg-old"))
    writes = []

    def push(content, sha):
        writes.append((content, sha))
        return True

    monkeypatch.setattr(tracker, "_sendgrid_push_csv", push)
    with tracker.app.test_request_context("/api/webhooks/sendgrid/events"):
        tracker._persist_sendgrid_events([
            {"event": "delivered", "sg_event_id": "known-1"},
            {"event": "processed", "sg_event_id": "fresh-2", "email": "sample@example.test"},
        ])
    assert len(writes) == 1
    assert writes[0][1] == "sg-old"
    rows = list(csv.DictReader(io.StringIO(writes[0][0])))
    assert [row["sg_event_id"] for row in rows] == ["known-1", "fresh-2"]


def test_missing_sendgrid_id_is_not_falsely_deduplicated(monkeypatch):
    remote = _event_csv()
    monkeypatch.setattr(tracker, "_sendgrid_remote_csv", lambda: (remote, "sg-old"))
    writes = []
    monkeypatch.setattr(
        tracker, "_sendgrid_push_csv",
        lambda content, sha: writes.append(content) or True,
    )
    with tracker.app.test_request_context("/api/webhooks/sendgrid/events"):
        tracker._persist_sendgrid_events([{"event": "processed", "email": "sample@example.test"}])
    assert len(writes) == 1
    assert len(list(csv.DictReader(io.StringIO(writes[0])))) == 2


def test_malformed_sendgrid_ledger_refuses_even_duplicate_replay(monkeypatch):
    bad = ",".join(tracker.SENDGRID_EVENT_HEADERS) + "\n" + "incomplete\n"
    monkeypatch.setattr(tracker, "_sendgrid_remote_csv", lambda: (bad, "sg-old"))
    writes = []
    monkeypatch.setattr(tracker, "_sendgrid_push_csv", lambda *a: writes.append(a))
    with tracker.app.test_request_context("/api/webhooks/sendgrid/events"):
        with pytest.raises(tracker.RemoteFetchError):
            tracker._persist_sendgrid_events([{"event": "delivered", "sg_event_id": "known-1"}])
    assert writes == []
