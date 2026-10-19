import base64, json
import pytest
import app as A


class R:
    def __init__(self, status, body=None, content=b""):
        self.status_code = status
        self._b = body
        self.content = content
        self.text = ""

    def json(self):
        return self._b


def meta(raw, with_content=True):
    return {"sha": "abc", "size": len(raw),
            "content": base64.b64encode(raw).decode() if with_content else ""}


def test_404_is_new_file(monkeypatch):
    monkeypatch.setattr(A.requests, "get", lambda *a, **k: R(404))
    c, sha = A._get_remote_csv()
    assert sha == "" and c.startswith("timestamp")


@pytest.mark.parametrize("code", [401, 403, 429, 500, 502])
def test_other_status_raises(monkeypatch, code):
    monkeypatch.setattr(A.requests, "get", lambda *a, **k: R(code))
    with pytest.raises(A.RemoteFetchError):
        A._get_remote_csv()


def test_network_error_raises(monkeypatch):
    def boom(*a, **k):
        raise TimeoutError("t")
    monkeypatch.setattr(A.requests, "get", boom)
    with pytest.raises(A.RemoteFetchError):
        A._get_remote_csv()


def test_small_file_ok(monkeypatch):
    raw = b"a,b\n1,2\n"
    monkeypatch.setattr(A.requests, "get", lambda *a, **k: R(200, meta(raw)))
    assert A._get_remote_csv() == (raw.decode(), "abc")


def test_large_file_empty_content_uses_raw(monkeypatch):
    raw = b"x" * (1048576 + 300)
    calls = []

    def get(url, headers=None, **k):
        calls.append(headers.get("Accept"))
        if "raw" in headers.get("Accept", ""):
            return R(200, content=raw)
        return R(200, meta(raw, with_content=False))
    monkeypatch.setattr(A.requests, "get", get)
    c, sha = A._get_remote_csv()
    assert len(c) == len(raw) and sha == "abc" and len(calls) == 2


def test_large_file_raw_failure_raises_not_empty(monkeypatch):
    raw = b"x" * 2000000

    def get(url, headers=None, **k):
        if "raw" in headers.get("Accept", ""):
            return R(500)
        return R(200, meta(raw, with_content=False))
    monkeypatch.setattr(A.requests, "get", get)
    with pytest.raises(A.RemoteFetchError):
        A._get_remote_csv()


def test_size_mismatch_raises(monkeypatch):
    raw = b"a" * 100

    def get(url, headers=None, **k):
        if "raw" in headers.get("Accept", ""):
            return R(200, content=b"short")
        m = meta(raw, with_content=False)
        return R(200, m)
    monkeypatch.setattr(A.requests, "get", get)
    with pytest.raises(A.RemoteFetchError):
        A._get_remote_csv()


def test_persist_never_pushes_on_fetch_failure(monkeypatch):
    pushed = []
    monkeypatch.setattr(A.requests, "get", lambda *a, **k: R(500))
    monkeypatch.setattr(A, "_push_csv", lambda *a, **k: pushed.append(1) or True)
    with A.app.test_request_context():
        with pytest.raises(A.RemoteFetchError):
            A._persist_sendgrid_events([{"event": "processed", "sg_event_id": "e", "email": "x@y.z"}])
    assert pushed == []


def test_persist_appends_and_keeps_history(monkeypatch):
    hdr = ",".join(A.SENDGRID_EVENT_HEADERS)
    old = hdr + "\n" + "1,processed,h,e0,m,,,,,,,,,,\n"
    seen = {}
    monkeypatch.setattr(A.requests, "get", lambda *a, **k: R(200, meta(old.encode())))
    monkeypatch.setattr(A, "_push_csv", lambda c, s, path=None: seen.update(c=c, s=s, path=path) or True)
    with A.app.test_request_context():
        A._persist_sendgrid_events([{"event": "processed", "sg_event_id": "e1", "email": "x@y.z", "timestamp": 2}])
    assert "e0" in seen["c"] and "e1" in seen["c"] and seen["s"] == "abc" and seen["path"] == "sendgrid_events.csv"


def test_no_global_path_swapping(monkeypatch):
    """Concurrent tracking flushes must never be redirected to the SendGrid ledger path."""
    urls = []
    monkeypatch.setattr(A.requests, "get", lambda url, **k: urls.append(url) or R(404))
    A._get_remote_csv()
    A._sendgrid_remote_csv()
    A._get_remote_csv()
    assert [u.rsplit("/", 1)[1] for u in urls] == ["tracking.csv", "sendgrid_events.csv", "tracking.csv"]
    assert A.GITHUB_PATH == "tracking.csv"
