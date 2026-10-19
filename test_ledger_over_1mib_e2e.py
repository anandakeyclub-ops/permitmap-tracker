"""End-to-end: webhook -> ledger persistence against a fake GitHub contents API that
behaves like the real one for files over 1 MiB (empty body in the JSON response)."""
import base64, csv, io, json
import app as A

HDR = ",".join(A.SENDGRID_EVENT_HEADERS)


class FakeGitHub:
    def __init__(self):
        self.files = {}
        self.n = 0
        self.puts = []

    def _path(self, url):
        return url.split("/contents/")[1]

    def get(self, url, headers=None, params=None, timeout=None):
        p = self._path(url)
        if p not in self.files:
            return Resp(404)
        raw, sha = self.files[p]
        if "raw" in (headers or {}).get("Accept", ""):
            return Resp(200, content=raw)
        body = {"sha": sha, "size": len(raw),
                "content": "" if len(raw) > 1048576 else base64.b64encode(raw).decode()}
        return Resp(200, body)

    def put(self, url, headers=None, json=None, timeout=None):
        p = self._path(url)
        cur = self.files.get(p)
        if cur and json.get("sha") != cur[1]:
            return Resp(422)
        if not cur and json.get("sha"):
            return Resp(422)
        self.n += 1
        self.files[p] = (base64.b64decode(json["content"]), f"sha{self.n}")
        self.puts.append(p)
        return Resp(200)


class Resp:
    def __init__(self, code, body=None, content=b""):
        self.status_code, self._b, self.content, self.text = code, body, content, ""

    def json(self):
        return self._b


def big_ledger(n_bytes):
    out = [HDR]
    i = 0
    while sum(len(x) + 1 for x in out) < n_bytes:
        out.append(f"{1792000000+i},processed,h{i},id{i},m,{'x'*200},,,,t{i},first_outreach,c,t,,")
        i += 1
    return ("\n".join(out) + "\n").encode(), i


def post(client, gh, events, monkeypatch):
    monkeypatch.setattr(A.requests, "get", gh.get)
    monkeypatch.setattr(A.requests, "put", gh.put)
    monkeypatch.setattr(A, "_verify_sendgrid_signature", lambda payload: True)
    return client.post("/api/webhooks/sendgrid/events", data=json.dumps(events),
                       content_type="application/json")


def test_ledger_over_1mib_is_preserved_on_webhook(monkeypatch):
    gh = FakeGitHub()
    raw, n = big_ledger(1048576 + 5000)
    assert len(raw) > 1048576
    gh.files["sendgrid_events.csv"] = (raw, "sha0")
    r = post(A.app.test_client(), gh, [{"event": "processed", "email": "a@b.c", "sg_event_id": "new1", "timestamp": 1792999999}], monkeypatch)
    assert r.status_code == 200, r.get_data(as_text=True)
    after = list(csv.DictReader(io.StringIO(gh.files["sendgrid_events.csv"][0].decode())))
    assert len(after) == n + 1
    assert {x["sg_event_id"] for x in after} >= {"id0", f"id{n-1}", "new1"}


def test_failed_read_aborts_without_write(monkeypatch):
    gh = FakeGitHub()
    raw, n = big_ledger(1048576 + 5000)
    gh.files["sendgrid_events.csv"] = (raw, "sha0")
    orig = gh.get
    gh.get = lambda url, headers=None, **k: Resp(502) if "raw" in (headers or {}).get("Accept", "") else orig(url, headers=headers, **k)
    r = post(A.app.test_client(), gh, [{"event": "processed", "email": "a@b.c", "sg_event_id": "new2", "timestamp": 1}], monkeypatch)
    assert r.status_code >= 500
    assert gh.puts == [] and gh.files["sendgrid_events.csv"][0] == raw


def test_tracking_flush_never_lands_in_ledger(monkeypatch):
    gh = FakeGitHub()
    gh.files["sendgrid_events.csv"] = ((HDR + "\n").encode(), "sha0")
    monkeypatch.setattr(A.requests, "get", gh.get)
    monkeypatch.setattr(A.requests, "put", gh.put)
    monkeypatch.setattr(A, "GITHUB_TOKEN", "t")
    with A.app.test_request_context():
        A._buffer.clear()
        A._buffer.append({"timestamp": "2026-10-19 00:00:00", "event": "open", "tracking_id": "1_x_c_t"})
        A._flush_buffer()
    assert "tracking.csv" in gh.files
    assert gh.files["sendgrid_events.csv"][0].decode().strip() == HDR
