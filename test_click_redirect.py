"""Regression tests for destination-aware /click redirect + open-redirect protection.

Network I/O (GitHub CSV flush) is patched out so these are pure, offline unit tests
of the redirect/allowlist logic.
"""
from urllib.parse import quote

import pytest

import app as tracker


@pytest.fixture()
def client(monkeypatch):
    # Never touch GitHub during tests.
    monkeypatch.setattr(tracker, "_log_event", lambda *a, **k: None)
    monkeypatch.setattr(tracker, "_flush_buffer", lambda *a, **k: None)
    tracker.app.config.update(TESTING=True)
    return tracker.app.test_client()


def _loc(client, tracking_id, to=None):
    path = f"/click/{tracking_id}"
    if to is not None:
        path += f"?to={quote(to, safe='')}"
    resp = client.get(path)
    assert resp.status_code == 302
    return resp.headers["Location"]


# ── A. PermitMap content destination → 302 to the exact URL, no attribution added ──
def test_permitmap_destination_exact():
    assert tracker._validate_destination("https://permitmap.org/?county=palm_beach") \
        == "https://permitmap.org/?county=palm_beach"


def test_permitmap_destination_redirect(client):
    dest = "https://permitmap.org/?county=palm_beach"
    assert _loc(client, "42_first_outreach_palm_beach_roofing", dest) == dest


# ── B. app.permitmap.org signup → 302 to exact signup URL + query ──────────────
def test_app_signup_destination_redirect(client):
    dest = ("https://app.permitmap.org/sign-up?plan=starter&source=outreach"
            "&utm_source=email&utm_medium=outreach&utm_campaign=close_email")
    assert _loc(client, "42_close_email_palm_beach_roofing", dest) == dest


# ── C. Stripe destination → preserves client_reference_id attribution ──────────
def test_stripe_destination_preserves_client_reference_id(client):
    tid = "42_close_email_palm_beach_roofing"
    loc = _loc(client, tid, "https://buy.stripe.com/abc123")
    assert loc == f"https://buy.stripe.com/abc123?client_reference_id={tid}"


def test_stripe_destination_with_existing_query_uses_ampersand(client):
    tid = "7_first_outreach_lee_hvac"
    loc = _loc(client, tid, "https://checkout.stripe.com/pay?x=1")
    assert loc == f"https://checkout.stripe.com/pay?x=1&client_reference_id={tid}"


# ── D. Existing query params preserved verbatim (no dup/broken query) ──────────
def test_existing_query_params_preserved(client):
    dest = "https://permitmap.org/?county=lee&utm_source=email&utm_campaign=fo"
    assert _loc(client, "1_first_outreach_lee_roofing", dest) == dest


# ── E. Invalid external destination → safe fallback, never open redirect ───────
def test_external_domain_blocked(client):
    assert _loc(client, "x", "https://evil.com/steal") == tracker.SAFE_DEFAULT
    assert tracker._validate_destination("https://evil.com/steal") is None


# ── F. javascript:/data:/protocol-relative blocked ─────────────────────────────
@pytest.mark.parametrize("bad", [
    "javascript:alert(1)",
    "data:text/html,<script>alert(1)</script>",
    "//evil.com/x",
    "http://permitmap.org/x",          # non-https rejected
    "ht!tp://permitmap.org",           # malformed
])
def test_dangerous_schemes_blocked(client, bad):
    assert tracker._validate_destination(bad) is None
    assert _loc(client, "x", bad) == tracker.SAFE_DEFAULT


def test_empty_string_destination_rejected_by_validator():
    # Empty is invalid to the validator; the route treats an empty ?to= as "no to"
    # (legacy Stripe default), which is covered by test_legacy_no_to_redirects_to_stripe_default.
    assert tracker._validate_destination("") is None


# ── G. Encoded / host-confusion allowlist bypass attempts blocked ──────────────
@pytest.mark.parametrize("bypass", [
    "https://permitmap.org.evil.com/x",     # suffix trick
    "https://permitmap.org@evil.com/x",     # userinfo trick → real host evil.com
    "https://evil.com/permitmap.org",       # path trick
    "https://permitmap.org\t.evil.com",     # embedded control char
    "https://xn--permitmap.org",            # punycode lookalike
])
def test_allowlist_bypass_blocked(client, bypass):
    assert tracker._validate_destination(bypass) is None
    assert _loc(client, "x", bypass) == tracker.SAFE_DEFAULT


# ── No ?to= → legacy Stripe default behavior preserved ─────────────────────────
def test_legacy_no_to_redirects_to_stripe_default(client):
    tid = "9_close_email_lee_roofing"
    loc = _loc(client, tid, None)
    assert loc.startswith(tracker.STRIPE_URL)
    assert f"client_reference_id={tid}" in loc


def test_www_permitmap_allowed():
    assert tracker._validate_destination("https://www.permitmap.org/pricing") \
        == "https://www.permitmap.org/pricing"
