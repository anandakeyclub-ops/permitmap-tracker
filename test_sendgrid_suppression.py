import csv, io
from sendgrid_suppression import build_suppression_rows, email_hash

def test_hard_bounce_suppresses():
    out=build_suppression_rows([{"event":"bounce","email":"x@example.com","status":"5.1.1","sg_event_id":"e1"}])
    rows=list(csv.DictReader(io.StringIO(out)))
    assert rows[0]["email_hash"]==email_hash("x@example.com")
    assert rows[0]["event"]=="bounce"

def test_deferred_and_soft_bounce_do_not_suppress():
    out=build_suppression_rows([
        {"event":"deferred","email":"x@example.com","status":"4.4.1"},
        {"event":"bounce","email":"y@example.com","status":"4.2.0","type":"soft"},
    ])
    assert list(csv.DictReader(io.StringIO(out)))==[]

def test_unsubscribe_and_spam_suppress():
    out=build_suppression_rows([
        {"event":"unsubscribe","email":"u@example.com","sg_event_id":"u1"},
        {"event":"spamreport","email":"s@example.com","sg_event_id":"s1"},
    ])
    assert len(list(csv.DictReader(io.StringIO(out))))==2
