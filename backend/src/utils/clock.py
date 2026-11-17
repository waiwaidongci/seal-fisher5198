"""Single clock for lease timestamps.

All lease values use a fixed-width UTC ISO-8601 representation so that the
database can compare them lexicographically in an atomic conditional UPDATE.
Tests may monkeypatch now_iso/after to advance time.
"""
from datetime import datetime, timedelta, timezone

FORMAT = '%Y-%m-%dT%H:%M:%S.%f+00:00'

def now_iso():
    return datetime.now(timezone.utc).strftime(FORMAT)

def after(seconds):
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).strftime(FORMAT)

def from_iso(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, FORMAT)
    except ValueError:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
