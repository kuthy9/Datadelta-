"""
test_progress.py — The ProgressSink protocol and its two built-in sinks.
"""

from __future__ import annotations

from datadelta.progress import NullProgress, ProgressEvent, ProgressSink, RecordingProgress


def test_both_sinks_satisfy_the_protocol():
    assert isinstance(NullProgress(), ProgressSink)
    assert isinstance(RecordingProgress(), ProgressSink)


def test_null_progress_accepts_every_call_and_returns_none():
    sink = NullProgress()
    assert sink.stage_start("load", "load", total=3, note="a.csv") is None
    assert sink.advance("load", 2, note="x") is None
    assert sink.stage_end("load", "done", summary="3 rows") is None
    assert sink.finding("FAIL") is None
    assert sink.tally({"FAIL": 1}) is None


def test_recording_progress_records_events_in_order():
    sink = RecordingProgress()
    sink.stage_start("load.before", "load before", total=None, note="a.csv")
    sink.advance("load.before")
    sink.stage_end("load.before", "done", summary="10 rows")
    sink.finding("WARN")
    sink.tally({"FAIL": 0, "WARN": 1})

    assert sink.events == [
        ProgressEvent("start",   "load.before", {"label": "load before", "total": None, "note": "a.csv"}),
        ProgressEvent("advance", "load.before", {"n": 1, "note": ""}),
        ProgressEvent("end",     "load.before", {"status": "done", "summary": "10 rows"}),
        ProgressEvent("finding", None,          {"severity": "WARN"}),
        ProgressEvent("tally",   None,          {"counts": {"FAIL": 0, "WARN": 1}}),
    ]


def test_tally_stores_a_copy_of_the_counts():
    sink = RecordingProgress()
    counts = {"FAIL": 1}
    sink.tally(counts)
    counts["FAIL"] = 99
    assert sink.events[0].data["counts"] == {"FAIL": 1}


def test_query_helpers():
    sink = RecordingProgress()
    sink.stage_start("schema", "schema")
    sink.stage_start("distribution", "distribution", total=3)
    sink.advance("distribution", note="region")
    sink.advance("distribution", 2, note="revenue")
    sink.advance("schema")
    sink.stage_end("distribution", "skipped", summary="nothing to compare")
    sink.stage_end("distribution", "done", summary="3 columns")
    sink.finding("FAIL")
    sink.finding("INFO")

    assert sink.stage_keys() == ["schema", "distribution"]
    assert sink.advances("distribution") == 3
    assert sink.advances("schema") == 1
    assert sink.advances("integrity") == 0
    assert sink.advance_notes("distribution") == ["region", "revenue"]
    assert sink.end_status("distribution") == "done"          # the last end event wins
    assert sink.end_summary("distribution") == "3 columns"
    assert sink.end_status("schema") is None                  # never ended
    assert sink.end_summary("schema") is None
    assert sink.findings() == ["FAIL", "INFO"]
