"""PI_BRIDGE_STREAM forwards each new Pi event as its own message."""

import json
import time

import pi_bridge as b


def _reset():
    b._hb_seen.clear()
    b._hb_sent.clear()
    b._stream_off.clear()


def _line(tool=None, text=None):
    parts = []
    if tool:
        parts.append({"type": "toolCall", "name": tool})
    if text:
        parts.append({"type": "text", "text": text})
    return json.dumps({"message": {"content": parts}}) + "\n"


def test_stream_events_reads_only_new_lines(tmp_path):
    path = tmp_path / "s.jsonl"
    path.write_text(_line(tool="bash") + _line(text="all done"), encoding="utf-8")
    events, off = b._stream_events(path, 0)
    assert events == [("tool", "bash"), ("text", "all done")]
    assert off == path.stat().st_size


def test_stream_events_holds_back_partial_line(tmp_path):
    path = tmp_path / "s.jsonl"
    complete = _line(tool="read")
    partial = _line(text="second event")[:-10]
    path.write_text(complete + partial, encoding="utf-8")

    events, off = b._stream_events(path, 0)
    assert events == [("tool", "read")]
    assert off == len(complete.encode())

    path.write_text(complete + _line(text="second event"), encoding="utf-8")
    events, off = b._stream_events(path, off)
    assert events == [("text", "second event")]


def test_stream_tick_posts_one_message_per_event(tmp_path, mk_task, session_file, monkeypatch):
    _reset()
    sent = []
    monkeypatch.setattr(b, "_post_signed", lambda url, payload, event: sent.append(payload))

    row = mk_task(b.db(), "st1", session_id="pi-st1", cwd=str(tmp_path))
    session_file("pi-st1", str(tmp_path), lines=[
        {"message": {"content": [{"type": "toolCall", "name": "bash"}]}},
        {"message": {"content": [{"type": "text", "text": "thinking about it"}]}},
    ])
    b._stream_off["st1"] = 0

    b._stream_tick(row, time.time())

    assert len(sent) == 2
    assert [p["state"] for p in sent] == ["tool", "text"]
    assert len({p["delivery_id"] for p in sent}) == 2


def test_stream_tick_first_sight_streams_fresh_task(tmp_path, mk_task, session_file, monkeypatch):
    _reset()
    sent = []
    monkeypatch.setattr(b, "_post_signed", lambda url, payload, event: sent.append(payload))

    row = mk_task(b.db(), "st9", session_id="pi-st9", cwd=str(tmp_path))
    session_file("pi-st9", str(tmp_path), lines=[
        {"message": {"content": [{"type": "text", "text": "already working"}]}},
    ])

    b._stream_tick(row, time.time())

    assert [p["state"] for p in sent] == ["text"]


def test_stream_tick_silent_without_new_events(tmp_path, mk_task, session_file, monkeypatch):
    _reset()
    sent = []
    monkeypatch.setattr(b, "_post_signed", lambda url, payload, event: sent.append(payload))

    row = mk_task(b.db(), "st2", session_id="pi-st2", cwd=str(tmp_path))
    session_file("pi-st2", str(tmp_path), lines=[
        {"message": {"content": [{"type": "text", "text": "one event"}]}},
    ])
    b._stream_off["st2"] = 0

    b._stream_tick(row, time.time())
    assert len(sent) == 1

    b._stream_tick(row, time.time())
    assert len(sent) == 1
