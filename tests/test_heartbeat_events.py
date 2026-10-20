"""Heartbeat is a change trigger, not a timer."""

import pi_bridge as b


def _reset():
    b._hb_seen.clear()
    b._hb_sent.clear()


def test_first_snapshot_sends():
    _reset()
    assert b._hb_should_send("t1", (10, 1, "read", "hi"), 1000.0) is True


def test_unchanged_snapshot_is_silent():
    _reset()
    sig = (10, 1, "read", "hi")
    b._hb_seen["t1"] = sig
    b._hb_sent["t1"] = 1000.0
    assert b._hb_should_send("t1", sig, 1000.0 + b.HEARTBEAT_INTERVAL) is False


def test_quiet_task_still_pings():
    _reset()
    sig = (10, 1, "read", "hi")
    b._hb_seen["t1"] = sig
    b._hb_sent["t1"] = 1000.0
    assert b._hb_should_send("t1", sig, 1000.0 + b.HEARTBEAT_IDLE) is True


def test_new_tool_sends_immediately():
    _reset()
    b._hb_seen["t1"] = (10, 1, "read", "hi")
    b._hb_sent["t1"] = 1000.0
    assert b._hb_should_send("t1", (10, 1, "bash", "hi"), 1000.0) is True


def test_keepalive_disabled_when_zero(monkeypatch):
    _reset()
    monkeypatch.setattr(b, "HEARTBEAT_IDLE", 0)
    sig = (10, 1, "read", "hi")
    b._hb_seen["t1"] = sig
    b._hb_sent["t1"] = 1000.0
    assert b._hb_should_send("t1", sig, 1e9) is False
