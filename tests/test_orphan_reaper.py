import pi_bridge as b

def _orphan_proc(monkeypatch, pid, task_id, cwd, killed):
    monkeypatch.setattr(b, "iter_pi_procs", lambda: [(pid, task_id, cwd)])
    monkeypatch.setattr(
        b, "os",
        type("_Os", (), {"kill": staticmethod(lambda p, s: killed.append(p))})(),
    )

def test_reaps_orphan_whose_session_went_stale(monkeypatch, session_file):
    killed = []
    _orphan_proc(monkeypatch, 4242, "deadbeef", "/tmp/orphan-a", killed)
    session_file("pi-deadbeef", "/tmp/orphan-a", age_s=b.ORPHAN_IDLE + 60)
    assert b.reap_orphans() == [4242]
    assert killed == [4242]

def test_leaves_task_with_a_running_row_alone(monkeypatch, mk_task, session_file):
    mk_task(b.db(), task_id="live0001", status="running")
    killed = []
    _orphan_proc(monkeypatch, 4243, "live0001", "/tmp/orphan-b", killed)
    session_file("pi-live0001", "/tmp/orphan-b", age_s=b.ORPHAN_IDLE + 60)
    assert b.reap_orphans() == []
    assert killed == []

def test_leaves_recently_active_process_alone(monkeypatch, session_file):
    killed = []
    _orphan_proc(monkeypatch, 4244, "fresh001", "/tmp/orphan-c", killed)
    session_file("pi-fresh001", "/tmp/orphan-c", age_s=1)
    assert b.reap_orphans() == []
    assert killed == []

def test_leaves_process_without_session_file_alone(monkeypatch):
    killed = []
    _orphan_proc(monkeypatch, 4245, "nosess01", "/tmp/orphan-d", killed)
    assert b.reap_orphans() == []
    assert killed == []
