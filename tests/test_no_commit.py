import os
import stat
import pi_bridge as b
def test_no_commit_env_sets_hooks_path():
    env = b.no_commit_env()
    assert env["PI_BRIDGE_NO_COMMIT"] == "1"
    assert env["GIT_CONFIG_KEY_0"] == "core.hooksPath"
    assert env["GIT_CONFIG_VALUE_0"] == str(b.HOOKS_DIR)
    assert env["GIT_CONFIG_COUNT"] == "1"
def test_no_commit_env_appends_to_existing_config(monkeypatch):
    monkeypatch.setenv("GIT_CONFIG_COUNT", "2")
    env = b.no_commit_env()
    assert env["GIT_CONFIG_COUNT"] == "3"
    assert env["GIT_CONFIG_KEY_2"] == "core.hooksPath"
def test_hooks_exist_and_are_executable():
    for name in ("pre-commit", "pre-push"):
        path = b.HOOKS_DIR / name
        assert path.is_file(), f"{name} missing"
        assert path.stat().st_mode & stat.S_IXUSR, f"{name} not executable"
def test_hooks_refuse_only_when_marked(monkeypatch, tmp_path):
    import subprocess
    for name in ("pre-commit", "pre-push"):
        path = b.HOOKS_DIR / name
        blocked = subprocess.run([str(path)], env={**os.environ, "PI_BRIDGE_NO_COMMIT": "1"})
        assert blocked.returncode == 1, f"{name} did not block"
        allowed = subprocess.run(
            [str(path)], env={k: v for k, v in os.environ.items() if k != "PI_BRIDGE_NO_COMMIT"}
        )
        assert allowed.returncode == 0, f"{name} blocked without the marker"
def test_submit_passes_no_commit_to_pi(monkeypatch):
    monkeypatch.setattr(b, "resolve_model", lambda *a, **k: ("hermes", "session-alias"))
    monkeypatch.setattr(b, "fit_model_to_pi", lambda m, p: (m, p, None))
    monkeypatch.setattr(b, "ensure_provider_placeholder", lambda: None)
    seen = {}
    class _Proc:
        pid = 4244
    def _popen(*a, **kw):
        seen.update(kw.get("env") or {})
        return _Proc()
    monkeypatch.setattr(b.subprocess, "Popen", _popen)
    b.submit("probe", "/tmp", no_commit=True)
    assert seen.get("PI_BRIDGE_NO_COMMIT") == "1"
    assert seen.get("GIT_CONFIG_VALUE_0") == str(b.HOOKS_DIR)
    seen.clear()
    b.submit("probe", "/tmp")
    assert "PI_BRIDGE_NO_COMMIT" not in seen
