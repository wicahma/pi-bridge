import os
import subprocess
import sys
from pathlib import Path

import pi_bridge as b


def _bridge_dir_with(**env):
    base = {k: v for k, v in os.environ.items()
            if not k.startswith(("PI_BRIDGE_STATE_DIR", "PI_BRIDGE_DIR", "XDG_STATE_HOME"))}
    base.update(env)
    out = subprocess.run(
        [sys.executable, "-c", "import pi_bridge as b; print(b.BRIDGE_DIR)"],
        capture_output=True, text=True, cwd=str(Path(b.__file__).parent), env=base,
    )
    assert out.returncode == 0, out.stderr
    return Path(out.stdout.strip())


def test_state_dir_defaults_to_xdg(tmp_path):
    assert _bridge_dir_with(XDG_STATE_HOME=str(tmp_path)) == tmp_path / "pi-bridge"


def test_state_dir_empty_xdg_is_not_relative():
    assert _bridge_dir_with(XDG_STATE_HOME="") == Path.home() / ".local" / "state" / "pi-bridge"


def test_state_dir_explicit_beats_xdg(tmp_path):
    got = _bridge_dir_with(PI_BRIDGE_STATE_DIR=str(tmp_path / "s"), XDG_STATE_HOME=str(tmp_path))
    assert got == tmp_path / "s"


def test_session_dir_slug():
    assert b.pi_session_dir_for("/home/u/repo") == b.PI_SESSIONS_DIR / "--home-u-repo--"


def test_find_session_file(session_file):
    p = session_file("pi-abc", "/tmp/x")
    assert b.find_session_file("pi-abc", "/tmp/x") == p
    assert b.find_session_file("pi-nope", "/tmp/x") is None


def test_log_error_ignores_pi_startup_warnings(tmp_path):
    log = tmp_path / "a.log"
    log.write_text(
        "Warning: No project session found with id 'pi-x'; creating a new session with that id.\n"
        'Warning: Extension package "builtin:mcp": Extension /x registers command `/mcp`.\n'
        'Warning: Model "inf/combo/x" not found for provider "local". Using custom model id.\n',
        encoding="utf-8",
    )
    assert b._read_log_error(str(log)) is None


def test_log_error_ignores_prose_that_quotes_failure(tmp_path):
    log = tmp_path / "b.log"
    log.write_text(
        "All 1075 tests pass; the previously Failed assertion is now green.\n"
        "I checked that nothing raised an Error anywhere.\n",
        encoding="utf-8",
    )
    assert b._read_log_error(str(log)) is None


def test_log_error_captures_real_error(tmp_path):
    log = tmp_path / "c.log"
    log.write_text(
        "Warning: Model \"x\" not found for provider \"local\". Using custom model id.\n"
        'Error: Model "nope" not found. Use --list-models to see available models.\n',
        encoding="utf-8",
    )
    assert b._read_log_error(str(log)) == (
        'Error: Model "nope" not found. Use --list-models to see available models.'
    )


def test_log_error_missing_file_is_none(tmp_path):
    assert b._read_log_error(str(tmp_path / "nope.log")) is None
    assert b._read_log_error(None) is None
