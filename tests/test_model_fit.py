import json

import pi_bridge as b

CATALOG = {
    "providers": {
        "local": {"baseUrl": "http://127.0.0.1:8080/v1", "models": [{"id": "hermes"}]},
        "dead": {"baseUrl": "http://example.invalid/v1", "models": []},
    }
}


def _catalog(tmp_path, monkeypatch, cfg=CATALOG):
    path = tmp_path / "models.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")
    monkeypatch.setattr(b, "PI_MODELS_JSON", path)
    return path


def test_declared_models_lists_ids(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch)
    assert b.declared_models() == {"local": {"hermes"}, "dead": set()}


def test_missing_catalog_is_not_a_rejection(tmp_path, monkeypatch):
    monkeypatch.setattr(b, "PI_MODELS_JSON", tmp_path / "absent.json")
    assert b.model_is_resolvable("inf/combo/deepseek-cheap", None) is True


def test_declared_model_resolves_without_provider(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch)
    assert b.model_is_resolvable("hermes", None) is True


def test_undeclared_model_needs_a_provider(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch)
    assert b.model_is_resolvable("inf/combo/deepseek-cheap", None) is False
    assert b.model_is_resolvable("inf/combo/deepseek-cheap", "local") is True
    assert b.model_is_resolvable("inf/combo/deepseek-cheap", "dead") is False


def test_passthrough_provider_skips_empty(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch)
    assert b.passthrough_provider() == "local"


def test_fit_attaches_provider_for_inherited_model(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch)
    assert b.fit_model_to_pi("inf/combo/deepseek-cheap", None) == (
        "inf/combo/deepseek-cheap", "local", "provider"
    )


def test_fit_leaves_resolvable_pairs_alone(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch)
    assert b.fit_model_to_pi("hermes", None) == ("hermes", None, None)
    assert b.fit_model_to_pi("anything", "local") == ("anything", "local", None)


def test_fit_falls_back_without_passthrough(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch, {"providers": {"dead": {"models": []}}})
    monkeypatch.setattr(b, "FALLBACK_MODEL", "hermes")
    assert b.fit_model_to_pi("inf/combo/deepseek-cheap", None) == ("hermes", None, "model")
