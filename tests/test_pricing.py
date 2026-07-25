"""Pricing registry: defaults, config override, premium derivation."""
from __future__ import annotations

from pathlib import Path

import pytest

from flume.pricing import Price, load_prices


def _config(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "config.toml"
    path.write_text(body)
    return path


def test_defaults_price_known_models_and_leave_others_unpriced(tmp_path: Path) -> None:
    book = load_prices(tmp_path / "missing.toml")
    assert book.for_model("claude-opus-5") == Price(5.0, 25.0)
    # Dated snapshot ids resolve through their family prefix.
    assert book.for_model("claude-haiku-4-5-20251001") == Price(1.0, 5.0)
    # An unknown vendor's model is unpriced, NOT silently free.
    assert book.for_model("gpt-5.6-sol") is None
    assert book.for_model(None) is None


def test_config_adds_and_overrides_models(tmp_path: Path) -> None:
    book = load_prices(_config(tmp_path, """
[pricing]
"gpt-5.6-sol" = { input = 1.25, output = 10.0 }
"local-llama" = { input = 0.0, output = 0.0 }
"claude-opus-5" = { input = 4.0, output = 20.0 }
"""))
    assert book.for_model("gpt-5.6-sol") == Price(1.25, 10.0)
    # A locally-hosted model can be declared free — distinct from unpriced.
    assert book.for_model("local-llama").output == 0.0
    # Config wins over the shipped default.
    assert book.for_model("claude-opus-5").input == 4.0


def test_longest_prefix_wins(tmp_path: Path) -> None:
    book = load_prices(_config(tmp_path, """
[pricing]
"gpt-5" = { input = 1.0, output = 4.0 }
"gpt-5.6-terra" = { input = 9.0, output = 40.0 }
"""))
    assert book.for_model("gpt-5.6-sol").input == 1.0       # family fallback
    assert book.for_model("gpt-5.6-terra-x").input == 9.0   # specific entry


def test_cache_multipliers_default_and_override(tmp_path: Path) -> None:
    book = load_prices(_config(tmp_path, """
[pricing."odd-cache"]
input = 10.0
output = 20.0
cache_read_multiplier = 0.25
cache_write_multiplier = 1.0
"""))
    default = book.for_model("claude-opus-5")
    assert default.cache_read == pytest.approx(0.5)   # 5.0 * 0.1
    assert default.cache_write == pytest.approx(6.25)  # 5.0 * 1.25
    odd = book.for_model("odd-cache")
    assert odd.cache_read == pytest.approx(2.5)
    assert odd.cache_write == pytest.approx(10.0)


def test_premium_is_derived_from_price_not_vendor_name(tmp_path: Path) -> None:
    book = load_prices(_config(tmp_path, """
[pricing]
"expensive-newcomer" = { input = 15.0, output = 75.0 }
"budget-model" = { input = 0.5, output = 2.0 }
"""))
    # A vendor flume has never heard of counts as premium purely on price.
    assert book.is_premium("expensive-newcomer")
    assert not book.is_premium("budget-model")
    assert book.is_premium("claude-opus-5")      # $25 output
    assert not book.is_premium("claude-haiku-4-5")
    assert "expensive-newcomer" in book.premium_models()


def test_premium_threshold_is_configurable(tmp_path: Path) -> None:
    book = load_prices(_config(tmp_path, """
[pricing]
premium_output_threshold = 4.0
"budget-model" = { input = 0.5, output = 5.0 }
"""))
    assert book.is_premium("budget-model")


def test_shorthand_list_form(tmp_path: Path) -> None:
    book = load_prices(_config(tmp_path, """
[pricing]
"terse" = [2.0, 8.0]
"""))
    assert book.for_model("terse") == Price(2.0, 8.0)


def test_malformed_entries_raise_clearly(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="missing output"):
        load_prices(_config(tmp_path, '[pricing]\n"x" = { input = 1.0 }\n'))
    with pytest.raises(ValueError, match="list form"):
        load_prices(_config(tmp_path, '[pricing]\n"x" = [1.0]\n'))


def test_index_markers_are_configurable(tmp_path: Path, monkeypatch) -> None:
    """The agent-index detector must not hardcode one tool's convention."""
    from flume.analysis.insights import DEFAULT_INDEX_MARKERS, _index_markers

    monkeypatch.setenv("FLUME_CONFIG", str(tmp_path / "none.toml"))
    assert _index_markers() == list(DEFAULT_INDEX_MARKERS)
    # Defaults cover the common conventions, not just one tool's.
    assert "AGENTS.md" in DEFAULT_INDEX_MARKERS

    config = _config(tmp_path, """
[insights]
index_markers = [".myagent/index", "NOTES.md"]
""")
    monkeypatch.setenv("FLUME_CONFIG", str(config))
    assert _index_markers() == [".myagent/index", "NOTES.md"]
