"""Harness backend registry: resolution, aliases, dependency reporting."""
from __future__ import annotations

import pytest

from flume.harness.backends import get_backend, names


def test_every_backend_resolves_and_describes_itself() -> None:
    assert names() == ["anthropic", "claude-sdk", "openai"]
    for name in names():
        backend = get_backend(name)
        assert backend.name == name
        assert backend.describe
        assert callable(backend.run)


def test_old_flag_values_still_resolve() -> None:
    # --backend api/sdk predate named backends; breaking them would break
    # any script or service definition written before the split.
    assert get_backend("api").name == "anthropic"
    assert get_backend("sdk").name == "claude-sdk"


def test_only_the_sdk_backends_run_async() -> None:
    assert get_backend("claude-sdk").is_async
    assert not get_backend("anthropic").is_async
    assert not get_backend("openai").is_async


def test_unknown_backend_lists_the_known_ones() -> None:
    with pytest.raises(SystemExit, match="anthropic, claude-sdk, openai"):
        get_backend("gemini")


def test_missing_optional_dependency_names_the_extra(monkeypatch) -> None:
    """Vendor SDKs are optional extras; a missing one must say what to
    install rather than surfacing a bare ImportError."""
    import builtins

    real_import = builtins.__import__

    def fail_anthropic(name, *args, **kwargs):
        if name == "anthropic":
            raise ImportError("no module named anthropic")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fail_anthropic)
    with pytest.raises(SystemExit, match=r"flume\[anthropic\]"):
        get_backend("anthropic")


def test_openai_backend_needs_no_vendor_package(monkeypatch) -> None:
    """It speaks the wire format over stdlib, so it works on a core install."""
    import builtins

    real_import = builtins.__import__

    def fail_vendor_sdks(name, *args, **kwargs):
        if name in ("anthropic", "claude_agent_sdk"):
            raise ImportError(f"no module named {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fail_vendor_sdks)
    assert get_backend("openai").name == "openai"
