from __future__ import annotations

from pathlib import Path

CONFIG = Path(__file__).resolve().parents[1] / "infra" / "collector" / "config.yaml"


def test_non_agent_infra_filter_is_in_trace_pipeline_before_langfuse_projection() -> None:
    text = CONFIG.read_text()

    assert "filter/drop_non_agent_infra:" in text
    assert 'IsMatch(name, "^/?moby\\\\.(buildkit|filesync|auth)\\\\.")' in text
    assert (
        'IsMatch(resource.attributes["service.name"], '
        '"^(docker|buildx|buildkit|moby)($|[._-])")'
    ) in text

    pipeline = next(
        line.strip()
        for line in text.splitlines()
        if line.strip().startswith("processors: [memory_limiter")
    )
    filter_index = pipeline.index("filter/drop_non_agent_infra")
    source_index = pipeline.index("transform/langfuse_source")
    assert filter_index < source_index


def test_non_agent_infra_filter_does_not_match_agent_prefixes() -> None:
    text = CONFIG.read_text()
    filter_block = text.split("filter/drop_non_agent_infra:", 1)[1].split(
        "  # Universal resource attrs.", 1
    )[0]

    assert "claude_code" not in filter_block
    assert "codex" not in filter_block


def test_codex_service_name_classification_overrides_inherited_source() -> None:
    text = CONFIG.read_text()
    source_block = text.split("transform/langfuse_source:", 1)[1].split(
        "  # Batch last.", 1
    )[0]

    codex_service_rule = (
        'set(attributes["langfuse.trace.metadata.agent_source"], "codex") '
        'where attributes["langfuse.trace.metadata.agent_source"] == nil '
        'and IsMatch(resource.attributes["service.name"], "^codex")'
    )
    inherited_source_rule = (
        'set(attributes["langfuse.trace.metadata.agent_source"], '
        'resource.attributes["source"])'
    )

    assert codex_service_rule in source_block
    assert source_block.index(codex_service_rule) < source_block.index(
        inherited_source_rule
    )
    assert 'set(attributes["langfuse.trace.metadata.agent_family"], "codex")' in source_block
    assert (
        'set(attributes["langfuse.trace.tags"], ["agent:codex", "family:codex"])'
        in source_block
    )


def test_claude_name_fallback_still_exists_after_inherited_source_rule() -> None:
    text = CONFIG.read_text()
    source_block = text.split("transform/langfuse_source:", 1)[1].split(
        "  # Batch last.", 1
    )[0]

    inherited_source_rule = (
        'set(attributes["langfuse.trace.metadata.agent_source"], '
        'resource.attributes["source"])'
    )
    claude_name_rule = (
        'set(attributes["langfuse.trace.metadata.agent_source"], "claude-code") '
        'where attributes["langfuse.trace.metadata.agent_source"] == nil '
        'and IsMatch(name, "^claude_code[._]")'
    )

    assert claude_name_rule in source_block
    assert source_block.index(inherited_source_rule) < source_block.index(
        claude_name_rule
    )
