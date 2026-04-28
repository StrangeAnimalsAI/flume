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
