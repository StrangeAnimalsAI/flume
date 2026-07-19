# Agent spend analysis — 2026-07-02

Source: the local session store (`~/.flume/store.sqlite3`), 2,693
sessions (1,655 Claude Code, 1,038 Codex), 255,084 tool calls, 313k turns,
spanning 2026-03-30 → 2026-07-02. Reproduce any number with
`flume analyze` or SQL against the store.

## Headline: per-turn context load

Every turn re-sends accumulated context. The two agents differ by ~600x in
what that costs:

| source | avg prompt/turn | uncached/turn | cache hit | total uncached input |
|---|---|---|---|---|
| claude-code | 207k tok | 0.3k | 99.8% | 39M |
| codex | 250k tok | **128.4k** | 48.6% | **25.1B** |

Codex re-billed ~128k uncached tokens on each of 195k turns. This is the
dominant cost in the corpus, an order of magnitude above everything else.

## Why: navigation floods the context

Tool-result bytes returned to the model, by class (all time):

| source | class | calls | tool time | result bytes | errors |
|---|---|---|---|---|---|
| codex | navigate-shell | 98,805 | 45.3h | **5.16 GB** | 1,265 |
| claude-code | navigate (Read/Grep/Glob) | 17,187 | 11.3h | 0.11 GB | 154 |
| codex | interactive-shell (write_stdin) | 21,991 | 42.6h | 0.11 GB | 0 |
| claude-code | navigate-shell | 29,437 | 212.9h | 0.04 GB | 1,064 |
| codex | other-shell | 33,159 | 81.0h | 0.06 GB | 857 |
| codex | mutate (apply_patch) | 11,836 | 53.5h | ~0 | 4 |
| claude-code | delegate (subagents) | 544 | 88.2h | ~0 | 6 |

94% of all result bytes (5.3 of 5.6 GB) are navigation. Codex's share is
5.16 GB because its shell navigation is unbounded:

- `rg`: 17,512 calls returning **4.8 GB** (~275 KB average per call).
- `sed -n`: 42,731 slice-reads (18h of tool time paging files).
- `git`: 30,787 calls; `nl`: 7,288; `find`: 3,995.

Claude's structured Read/Grep/Glob did equivalent work in 0.11 GB because
those tools cap output. Every flooded byte rides in context for the rest of
the session and is re-billed at Codex's 51% miss rate. Long resumed threads
compound it (one archived rollout: ~35 resumes, 4.8 GB raw).

## Secondary waste

- **Duplicate identical calls:** 25,072 (28.8h tool time, 55 MB re-fed).
  Much is legitimate polling; the byte-identical subset includes 626 failed
  `StructuredOutput` calls — schema-retry loops in workflow subagents.
- **Throwaway inline scripts:** 5,503 in 30 days across 553 sessions; 62
  shapes rewritten in 3+ sessions. Top: a `json.load` stats one-liner
  rewritten in **362 sessions**; a `Counter` histogram shape in 120.
- **Whole-file Reads ≥50k chars (no range):** 162 calls / 9 MB — minor;
  Claude's Read limits mostly prevent this.
- **Errors:** 4,865 failing calls total (Bash 1,554; codex exec 1,265).

## docnav validation (bounty-docnav A/B)

Claude sessions in Expensify checkouts with ≥10 turns, June 2026 — sessions
that consulted `_docnav/*` vs not:

| | n | median out tok | median tools | median turns | median wall |
|---|---|---|---|---|---|
| with docnav | 15 | 8,419 | 21 | 33 | 2m |
| without | 213 | 11,375 | 35 | 53 | 6m |

Directional (task mix differs), but consistent: ~30–40% fewer tool
calls/turns and ~3x faster wall time when a source-derived index exists.

## Ranked wins

1. **Bound Codex navigation output.** A capped search/read CLI (repo-nav)
   mandated via AGENTS.md turns 275 KB greps into ~2 KB answers. Halving
   average context at current miss rates is on the order of 10B+ input
   tokens over a comparable period.
2. **Generalize docnav into a per-repo index generator** (repo-nav `index`).
   Validated by the A/B; also shrinks navigation output (grep the index,
   not the tree), compounding with #1.
3. **Durable tools for the top script clusters** (grading-stats,
   matrix-stats, catalog-query CLIs).
4. **Fix `StructuredOutput` retry loops** (schema bug, 626 wasted calls) and
   optionally a PreToolUse repeat-guard hook backed by the store.
5. **Codex session hygiene:** fresh threads per task instead of multi-resume
   marathons; improves cache hit and bounds rollout size.

## Known limits of this analysis

- Dollar figures omitted: token counts are exact, prices vary per model.
- Claude `navigate-shell` time (212.9h) includes long-running/waiting
  commands the classifier can't separate from pure navigation.
- Codex reasoning (48.2M output tokens) is encrypted at source; counted,
  not readable.
- Per-turn active time is absent from recent Claude transcripts
  (`turn_duration` stopped being emitted ~May 2026); wall time is used.
