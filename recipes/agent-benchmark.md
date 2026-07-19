# Agent navigation benchmark ("agent CI")

Standing correctness + cost check for navigation tooling and contract
changes. Cost-shaped metrics alone can't catch quality erosion — in the
2026-07-08 baseline run, the *cheaper* condition missed a relevant file.
Re-run this after changing repo-nav, `_docnav` playbooks, nav contract
text in CLAUDE.md/AGENTS.md, or nudge hooks. ~$0.10/run on haiku scouts.

## Protocol

Launch each task twice as `scout` subagents (same model both conditions):

- **docnav condition** — prompt prepends the navigation contract: read
  `<repo>/_docnav/index.md` first, grep the symbols maps, trust the map,
  `repo-nav read --around` only for implementation bodies; no tree grep,
  no whole-file reads.
- **control condition** — "navigate with standard tools however you judge
  best; do NOT use anything under _docnav/ or the repo-nav command."

Agents must return ONLY file:line references (plus a one-line note each),
so answers are mechanically scorable.

## Tasks and ground truth

Verify ground truth before scoring — line numbers drift. Refresh with the
greps shown; update this file when they move.

| Task | Repo | Ask | Ground truth (2026-07-08) |
|---|---|---|---|
| T1 cross-language | biz/sketchup/exploder | geometry-warning banner: HTML definition + every JS show/hide site | `panel.html:826`; `panel.js:941,945` (fns at 940/944 acceptable). Refresh: `grep -n 'geometry-warning' src/exploder/html/panel.html src/exploder/html/js/panel.js` |
| T2 symbol lookup | biz/security/crypto-analysis | definition, signature, return shape of `admin_blast_radius_signals` | `analyzers/summary/admin_blast_radius_signals.py:69` |
| T3 flow trace | biz/notifield | checkout-session view + product-resolving helper | `payment/views.py:32` (`create_session_view`), `:19` (`_get_product`) |
| T4 inventory (recall test) | biz/sketchup/exploder | EVERY element id containing warning/banner/error + JS togglers | 5 elements: `geometry-warning:826`, `error-banner:832`, `error-banner-msg:834`, `notice-banner:842`, `notice-banner-msg:844`; JS at `panel.js:941-992` |

## Scoring

Per agent, from the subagent usage stats and the store (subagents ingest
automatically; find them via `sessions --all` under the parent session):

- **correct** — all ground-truth refs present (T4: count elements found /5)
- **tool calls**, **wall seconds**, **tokens** (from Agent tool usage)
- **call precision** — classify each tool call target/map/search/offtarget
  against ground truth (see `_docnav`-aware classifier in
  `flume/store/navtime.py:classify_tool` plus the task's target
  file set)

## Baseline (2026-07-08, haiku scouts)

| Metric | docnav | control |
|---|---|---|
| correctness | 4/4 (T4: 5/5 elements) | 3.5/4 (T4: 4/5, one hedged) |
| tool calls | 31 | 21 |
| wall time | 127.7s | 93.8s (contract then required confirm-reads; since removed) |
| tokens | 46.0k | 88.4k |
| on-task call rate | 90% | 100% |

Interpretation guardrails: single-turn scouts hide context-compounding
(the token column is the one that matters for long sessions); n=4 pairs
is directional. Log each rerun by tagging a `bench-<change>` experiment
window so results live in the store next to everything else.
