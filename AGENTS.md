# flume — agent conventions

## Code navigation

- `_docnav/index.md` + `_docnav/symbols-*.md` map every symbol in this repo
  to file + line. Check them BEFORE Grep/Glob sweeps; then Read the exact
  range. Regenerate with `repo-nav index` if they look stale.
- Prefer ranged Reads (offset/limit) over whole-file reads for files
  longer than ~200 lines.
