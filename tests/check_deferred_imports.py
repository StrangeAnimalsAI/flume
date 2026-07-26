"""Find function-level imports that defer nothing.

PLC0415 flags every import inside a function. That is the wrong question:
some of them are load-bearing (an optional vendor SDK that must not be
imported on a core install) and some defer nothing at all, because the
module already pulls the target in transitively at import time.

Only the second kind is a defect, and telling them apart needs the runtime
import graph, not the AST. So: import each module in a clean interpreter,
record sys.modules, and report the function-level imports whose target was
already there.
"""
import ast
import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent / "flume"


def deferred_imports(path: pathlib.Path) -> list[tuple[int, str]]:
    """(lineno, module) for every import inside a function."""
    tree = ast.parse(path.read_text())
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for sub in ast.walk(node):
            if isinstance(sub, ast.Import):
                out += [(sub.lineno, a.name) for a in sub.names]
            elif isinstance(sub, ast.ImportFrom) and sub.module and sub.level == 0:
                out.append((sub.lineno, sub.module))
    return out


def loaded_after_importing(module: str) -> set[str]:
    code = f"import sys, {module}; print(json.dumps(sorted(sys.modules)))"
    code = "import json\n" + code
    res = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    if res.returncode != 0:
        return set()
    return set(json.loads(res.stdout))


pointless, real = [], []
for path in sorted(ROOT.rglob("*.py")):
    if path.name == "__init__.py" and path.parent == ROOT:
        continue
    module = ".".join(path.relative_to(ROOT.parent).with_suffix("").parts)
    imports = deferred_imports(path)
    if not imports:
        continue
    already = loaded_after_importing(module)
    for lineno, target in imports:
        (pointless if target in already else real).append((path, lineno, target))

print(f"POINTLESS — target already loaded when the module finishes importing ({len(pointless)})")
for path, lineno, target in pointless:
    print(f"  {path}:{lineno}  {target}")
print(f"\nREAL — genuinely deferred ({len(real)})")
by_file: dict[str, list[str]] = {}
for path, _lineno, target in real:
    by_file.setdefault(str(path), []).append(target)
for f, targets in sorted(by_file.items()):
    print(f"  {f}: {len(targets)} ({', '.join(sorted(set(targets))[:3])}…)")


if pointless:
    sys.exit(1)
