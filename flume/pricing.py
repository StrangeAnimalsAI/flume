"""Model pricing in $/MTok, extendable from config.

Ships rates for the models flume's own authors use so `analyze cost` works
out of the box, but pricing is data, not code: any model — another vendor's,
a locally-hosted one, a release newer than this file — is priced from
`~/.flume/config.toml` without touching flume:

    [pricing]
    "gpt-5.6-sol" = { input = 1.25, output = 10.0 }
    "local-llama" = { input = 0.0, output = 0.0 }   # no marginal cost
    "claude-opus-5" = { input = 4.0, output = 20.0 }  # negotiated rate

Config entries override a shipped default of the same name and add models
that aren't shipped at all. Matching is longest-prefix, so an entry for
`gpt-5` prices every `gpt-5.*` variant unless a longer key matches first.

Cache accounting follows the convention the shipped rates use: a cache read
costs 0.1x the input rate, a cache write 1.25x (the 5-minute TTL premium).
Vendors that price caching differently override per entry:

    [pricing."some-model"]
    input = 1.0
    output = 3.0
    cache_read_multiplier = 0.25
    cache_write_multiplier = 1.0

A model with no matching entry is reported as UNPRICED, never as zero —
silently costing an unknown model at $0 understates spend and hides the
gap. `analyze cost` surfaces the unpriced turn count per group.
"""
from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

DEFAULT_CONFIG_PATH = Path.home() / ".flume" / "config.toml"

# Cache multipliers relative to the input rate.
_CACHE_READ = 0.1
_CACHE_WRITE = 1.25

# A model counts as "premium" when its output rate is at or above this many
# $/MTok. Derived rather than name-matched so the notion travels to any
# vendor: detectors that flag premium-model waste work for a $75/MTok model
# nobody has shipped yet. Override with `premium_output_threshold`.
DEFAULT_PREMIUM_OUTPUT_THRESHOLD = 20.0


@dataclass(frozen=True)
class Price:
    """$/MTok for one model, plus how that vendor prices cache traffic."""

    input: float
    output: float
    cache_read_multiplier: float = _CACHE_READ
    cache_write_multiplier: float = _CACHE_WRITE

    @property
    def cache_read(self) -> float:
        return self.input * self.cache_read_multiplier

    @property
    def cache_write(self) -> float:
        return self.input * self.cache_write_multiplier


# Anthropic list rates, verified 2026-07-24 against the published pricing
# table. Keys are matched as prefixes, so a dated snapshot id like
# `claude-haiku-4-5-20251001` resolves through its family key.
#
# Rates go stale. Anything wrong or missing here is a one-line config fix
# on the user's side — do not edit this table to price another vendor's
# models, because nobody maintaining flume can verify those.
_DEFAULTS: dict[str, Price] = {
    "claude-fable-5": Price(10.0, 50.0),
    "claude-mythos-5": Price(10.0, 50.0),
    "claude-opus-5": Price(5.0, 25.0),
    "claude-opus-4-8": Price(5.0, 25.0),
    "claude-opus-4-7": Price(5.0, 25.0),
    "claude-opus-4-6": Price(5.0, 25.0),
    "claude-opus-4-5": Price(5.0, 25.0),
    # Sonnet 5 introductory pricing runs through 2026-08-31; list is $3/$15.
    "claude-sonnet-5": Price(2.0, 10.0),
    "claude-sonnet-4-6": Price(3.0, 15.0),
    "claude-sonnet-4-5": Price(3.0, 15.0),
    "claude-haiku-4-5": Price(1.0, 5.0),
}


@dataclass(frozen=True)
class PriceBook:
    """Resolved prices plus the premium threshold, ready to query."""

    prices: dict[str, Price]
    premium_output_threshold: float = DEFAULT_PREMIUM_OUTPUT_THRESHOLD

    def for_model(self, model: str | None) -> Price | None:
        """Longest-prefix match; None when the model is unpriced."""
        if not model:
            return None
        best: str | None = None
        for key in self.prices:
            if model.startswith(key) and (best is None or len(key) > len(best)):
                best = key
        return self.prices[best] if best is not None else None

    def premium_models(self) -> list[str]:
        """Model keys priced at or above the premium output threshold."""
        return sorted(
            key
            for key, price in self.prices.items()
            if price.output >= self.premium_output_threshold
        )

    def is_premium(self, model: str | None) -> bool:
        price = self.for_model(model)
        return price is not None and price.output >= self.premium_output_threshold

    def known(self) -> list[str]:
        return sorted(self.prices)


def load_prices(path: Path | None = None) -> PriceBook:
    """Shipped rates, with `[pricing]` from config layered on top."""
    resolved = path or Path(
        os.environ.get("FLUME_CONFIG", str(DEFAULT_CONFIG_PATH))
    )
    prices = dict(_DEFAULTS)
    threshold = DEFAULT_PREMIUM_OUTPUT_THRESHOLD
    if not resolved.is_file():
        return PriceBook(prices, threshold)
    with open(resolved, "rb") as fh:
        doc = tomllib.load(fh)
    section = doc.get("pricing") or {}
    for name, entry in section.items():
        if name == "premium_output_threshold":
            threshold = float(entry)
            continue
        prices[name] = _parse_entry(name, entry)
    return PriceBook(prices, threshold)


def _parse_entry(name: str, entry: object) -> Price:
    """Accept `{input, output, ...}` or the shorthand `[input, output]`."""
    if isinstance(entry, (list, tuple)):
        if len(entry) != 2:
            raise ValueError(
                f"bad pricing entry for {name!r}: list form is [input, output]"
            )
        return Price(float(entry[0]), float(entry[1]))
    if not isinstance(entry, dict):
        raise ValueError(
            f"bad pricing entry for {name!r}: expected a table or [input, output]"
        )
    missing = {"input", "output"} - set(entry)
    if missing:
        raise ValueError(
            f"pricing entry {name!r} is missing {', '.join(sorted(missing))}"
        )
    return Price(
        input=float(entry["input"]),
        output=float(entry["output"]),
        cache_read_multiplier=float(entry.get("cache_read_multiplier", _CACHE_READ)),
        cache_write_multiplier=float(
            entry.get("cache_write_multiplier", _CACHE_WRITE)
        ),
    )
