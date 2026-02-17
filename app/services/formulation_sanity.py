from __future__ import annotations

from typing import Any, Iterable


def _coerce_percent(value: Any) -> float:
    """Convert backend dry-weight values into float percentages for validation checks."""
    if value is None or value == "":
        return 0.0
    return float(value)


def sum_sku_percentages(items: Iterable[dict[str, Any]]) -> float:
    """Return the percentage total for a formulation's dry-weight item collection."""
    total = 0.0
    for item in items:
        # Read the canonical wt_percent field from each SKU record.
        total += _coerce_percent(item.get("wt_percent"))
    return total


def percentages_sum_to_100(items: Iterable[dict[str, Any]], tolerance: float = 0.01) -> bool:
    """Validate that SKU dry weights add up to 100% within a small floating-point tolerance."""
    return abs(sum_sku_percentages(items) - 100.0) <= tolerance
