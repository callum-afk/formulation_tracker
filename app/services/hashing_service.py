from __future__ import annotations

import hashlib
from decimal import Decimal
from typing import Dict, Iterable, List, Tuple


def hash_set(skus: Iterable[str]) -> str:
    canonical = "|".join(sorted(skus))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def hash_weights(items: Iterable[Tuple[str, Decimal]]) -> str:
    parts: List[str] = []
    for sku, wt in sorted(items, key=lambda x: x[0]):
        parts.append(f"{sku}={wt:.2f}")
    canonical = "|".join(parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def hash_batches(items: Iterable[Tuple[str, str]]) -> str:
    parts: List[str] = []
    for sku, batch in sorted(items, key=lambda x: x[0]):
        parts.append(f"{sku}={batch}")
    canonical = "|".join(parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
