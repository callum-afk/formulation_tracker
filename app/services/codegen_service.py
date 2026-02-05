from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class CodegenConfig:
    start_set: int
    start_weight: int
    start_batch: int


def int_to_code(value: int) -> str:
    if value < 0 or value >= 26 * 26:
        raise ValueError("code value out of range")
    first = value // 26
    second = value % 26
    return f"{chr(65 + first)}{chr(65 + second)}"


def format_sku(category_code: int, seq: int, pack_size_value: int) -> str:
    return f"{category_code}{seq:04d}{pack_size_value}"


def parse_sku(sku: str) -> Tuple[int, int, int]:
    if len(sku) < 6:
        raise ValueError("sku too short")
    category_code = int(sku[0])
    seq = int(sku[1:5])
    pack_size_value = int(sku[5:])
    return category_code, seq, pack_size_value
