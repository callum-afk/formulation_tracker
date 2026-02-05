from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable, Tuple


FORMAT_VALUES = {"Powder", "Liquid", "Pellet", "Film", "Granules", "Water"}
PACK_UNITS = {"kg", "L"}


class ValidationError(ValueError):
    pass


def validate_pack_size_unit(value: str) -> None:
    if value not in PACK_UNITS:
        raise ValidationError("pack_size_unit must be kg or L")


def validate_pack_size_value(value: int) -> None:
    if value < 1 or value > 1000:
        raise ValidationError("pack_size_value must be between 1 and 1000")


def validate_format(value: str) -> None:
    if value not in FORMAT_VALUES:
        raise ValidationError("format must be one of Powder, Liquid, Pellet, Film, Granules, Water")


def round_weight(value: float) -> Decimal:
    return Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def validate_weight_sum(items: Iterable[Tuple[str, Decimal]]) -> None:
    total = sum((item[1] for item in items), Decimal("0.00"))
    if total != Decimal("100.00"):
        raise ValidationError("Weight percentages must sum to 100.00 after rounding")
