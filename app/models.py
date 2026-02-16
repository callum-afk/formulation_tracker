from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field, validator


class ApiResponse(BaseModel):
    ok: bool
    data: Optional[dict] = None
    error: Optional[str] = None


class IngredientCreate(BaseModel):
    category_code: int
    trade_name_inci: str
    supplier: str
    spec_grade: Optional[str] = None
    format: str
    pack_size_value: int
    pack_size_unit: str


class IngredientImport(IngredientCreate):
    sku: str


class IngredientBatchCreate(BaseModel):
    sku: str
    ingredient_batch_code: str
    received_at: Optional[str] = None
    notes: Optional[str] = None
    quantity_value: Optional[float] = None
    quantity_unit: Optional[str] = None


class IngredientSetCreate(BaseModel):
    skus: List[str]


class DryWeightItem(BaseModel):
    sku: str
    wt_percent: float


class DryWeightCreate(BaseModel):
    # Enforce two-letter uppercase set codes to keep code formats consistent across UI and API.
    set_code: str
    items: List[DryWeightItem]

    @validator("set_code", pre=True)
    def normalize_set_code(cls, value: str) -> str:
        # Normalize user-entered lowercase codes and reject invalid non-two-letter formats.
        code = (value or "").strip().upper()
        if len(code) != 2 or not code.isalpha():
            raise ValueError("set_code must be exactly two letters (A-Z)")
        return code


class BatchVariantItem(BaseModel):
    sku: str
    ingredient_batch_code: str


class BatchVariantCreate(BaseModel):
    # Enforce two-letter uppercase set and weight codes for batch variant API payloads.
    set_code: str
    weight_code: str
    items: List[BatchVariantItem]

    @validator("set_code", "weight_code", pre=True)
    def normalize_two_letter_codes(cls, value: str) -> str:
        # Normalize to uppercase and block anything that is not exactly two alphabetic characters.
        code = (value or "").strip().upper()
        if len(code) != 2 or not code.isalpha():
            raise ValueError("codes must be exactly two letters (A-Z)")
        return code


class UploadRequest(BaseModel):
    filename: str
    content_type: str
    content_length: int = Field(gt=0)


class UploadConfirm(BaseModel):
    object_path: str
    filename: str
    content_type: str
    content_length: int = Field(gt=0)


class MsdsUploadResponse(BaseModel):
    object_path: str
    filename: str
    content_type: str
    uploaded_at: Optional[str] = None
