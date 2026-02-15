from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


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
    set_code: str
    items: List[DryWeightItem]


class BatchVariantItem(BaseModel):
    sku: str
    ingredient_batch_code: str


class BatchVariantCreate(BaseModel):
    set_code: str
    weight_code: str
    items: List[BatchVariantItem]


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
