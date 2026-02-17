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


class LocationPartnerCreate(BaseModel):
    # Require a readable partner label so users can pick this value in the location-code workflow.
    partner_name: str
    # Store machine details as free text, as requested for lightweight partner onboarding.
    machine_specification: str


class LocationCodeCreate(BaseModel):
    # The first three code parts map to set + weight + batch variant and must each be two letters.
    set_code: str
    weight_code: str
    batch_variant_code: str
    # Partner code must also stay in the same two-letter alphabetic format.
    partner_code: str
    # Production date is encoded backwards as YYMMDD for final location IDs.
    production_date: str

    @validator("set_code", "weight_code", "batch_variant_code", "partner_code", pre=True)
    def normalize_location_code_parts(cls, value: str) -> str:
        # Uppercase and validate each part to enforce strict two-letter code hygiene across the flow.
        code = (value or "").strip().upper()
        if len(code) != 2 or not code.isalpha():
            raise ValueError("location code parts must be exactly two letters (A-Z)")
        return code

    @validator("production_date", pre=True)
    def normalize_production_date(cls, value: str) -> str:
        # Accept only YYMMDD numeric values so generated location IDs stay machine-readable.
        normalized = (value or "").strip()
        if len(normalized) != 6 or not normalized.isdigit():
            raise ValueError("production_date must be exactly 6 digits in YYMMDD format")
        return normalized


class CompoundingHowCreate(BaseModel):
    # Existing location code selected by the user as the immutable base for processing code generation.
    location_code: str
    # Two-letter process suffix generated from the code counter and appended to the location code.
    process_code_suffix: str
    # Failure mode must be one of the predefined options configured in the API layer.
    failure_mode: str
    # Optional machine setup sheet URL for operator notes and setup traceability.
    machine_setup_url: Optional[str] = None
    # Optional processed data sheet URL for post-run analysis linkage.
    processed_data_url: Optional[str] = None


class CompoundingHowUpdate(BaseModel):
    # Failure mode remains editable after creation so records can be corrected later.
    failure_mode: str
    # Optional machine setup URL can be added or updated after initial save.
    machine_setup_url: Optional[str] = None
    # Optional processed data URL can be added or updated after initial save.
    processed_data_url: Optional[str] = None


class PelletBagCreate(BaseModel):
    # Persist the existing compounding how code text exactly as entered tokens.
    compounding_how_code: str
    # Product type controls the namespace and suffix token (PR/PF/PI).
    product_type: str
    # Bag mass is immutable after creation and supports decimal values.
    bag_mass_kg: float = Field(ge=0)
    # Allow minting one or many sequential pellet bag codes in one request.
    number_of_bags: int = Field(default=1, ge=1, le=500)
    # Optional data captured during creation and editable after creation.
    short_moisture_percent: Optional[float] = None
    purpose: Optional[str] = None
    reference_sample_taken: Optional[str] = None
    qc_status: Optional[str] = None
    long_moisture_status: Optional[str] = None
    density_status: Optional[str] = None
    injection_moulding_status: Optional[str] = None
    film_forming_status: Optional[str] = None
    injection_moulding_assignee_email: Optional[str] = None
    film_forming_assignee_email: Optional[str] = None
    remaining_mass_kg: Optional[float] = Field(default=None, ge=0)
    notes: Optional[str] = None
    customer: Optional[str] = None

    @validator("compounding_how_code", pre=True)
    def validate_compounding_how_code(cls, value: str) -> str:
        # Normalize spacing while keeping token text unchanged for deterministic code construction.
        normalized = " ".join((value or "").strip().split())
        if not normalized:
            raise ValueError("compounding_how_code is required")
        return normalized

    @validator("product_type", pre=True)
    def validate_product_type(cls, value: str) -> str:
        # Restrict product type to approved code suffixes.
        normalized = (value or "").strip().upper()
        if normalized not in {"PR", "PF", "PI"}:
            raise ValueError("product_type must be one of PR, PF, PI")
        return normalized


class PelletBagUpdate(BaseModel):
    # Editable optional and operational fields for post-creation updates.
    short_moisture_percent: Optional[float] = None
    purpose: Optional[str] = None
    reference_sample_taken: Optional[str] = None
    qc_status: Optional[str] = None
    long_moisture_status: Optional[str] = None
    density_status: Optional[str] = None
    injection_moulding_status: Optional[str] = None
    film_forming_status: Optional[str] = None
    injection_moulding_assignee_email: Optional[str] = None
    film_forming_assignee_email: Optional[str] = None
    remaining_mass_kg: Optional[float] = Field(default=None, ge=0)
    notes: Optional[str] = None
    customer: Optional[str] = None
