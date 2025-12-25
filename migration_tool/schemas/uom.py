"""
Pydantic Schemas for uom.uom

Validation schemas for Unit of Measure data.
"""

from typing import Any, Literal
from pydantic import BaseModel, Field, field_validator


UoMType = Literal["bigger", "reference", "smaller"]


class UoMBaseSchema(BaseModel):
    """Base schema for UoM fields."""
    
    name: str = Field(..., min_length=1, max_length=64, description="Unit name")
    
    # Category
    category_id: int | str | None = Field(
        default=None, description="UoM category (ID or name)"
    )
    
    # Type and conversion
    uom_type: UoMType = Field(
        default="reference",
        description="UoM type: reference, bigger, or smaller"
    )
    factor: float = Field(
        default=1.0, gt=0,
        description="Ratio to reference UoM (for bigger/smaller)"
    )
    factor_inv: float = Field(
        default=1.0, gt=0,
        description="Inverse ratio (computed, but can be set)"
    )
    
    # Precision
    rounding: float = Field(
        default=0.01, gt=0,
        description="Rounding precision"
    )
    
    # Status
    active: bool = Field(default=True, description="Active status")
    
    @field_validator("name", mode="before")
    @classmethod
    def validate_name(cls, v: Any) -> str:
        if v is None:
            raise ValueError("UoM name is required")
        name = str(v).strip()
        if not name:
            raise ValueError("UoM name cannot be empty")
        return name
    
    @field_validator("factor", "factor_inv", "rounding", mode="before")
    @classmethod
    def validate_positive_float(cls, v: Any) -> float:
        if v is None or v == "":
            return 1.0
        try:
            val = float(v)
            return val if val > 0 else 1.0
        except (ValueError, TypeError):
            return 1.0

    model_config = {
        "extra": "allow",
    }


class UoMSchema(UoMBaseSchema):
    """Full UoM schema including read-only fields."""
    
    id: int | None = Field(default=None, description="Odoo record ID")


class UoMCreateSchema(UoMBaseSchema):
    """Schema for creating new UoMs."""
    
    def to_odoo_dict(self) -> dict[str, Any]:
        """Convert to dictionary for Odoo create/write."""
        data = self.model_dump(exclude_none=True)
        return data


class UoMCategorySchema(BaseModel):
    """Schema for UoM categories."""
    
    id: int | None = Field(default=None, description="Odoo record ID")
    name: str = Field(..., min_length=1, max_length=64, description="Category name")
    
    @field_validator("name", mode="before")
    @classmethod
    def validate_name(cls, v: Any) -> str:
        if v is None:
            raise ValueError("UoM category name is required")
        return str(v).strip()

    model_config = {
        "extra": "allow",
    }


class UoMImportRow(BaseModel):
    """Schema for validating raw UoM import data."""
    
    # Common column name aliases
    unit_name: str | None = Field(default=None, alias="Unit Name")
    unit_category: str | None = Field(default=None, alias="Unit Category")
    unit_type: str | None = Field(default=None, alias="Unit Type")
    conversion_factor: float | None = Field(default=None, alias="Conversion Factor")

    model_config = {
        "extra": "allow",
        "populate_by_name": True,
    }
