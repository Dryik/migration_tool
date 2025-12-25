"""
Pydantic Schemas for product.template and product.product

Validation schemas for product data.
"""

from typing import Any, Literal
from pydantic import BaseModel, Field, field_validator


ProductType = Literal["consu", "service", "product"]


class ProductBaseSchema(BaseModel):
    """Base schema for product fields."""
    
    name: str = Field(..., min_length=1, max_length=256, description="Product name")
    
    # Identification
    default_code: str | None = Field(
        default=None, max_length=64, description="Internal reference / SKU"
    )
    barcode: str | None = Field(default=None, max_length=64, description="Barcode (EAN13, UPC, etc.)")
    
    # Classification
    detailed_type: ProductType = Field(
        default="consu",
        description="Product type: consu (consumable), service, product (storable)"
    )
    categ_id: int | str | None = Field(
        default=None, description="Product category (ID or name)"
    )
    
    # Pricing
    list_price: float = Field(default=0.0, ge=0, description="Sales price")
    standard_price: float = Field(default=0.0, ge=0, description="Cost price")
    
    # Units
    uom_id: int | str | None = Field(default=None, description="Unit of Measure (ID or name)")
    uom_po_id: int | str | None = Field(
        default=None, description="Purchase Unit of Measure (ID or name)"
    )
    
    # Sales & Purchase
    sale_ok: bool = Field(default=True, description="Can be sold")
    purchase_ok: bool = Field(default=True, description="Can be purchased")
    
    # Inventory
    tracking: Literal["none", "serial", "lot"] = Field(
        default="none", description="Tracking type"
    )
    weight: float = Field(default=0.0, ge=0, description="Weight in kg")
    volume: float = Field(default=0.0, ge=0, description="Volume in m³")
    
    # Description
    description: str | None = Field(default=None, description="Internal description")
    description_sale: str | None = Field(default=None, description="Sales description")
    description_purchase: str | None = Field(default=None, description="Purchase description")
    
    # Status
    active: bool = Field(default=True, description="Active status")
    
    @field_validator("name", mode="before")
    @classmethod
    def validate_name(cls, v: Any) -> str:
        if v is None:
            raise ValueError("Product name is required")
        name = str(v).strip()
        if not name:
            raise ValueError("Product name cannot be empty")
        return name
    
    @field_validator("list_price", "standard_price", "weight", "volume", mode="before")
    @classmethod
    def validate_numeric(cls, v: Any) -> float:
        if v is None or v == "":
            return 0.0
        try:
            return float(v)
        except (ValueError, TypeError):
            return 0.0

    model_config = {
        "extra": "allow",
    }


class ProductTemplateSchema(ProductBaseSchema):
    """Full product template schema."""
    
    id: int | None = Field(default=None, description="Odoo record ID")
    
    # Variants
    product_variant_ids: list[int] | None = Field(
        default=None, description="Product variant IDs"
    )
    product_variant_count: int | None = Field(
        default=None, description="Number of variants"
    )
    
    # Taxes
    taxes_id: list[int] | None = Field(default=None, description="Customer tax IDs")
    supplier_taxes_id: list[int] | None = Field(default=None, description="Vendor tax IDs")


class ProductProductSchema(ProductBaseSchema):
    """Schema for product.product (variants)."""
    
    id: int | None = Field(default=None, description="Odoo record ID")
    product_tmpl_id: int | str | None = Field(
        default=None, description="Product template (ID or name)"
    )
    
    # Variant-specific fields
    product_template_attribute_value_ids: list[int] | None = Field(
        default=None, description="Variant attribute value IDs"
    )
    combination_indices: str | None = Field(
        default=None, description="Variant combination string"
    )


class ProductCreateSchema(ProductBaseSchema):
    """Schema for creating new products."""
    
    # Image (base64 encoded)
    image_1920: str | None = Field(default=None, description="Main product image (base64)")
    
    # Additional fields for creation
    company_id: int | str | None = Field(default=None, description="Company (ID or name)")
    
    def to_odoo_dict(self) -> dict[str, Any]:
        """Convert to dictionary for Odoo create/write."""
        data = self.model_dump(exclude_none=True)
        return data


class ProductImportRow(BaseModel):
    """Schema for validating raw product import data."""
    
    # Common column name aliases
    product_name: str | None = Field(default=None, alias="Product Name")
    sku: str | None = Field(default=None, alias="SKU")
    internal_reference: str | None = Field(default=None, alias="Internal Reference")
    product_code: str | None = Field(default=None, alias="Product Code")
    
    category: str | None = Field(default=None, alias="Category")
    product_type: str | None = Field(default=None, alias="Product Type")
    
    sale_price: float | None = Field(default=None, alias="Sale Price")
    cost_price: float | None = Field(default=None, alias="Cost Price")
    
    unit: str | None = Field(default=None, alias="Unit")
    
    model_config = {
        "extra": "allow",
        "populate_by_name": True,
    }
