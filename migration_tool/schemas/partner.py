"""
Pydantic Schemas for res.partner

Validation schemas for customer and vendor data.
"""

from typing import Any
from pydantic import BaseModel, Field, EmailStr, field_validator, model_validator


class PartnerBaseSchema(BaseModel):
    """Base schema for partner fields."""
    
    name: str = Field(..., min_length=1, max_length=256, description="Partner name")
    
    # Contact information
    email: EmailStr | None = Field(default=None, description="Email address")
    phone: str | None = Field(default=None, max_length=32, description="Phone number")
    mobile: str | None = Field(default=None, max_length=32, description="Mobile number")
    website: str | None = Field(default=None, max_length=256, description="Website URL")
    
    # Address
    street: str | None = Field(default=None, max_length=256, description="Street address")
    street2: str | None = Field(default=None, max_length=256, description="Street address line 2")
    city: str | None = Field(default=None, max_length=128, description="City")
    zip: str | None = Field(default=None, max_length=24, description="ZIP/Postal code")
    state_id: int | str | None = Field(default=None, description="State/Province (ID or name)")
    country_id: int | str | None = Field(default=None, description="Country (ID or name)")
    
    # Classification
    is_company: bool = Field(default=False, description="Is a company (vs individual)")
    company_type: str | None = Field(default=None, description="'company' or 'person'")
    customer_rank: int = Field(default=0, ge=0, description="Customer rank (0 = not customer)")
    supplier_rank: int = Field(default=0, ge=0, description="Supplier rank (0 = not supplier)")
    
    # Tax information
    vat: str | None = Field(default=None, max_length=32, description="Tax ID / VAT number")
    
    # Reference
    ref: str | None = Field(default=None, max_length=64, description="Internal reference")
    
    # Parent company (for contacts)
    parent_id: int | str | None = Field(default=None, description="Parent company (ID or name)")
    
    # Notes
    comment: str | None = Field(default=None, description="Internal notes")
    
    # Active status
    active: bool = Field(default=True, description="Active status")
    
    @field_validator("email", mode="before")
    @classmethod
    def validate_email(cls, v: Any) -> Any:
        """Allow empty strings as None."""
        if v == "" or v is None:
            return None
        return v
    
    @field_validator("name", mode="before")
    @classmethod
    def validate_name(cls, v: Any) -> str:
        """Ensure name is stripped and not empty."""
        if v is None:
            raise ValueError("Name is required")
        name = str(v).strip()
        if not name:
            raise ValueError("Name cannot be empty")
        return name
    
    @model_validator(mode="after")
    def set_company_type(self) -> "PartnerBaseSchema":
        """Set company_type based on is_company if not provided."""
        if self.company_type is None:
            self.company_type = "company" if self.is_company else "person"
        return self

    model_config = {
        "extra": "allow",  # Allow extra fields for flexibility
    }


class PartnerSchema(PartnerBaseSchema):
    """Full partner schema including read-only fields."""
    
    id: int | None = Field(default=None, description="Odoo record ID")
    display_name: str | None = Field(default=None, description="Full display name")
    create_date: str | None = Field(default=None, description="Creation date")


class PartnerCreateSchema(PartnerBaseSchema):
    """Schema for creating new partners."""
    
    # Additional fields commonly used in creation
    lang: str | None = Field(default=None, description="Language code")
    tz: str | None = Field(default=None, description="Timezone")
    user_id: int | str | None = Field(default=None, description="Salesperson (ID or login)")
    team_id: int | str | None = Field(default=None, description="Sales team (ID or name)")
    category_id: list[int] | None = Field(default=None, description="Partner tags/categories")
    title: int | str | None = Field(default=None, description="Title (ID or name)")
    
    def to_odoo_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary suitable for Odoo create/write.
        
        Removes None values and internal fields.
        """
        data = self.model_dump(exclude_none=True)
        
        # Remove fields that shouldn't be sent to Odoo
        data.pop("company_type", None)  # Computed field
        
        return data


class PartnerImportRow(BaseModel):
    """
    Schema for validating raw import data before mapping.
    
    This is a flexible schema that accepts common column names
    and maps them to Odoo field names.
    """
    
    # Common alternative column names
    customer_name: str | None = Field(default=None, alias="Customer Name")
    company_name: str | None = Field(default=None, alias="Company Name")
    contact_name: str | None = Field(default=None, alias="Contact Name")
    
    email_address: str | None = Field(default=None, alias="Email Address")
    phone_number: str | None = Field(default=None, alias="Phone Number")
    mobile_number: str | None = Field(default=None, alias="Mobile Number")
    
    address: str | None = Field(default=None, alias="Address")
    city_name: str | None = Field(default=None, alias="City")
    state_name: str | None = Field(default=None, alias="State")
    country_name: str | None = Field(default=None, alias="Country")
    postal_code: str | None = Field(default=None, alias="Postal Code")
    
    is_customer: bool | str | None = Field(default=None, alias="Is Customer")
    is_vendor: bool | str | None = Field(default=None, alias="Is Vendor")
    
    model_config = {
        "extra": "allow",
        "populate_by_name": True,
    }
