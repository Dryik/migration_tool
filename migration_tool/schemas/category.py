"""
Pydantic Schemas for product.category

Validation schemas for product category data with hierarchy support.
"""

from typing import Any
from pydantic import BaseModel, Field, field_validator


class CategoryBaseSchema(BaseModel):
    """Base schema for product category fields."""
    
    name: str = Field(..., min_length=1, max_length=64, description="Category name")
    
    # Hierarchy
    parent_id: int | str | None = Field(
        default=None, description="Parent category (ID, name, or complete_name)"
    )
    
    # Full path
    complete_name: str | None = Field(
        default=None,
        description="Full category path (e.g., 'All / Electronics / Phones')"
    )
    
    @field_validator("name", mode="before")
    @classmethod
    def validate_name(cls, v: Any) -> str:
        if v is None:
            raise ValueError("Category name is required")
        name = str(v).strip()
        if not name:
            raise ValueError("Category name cannot be empty")
        return name

    model_config = {
        "extra": "allow",
    }


class CategorySchema(CategoryBaseSchema):
    """Full category schema including read-only fields."""
    
    id: int | None = Field(default=None, description="Odoo record ID")
    child_id: list[int] | None = Field(default=None, description="Child category IDs")
    product_count: int | None = Field(default=None, description="Number of products")
    parent_path: str | None = Field(default=None, description="Parent path for hierarchy")


class CategoryCreateSchema(CategoryBaseSchema):
    """Schema for creating new categories."""
    
    def to_odoo_dict(self) -> dict[str, Any]:
        """Convert to dictionary for Odoo create/write."""
        data = self.model_dump(exclude_none=True)
        # Remove complete_name as it's computed
        data.pop("complete_name", None)
        return data
    
    @classmethod
    def from_complete_name(cls, complete_name: str) -> "CategoryCreateSchema":
        """
        Create a CategoryCreateSchema from a complete_name path.
        
        Example:
            >>> cat = CategoryCreateSchema.from_complete_name("All / Electronics / Phones")
            >>> cat.name
            'Phones'
        """
        parts = [p.strip() for p in complete_name.split(" / ")]
        name = parts[-1]
        parent_path = " / ".join(parts[:-1]) if len(parts) > 1 else None
        
        return cls(
            name=name,
            parent_id=parent_path,
            complete_name=complete_name,
        )


class CategoryImportRow(BaseModel):
    """Schema for validating raw category import data."""
    
    # Common column name aliases
    category_name: str | None = Field(default=None, alias="Category Name")
    category_path: str | None = Field(default=None, alias="Category Path")
    parent_category: str | None = Field(default=None, alias="Parent Category")
    full_path: str | None = Field(default=None, alias="Full Path")

    model_config = {
        "extra": "allow",
        "populate_by_name": True,
    }
