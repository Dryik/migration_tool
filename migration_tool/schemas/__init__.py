"""
Pydantic schemas for Odoo model validation.
"""

from migration_tool.schemas.partner import PartnerSchema, PartnerCreateSchema
from migration_tool.schemas.product import (
    ProductTemplateSchema,
    ProductProductSchema,
    ProductCreateSchema,
)
from migration_tool.schemas.category import CategorySchema, CategoryCreateSchema
from migration_tool.schemas.uom import UoMSchema, UoMCreateSchema
from migration_tool.schemas.account import (
    AccountSchema,
    AccountCreateSchema,
    JournalEntrySchema,
    JournalEntryLineSchema,
)

__all__ = [
    "PartnerSchema",
    "PartnerCreateSchema",
    "ProductTemplateSchema",
    "ProductProductSchema",
    "ProductCreateSchema",
    "CategorySchema",
    "CategoryCreateSchema",
    "UoMSchema",
    "UoMCreateSchema",
    "AccountSchema",
    "AccountCreateSchema",
    "JournalEntrySchema",
    "JournalEntryLineSchema",
]
