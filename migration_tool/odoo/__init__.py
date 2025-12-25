"""
Odoo API client and model adapters.
"""

from migration_tool.odoo.client import OdooClient, OdooConnectionError, OdooAPIError
from migration_tool.odoo.adapters import (
    BaseAdapter,
    PartnerAdapter,
    ProductAdapter,
    CategoryAdapter,
    UoMAdapter,
    AccountAdapter,
)

__all__ = [
    "OdooClient",
    "OdooConnectionError",
    "OdooAPIError",
    "BaseAdapter",
    "PartnerAdapter",
    "ProductAdapter",
    "CategoryAdapter",
    "UoMAdapter",
    "AccountAdapter",
]
