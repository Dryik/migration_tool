"""
Model Adapters for Odoo

Provides model-specific logic for handling Odoo model nuances,
including Many2one field resolution, default values, and validation.
"""

from abc import ABC, abstractmethod
from typing import Any
from dataclasses import dataclass, field

from migration_tool.odoo.client import OdooClient


@dataclass
class ReferenceCache:
    """Cache for resolved references to avoid repeated API calls."""
    
    _cache: dict[str, dict[str, int]] = field(default_factory=dict)
    
    def get(self, model: str, key: str) -> int | None:
        """Get cached reference ID."""
        return self._cache.get(model, {}).get(key.lower() if isinstance(key, str) else str(key))
    
    def set(self, model: str, key: str, record_id: int) -> None:
        """Cache a reference ID."""
        if model not in self._cache:
            self._cache[model] = {}
        self._cache[model][key.lower() if isinstance(key, str) else str(key)] = record_id
    
    def clear(self, model: str | None = None) -> None:
        """Clear cache for a model or all models."""
        if model:
            self._cache.pop(model, None)
        else:
            self._cache.clear()


class BaseAdapter(ABC):
    """
    Base class for Odoo model adapters.
    
    Adapters handle model-specific logic like:
    - Many2one field resolution
    - Default value population
    - Field name mapping
    - Pre/post processing
    """
    
    # Override in subclasses
    MODEL_NAME: str = ""
    REQUIRED_FIELDS: list[str] = []
    REFERENCE_FIELDS: dict[str, tuple[str, str]] = {}  # field -> (model, search_field)
    
    def __init__(self, client: OdooClient, cache: ReferenceCache | None = None):
        """
        Initialize adapter.
        
        Args:
            client: Odoo client for API calls
            cache: Optional reference cache for performance
        """
        self.client = client
        self.cache = cache or ReferenceCache()
    
    @abstractmethod
    def prepare_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Prepare a record for import into Odoo.
        
        Args:
            record: Raw record data
            
        Returns:
            Prepared record with resolved references
        """
        pass
    
    def resolve_references(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Resolve all Many2one references in a record.
        
        Args:
            record: Record with string references
            
        Returns:
            Record with resolved integer IDs
        """
        resolved = record.copy()
        
        for field_name, (ref_model, search_field) in self.REFERENCE_FIELDS.items():
            if field_name in resolved and resolved[field_name]:
                value = resolved[field_name]
                
                # Skip if already an integer ID
                if isinstance(value, int):
                    continue
                
                # Check cache first
                cached_id = self.cache.get(ref_model, str(value))
                if cached_id:
                    resolved[field_name] = cached_id
                    continue
                
                # Resolve via API
                ref_id = self.client.resolve_reference(
                    ref_model, value, search_field
                )
                
                if ref_id:
                    self.cache.set(ref_model, str(value), ref_id)
                    resolved[field_name] = ref_id
                else:
                    # Set to False (Odoo's null for Many2one) if not found
                    resolved[field_name] = False
        
        return resolved
    
    def validate_required(self, record: dict[str, Any]) -> list[str]:
        """
        Check for missing required fields.
        
        Returns:
            List of missing field names
        """
        missing = []
        for field_name in self.REQUIRED_FIELDS:
            value = record.get(field_name)
            if value is None or value == "" or value is False:
                missing.append(field_name)
        return missing
    
    def apply_defaults(
        self,
        record: dict[str, Any],
        defaults: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Apply default values for missing fields.
        
        Args:
            record: Record data
            defaults: Default values to apply
            
        Returns:
            Record with defaults applied
        """
        result = record.copy()
        for field_name, default_value in defaults.items():
            if field_name not in result or result[field_name] is None:
                result[field_name] = default_value
        return result
    
    def clean_empty_values(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Remove empty string values and convert to appropriate Odoo values.
        
        Odoo has specific requirements:
        - Many2one: False instead of empty string
        - Char: Empty string is fine
        - Integer/Float: 0 instead of empty string
        """
        cleaned = {}
        for key, value in record.items():
            if value == "":
                continue  # Skip empty strings
            if value is None:
                continue  # Skip None values
            cleaned[key] = value
        return cleaned


class PartnerAdapter(BaseAdapter):
    """Adapter for res.partner model."""
    
    MODEL_NAME = "res.partner"
    REQUIRED_FIELDS = ["name"]
    REFERENCE_FIELDS = {
        "country_id": ("res.country", "name"),
        "state_id": ("res.country.state", "name"),
        "parent_id": ("res.partner", "name"),
        "category_id": ("res.partner.category", "name"),
        "title": ("res.partner.title", "name"),
        "user_id": ("res.users", "login"),
        "company_id": ("res.company", "name"),
    }
    
    def prepare_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Prepare partner record for import."""
        # Clean empty values
        prepared = self.clean_empty_values(record)
        
        # Ensure boolean fields are proper booleans
        for bool_field in ["is_company", "active"]:
            if bool_field in prepared:
                prepared[bool_field] = bool(prepared[bool_field])
        
        # Ensure rank fields are integers
        for rank_field in ["customer_rank", "supplier_rank"]:
            if rank_field in prepared:
                try:
                    prepared[rank_field] = int(prepared[rank_field])
                except (ValueError, TypeError):
                    prepared[rank_field] = 0
        
        # Resolve references
        prepared = self.resolve_references(prepared)
        
        return prepared


class ProductAdapter(BaseAdapter):
    """Adapter for product.template and product.product models."""
    
    MODEL_NAME = "product.template"
    REQUIRED_FIELDS = ["name"]
    REFERENCE_FIELDS = {
        "categ_id": ("product.category", "name"),
        "uom_id": ("uom.uom", "name"),
        "uom_po_id": ("uom.uom", "name"),
        "company_id": ("res.company", "name"),
    }
    
    def prepare_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Prepare product record for import."""
        prepared = self.clean_empty_values(record)
        
        # Get valid fields from Odoo (cached after first call)
        if not hasattr(self, '_valid_fields'):
            try:
                fields_info = self.client.fields_get(self.MODEL_NAME, attributes=["type"])
                self._valid_fields = set(fields_info.keys())
            except Exception:
                self._valid_fields = None
        
        # Set product type defaults
        if "detailed_type" not in prepared:
            prepared["detailed_type"] = "consu"  # Consumable by default
        
        # Ensure boolean fields
        for bool_field in ["active", "sale_ok", "purchase_ok"]:
            if bool_field in prepared:
                prepared[bool_field] = bool(prepared[bool_field])
        
        # Ensure numeric fields
        for num_field in ["list_price", "standard_price", "weight", "volume"]:
            if num_field in prepared:
                try:
                    prepared[num_field] = float(prepared[num_field])
                except (ValueError, TypeError):
                    prepared[num_field] = 0.0
        
        # Resolve references
        prepared = self.resolve_references(prepared)
        
        # Default category if not set (CACHED for performance)
        if "categ_id" not in prepared or not prepared["categ_id"]:
            if not hasattr(self, '_default_categ_id'):
                all_categ = self.client.search(
                    "product.category",
                    [("parent_id", "=", False)],
                    limit=1
                )
                self._default_categ_id = all_categ[0] if all_categ else None
            if self._default_categ_id:
                prepared["categ_id"] = self._default_categ_id
        
        # Default UoM if not set (CACHED for performance)
        if "uom_id" not in prepared or not prepared["uom_id"]:
            if not hasattr(self, '_default_uom_id'):
                units_uom = self.client.search(
                    "uom.uom",
                    [("name", "=", "Units")],
                    limit=1
                )
                if units_uom:
                    self._default_uom_id = units_uom[0]
                else:
                    # Try "Unit(s)" for older versions
                    units_uom = self.client.search(
                        "uom.uom",
                        ["|", ("name", "ilike", "unit"), ("name", "ilike", "unité")],
                        limit=1
                    )
                    self._default_uom_id = units_uom[0] if units_uom else None
            if self._default_uom_id:
                prepared["uom_id"] = self._default_uom_id
        
        # Set uom_po_id to same as uom_id if not specified
        if "uom_po_id" not in prepared or not prepared["uom_po_id"]:
            if "uom_id" in prepared and prepared["uom_id"]:
                prepared["uom_po_id"] = prepared["uom_id"]
        
        # Filter out invalid fields (fields that don't exist on the model)
        if self._valid_fields:
            prepared = {k: v for k, v in prepared.items() if k in self._valid_fields}
        
        return prepared


class CategoryAdapter(BaseAdapter):
    """Adapter for product.category model with hierarchy support."""
    
    MODEL_NAME = "product.category"
    REQUIRED_FIELDS = ["name"]
    REFERENCE_FIELDS = {
        "parent_id": ("product.category", "complete_name"),
    }
    
    def prepare_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Prepare category record for import."""
        prepared = self.clean_empty_values(record)
        
        # Handle hierarchical category paths (e.g., "Parent / Child / Grandchild")
        if "complete_name" in prepared and "name" not in prepared:
            # Extract the leaf name
            parts = prepared["complete_name"].split(" / ")
            prepared["name"] = parts[-1].strip()
            
            # Set parent if hierarchy exists
            if len(parts) > 1:
                parent_path = " / ".join(parts[:-1])
                parent_id = self.client.resolve_reference(
                    "product.category",
                    parent_path,
                    "complete_name"
                )
                if parent_id:
                    prepared["parent_id"] = parent_id
            
            del prepared["complete_name"]
        
        prepared = self.resolve_references(prepared)
        return prepared
    
    def create_with_hierarchy(self, complete_name: str) -> int:
        """
        Create a category and all its parent categories if needed.
        
        Args:
            complete_name: Full path like "Parent / Child / Grandchild"
            
        Returns:
            ID of the leaf category
        """
        parts = [p.strip() for p in complete_name.split(" / ")]
        parent_id: int | None = None
        
        for i, part in enumerate(parts):
            current_path = " / ".join(parts[:i + 1])
            
            # Check if exists
            existing = self.client.search(
                "product.category",
                [("complete_name", "=", current_path)],
                limit=1
            )
            
            if existing:
                parent_id = existing[0]
            else:
                # Create it
                values: dict[str, Any] = {"name": part}
                if parent_id:
                    values["parent_id"] = parent_id
                parent_id = self.client.create("product.category", values)
                self.cache.set("product.category", current_path, parent_id)  # type: ignore
        
        return parent_id  # type: ignore


class UoMAdapter(BaseAdapter):
    """Adapter for uom.uom model."""
    
    MODEL_NAME = "uom.uom"
    REQUIRED_FIELDS = ["name", "category_id", "uom_type"]
    REFERENCE_FIELDS = {
        "category_id": ("uom.category", "name"),
    }
    
    def prepare_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Prepare UoM record for import."""
        prepared = self.clean_empty_values(record)
        
        # Default to reference UoM type
        if "uom_type" not in prepared:
            prepared["uom_type"] = "reference"
        
        # Ensure factor is a float
        for factor_field in ["factor", "factor_inv"]:
            if factor_field in prepared:
                try:
                    prepared[factor_field] = float(prepared[factor_field])
                except (ValueError, TypeError):
                    prepared[factor_field] = 1.0
        
        # Ensure rounding is a float
        if "rounding" in prepared:
            try:
                prepared["rounding"] = float(prepared["rounding"])
            except (ValueError, TypeError):
                prepared["rounding"] = 0.01
        
        prepared = self.resolve_references(prepared)
        return prepared


class AccountAdapter(BaseAdapter):
    """Adapter for account.account model."""
    
    MODEL_NAME = "account.account"
    REQUIRED_FIELDS = ["name", "code"]
    REFERENCE_FIELDS = {
        "company_id": ("res.company", "name"),
        "currency_id": ("res.currency", "name"),
        "group_id": ("account.group", "code_prefix_start"),
    }
    
    # Account type mapping for common types
    ACCOUNT_TYPE_MAP = {
        "asset": "asset_current",
        "liability": "liability_current",
        "equity": "equity",
        "income": "income",
        "expense": "expense",
        "receivable": "asset_receivable",
        "payable": "liability_payable",
        "bank": "asset_cash",
        "cash": "asset_cash",
    }
    
    def prepare_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Prepare account record for import."""
        prepared = self.clean_empty_values(record)
        
        # Normalize account type
        if "account_type" in prepared:
            type_value = str(prepared["account_type"]).lower()
            if type_value in self.ACCOUNT_TYPE_MAP:
                prepared["account_type"] = self.ACCOUNT_TYPE_MAP[type_value]
        
        # Ensure reconcile is boolean
        if "reconcile" in prepared:
            prepared["reconcile"] = bool(prepared["reconcile"])
        
        prepared = self.resolve_references(prepared)
        return prepared


class JournalEntryAdapter(BaseAdapter):
    """
    Adapter for account.move model (journal entries).
    
    ⚠️ THIS ADAPTER SHOULD BE USED WITH EXTREME CAUTION.
    Journal entries affect financial data and should only be used
    for opening balances with proper review.
    """
    
    MODEL_NAME = "account.move"
    REQUIRED_FIELDS = ["journal_id", "date", "line_ids"]
    REFERENCE_FIELDS = {
        "journal_id": ("account.journal", "code"),
        "partner_id": ("res.partner", "name"),
        "currency_id": ("res.currency", "name"),
        "company_id": ("res.company", "name"),
    }
    
    def __init__(self, client: OdooClient, cache: ReferenceCache | None = None):
        super().__init__(client, cache)
        self._safety_confirmed = False
    
    def confirm_safety(self) -> None:
        """
        Explicitly confirm that journal entry import is intentional.
        
        This must be called before prepare_record will work.
        """
        self._safety_confirmed = True
    
    def prepare_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Prepare journal entry record for import."""
        if not self._safety_confirmed:
            raise RuntimeError(
                "Journal entry import requires explicit confirmation. "
                "Call confirm_safety() first. This action affects financial data!"
            )
        
        prepared = self.clean_empty_values(record)
        
        # Set move type for opening balance
        if "move_type" not in prepared:
            prepared["move_type"] = "entry"
        
        # Resolve header-level references
        prepared = self.resolve_references(prepared)
        
        # Process line items
        if "line_ids" in prepared and isinstance(prepared["line_ids"], list):
            processed_lines = []
            for line in prepared["line_ids"]:
                processed_line = self._prepare_move_line(line)
                # Use Odoo's command format: (0, 0, values) for create
                processed_lines.append((0, 0, processed_line))
            prepared["line_ids"] = processed_lines
        
        return prepared
    
    def _prepare_move_line(self, line: dict[str, Any]) -> dict[str, Any]:
        """Prepare a journal entry line."""
        prepared = self.clean_empty_values(line)
        
        # Resolve account
        if "account_id" in prepared and not isinstance(prepared["account_id"], int):
            account_id = self.client.resolve_reference(
                "account.account",
                prepared["account_id"],
                "code"
            )
            prepared["account_id"] = account_id or False
        
        # Resolve partner
        if "partner_id" in prepared and not isinstance(prepared["partner_id"], int):
            partner_id = self.client.resolve_reference(
                "res.partner",
                prepared["partner_id"],
                "name"
            )
            prepared["partner_id"] = partner_id or False
        
        # Ensure debit/credit are floats
        for amount_field in ["debit", "credit"]:
            if amount_field in prepared:
                try:
                    prepared[amount_field] = float(prepared[amount_field])
                except (ValueError, TypeError):
                    prepared[amount_field] = 0.0
        
        return prepared


# Adapter registry for easy lookup
ADAPTER_REGISTRY: dict[str, type[BaseAdapter]] = {
    "res.partner": PartnerAdapter,
    "product.template": ProductAdapter,
    "product.product": ProductAdapter,
    "product.category": CategoryAdapter,
    "uom.uom": UoMAdapter,
    "account.account": AccountAdapter,
    "account.move": JournalEntryAdapter,
}


def get_adapter(model: str, client: OdooClient, cache: ReferenceCache | None = None) -> BaseAdapter:
    """
    Get the appropriate adapter for a model.
    
    Args:
        model: Odoo model name
        client: Odoo client
        cache: Optional reference cache
        
    Returns:
        Adapter instance for the model
        
    Raises:
        ValueError: If no adapter exists for the model
    """
    adapter_class = ADAPTER_REGISTRY.get(model)
    if not adapter_class:
        raise ValueError(f"No adapter registered for model: {model}")
    return adapter_class(client, cache)
