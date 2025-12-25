"""
Schema Inspector

Dynamic Odoo schema discovery via XML-RPC.
Pulls field definitions directly from Odoo without hardcoding.
"""

from typing import Any
from dataclasses import dataclass

from migration_tool.core.schema.models import FieldMeta, ModelMeta, FieldType
from migration_tool.core.schema.classifier import FieldClassifier
from migration_tool.core.schema.cache import SchemaCache


# Common models for data migration
COMMON_IMPORT_MODELS = [
    "res.partner",
    "res.partner.category",
    "res.country",
    "res.country.state",
    "res.currency",
    "res.company",
    "res.users",
    "product.template",
    "product.product",
    "product.category",
    "uom.uom",
    "uom.category",
    "account.account",
    "account.journal",
    "account.move",
    "account.move.line",
    "account.tax",
    "stock.warehouse",
    "stock.location",
    "sale.order",
    "sale.order.line",
    "purchase.order",
    "purchase.order.line",
]

# Fields to request from fields_get
FIELD_ATTRIBUTES = [
    "string",
    "type",
    "required",
    "readonly",
    "relation",
    "relation_field",
    "store",
    "compute",
    "inverse",
    "related",
    "company_dependent",
    "help",
    "size",
    "digits",
    "selection",
    "domain",
    "context",
]


class SchemaInspector:
    """
    Discovers and classifies Odoo model schemas dynamically.
    
    Uses only Odoo's XML-RPC API (no direct DB access).
    Caches results for performance.
    
    Example:
        >>> from migration_tool.odoo import OdooClient
        >>> client = OdooClient(url, db, user, password)
        >>> client.authenticate()
        >>> inspector = SchemaInspector(client)
        >>> 
        >>> # Get all available models
        >>> models = inspector.get_models()
        >>> 
        >>> # Get fields for a model
        >>> fields = inspector.get_fields("res.partner")
        >>> 
        >>> # Get only importable fields
        >>> importable = inspector.get_importable_fields("res.partner")
    """
    
    def __init__(
        self,
        client: Any,  # OdooClient
        cache: SchemaCache | None = None,
        classifier: FieldClassifier | None = None,
        auto_cache: bool = True,
    ):
        """
        Initialize schema inspector.
        
        Args:
            client: Authenticated OdooClient
            cache: Optional SchemaCache instance
            classifier: Optional FieldClassifier instance
            auto_cache: Whether to automatically cache results
        """
        self.client = client
        self.cache = cache or SchemaCache()
        self.classifier = classifier or FieldClassifier()
        self.auto_cache = auto_cache
        
        # In-memory schema storage
        self._models: dict[str, ModelMeta] = {}
        self._odoo_version: str | None = None
        self._server_version_info: tuple[int, ...] | None = None
        self._installed_modules: list[tuple[str, str]] | None = None
    
    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------
    
    def get_models(self, refresh: bool = False) -> list[str]:
        """
        Get list of all available model names.
        
        Args:
            refresh: Force refresh from Odoo
            
        Returns:
            List of model technical names
        """
        if self._models and not refresh:
            return list(self._models.keys())
        
        # Query ir.model for all models
        models = self.client.search_read(
            "ir.model",
            [("transient", "=", False)],
            ["model", "name"],
            order="model",
        )
        
        return [m["model"] for m in models]
    
    def get_model(self, model: str, refresh: bool = False) -> ModelMeta | None:
        """
        Get full metadata for a model.
        
        Args:
            model: Model technical name (e.g., "res.partner")
            refresh: Force refresh from Odoo
            
        Returns:
            ModelMeta or None if model doesn't exist
        """
        if model in self._models and not refresh:
            return self._models[model]
        
        # Try loading from cache
        if not refresh and self._try_load_from_cache(model):
            return self._models.get(model)
        
        # Fetch from Odoo
        model_meta = self._fetch_model_schema(model)
        if model_meta:
            self._models[model] = model_meta
            
            if self.auto_cache:
                self._save_to_cache()
        
        return model_meta
    
    def get_fields(self, model: str, refresh: bool = False) -> list[FieldMeta]:
        """
        Get all fields for a model.
        
        Args:
            model: Model technical name
            refresh: Force refresh from Odoo
            
        Returns:
            List of FieldMeta for all fields
        """
        model_meta = self.get_model(model, refresh)
        if not model_meta:
            return []
        return list(model_meta.fields.values())
    
    def get_importable_fields(self, model: str, refresh: bool = False) -> list[FieldMeta]:
        """
        Get only importable fields for a model.
        
        Args:
            model: Model technical name
            refresh: Force refresh from Odoo
            
        Returns:
            List of importable FieldMeta
        """
        model_meta = self.get_model(model, refresh)
        if not model_meta:
            return []
        return model_meta.importable_fields
    
    def get_exportable_fields(self, model: str, refresh: bool = False) -> list[FieldMeta]:
        """
        Get only exportable fields for a model.
        
        Args:
            model: Model technical name
            refresh: Force refresh from Odoo
            
        Returns:
            List of exportable FieldMeta
        """
        model_meta = self.get_model(model, refresh)
        if not model_meta:
            return []
        return model_meta.exportable_fields
    
    def get_required_fields(self, model: str, refresh: bool = False) -> list[FieldMeta]:
        """
        Get required importable fields for a model.
        
        Args:
            model: Model technical name
            refresh: Force refresh from Odoo
            
        Returns:
            List of required FieldMeta
        """
        model_meta = self.get_model(model, refresh)
        if not model_meta:
            return []
        return model_meta.required_fields
    
    def get_relational_fields(self, model: str, refresh: bool = False) -> list[FieldMeta]:
        """
        Get many2one fields that need resolution.
        
        Args:
            model: Model technical name
            refresh: Force refresh from Odoo
            
        Returns:
            List of relational FieldMeta
        """
        model_meta = self.get_model(model, refresh)
        if not model_meta:
            return []
        return model_meta.relational_fields
    
    def get_field(self, model: str, field_name: str) -> FieldMeta | None:
        """
        Get a specific field by name.
        
        Args:
            model: Model technical name
            field_name: Field name
            
        Returns:
            FieldMeta or None
        """
        model_meta = self.get_model(model)
        if not model_meta:
            return None
        return model_meta.get_field(field_name)
    
    def refresh_schema(self, models: list[str] | None = None) -> None:
        """
        Force refresh schema from Odoo.
        
        Args:
            models: Specific models to refresh, or None for all cached
        """
        target_models = models or list(self._models.keys())
        
        for model in target_models:
            model_meta = self._fetch_model_schema(model)
            if model_meta:
                self._models[model] = model_meta
        
        if self.auto_cache:
            self._save_to_cache()
    
    def preload_common_models(self) -> None:
        """Preload schemas for commonly imported models."""
        for model in COMMON_IMPORT_MODELS:
            try:
                self.get_model(model)
            except Exception:
                pass  # Model may not exist in this Odoo instance
    
    # -------------------------------------------------------------------------
    # Internal Methods
    # -------------------------------------------------------------------------
    
    def _get_odoo_version(self) -> str:
        """Get Odoo version string."""
        if self._odoo_version is None:
            version_info = self.client.version()
            self._odoo_version = version_info.get("server_version", "unknown")
            
            # Parse version info tuple
            version_info_list = version_info.get("server_version_info", [])
            if version_info_list:
                self._server_version_info = tuple(version_info_list)
        
        return self._odoo_version
    
    def _get_installed_modules(self) -> list[tuple[str, str]]:
        """Get list of installed modules with their states."""
        if self._installed_modules is None:
            try:
                modules = self.client.search_read(
                    "ir.module.module",
                    [("state", "=", "installed")],
                    ["name", "installed_version"],
                    order="name",
                )
                self._installed_modules = [
                    (m["name"], m.get("installed_version", ""))
                    for m in modules
                ]
            except Exception:
                # Fallback if ir.module.module not accessible
                self._installed_modules = []
        
        return self._installed_modules
    
    def _fetch_model_schema(self, model: str) -> ModelMeta | None:
        """Fetch model schema from Odoo."""
        try:
            # Get model info
            model_info = self.client.search_read(
                "ir.model",
                [("model", "=", model)],
                ["name", "model", "transient"],
                limit=1,
            )
            
            if not model_info:
                return None
            
            model_data = model_info[0]
            
            # Get field definitions
            fields_data = self.client.fields_get(
                model,
                attributes=FIELD_ATTRIBUTES,
            )
            
            # Check access rights
            can_create = self._check_access(model, "create")
            can_read = self._check_access(model, "read")
            can_write = self._check_access(model, "write")
            can_unlink = self._check_access(model, "unlink")
            
            # Create ModelMeta
            model_meta = ModelMeta(
                name=model,
                label=model_data.get("name", model),
                is_transient=model_data.get("transient", False),
                can_create=can_create,
                can_read=can_read,
                can_write=can_write,
                can_unlink=can_unlink,
            )
            
            # Classify and add fields
            for field_name, field_info in fields_data.items():
                field_meta = self.classifier.classify(
                    model=model,
                    name=field_name,
                    label=field_info.get("string", field_name),
                    field_info=field_info,
                )
                model_meta.fields[field_name] = field_meta
            
            # Enrich with ir.model.fields if accessible
            self._enrich_from_ir_model_fields(model, model_meta)
            
            return model_meta
            
        except Exception as e:
            # Model might not exist or not accessible
            return None
    
    def _enrich_from_ir_model_fields(
        self,
        model: str,
        model_meta: ModelMeta,
    ) -> None:
        """Enrich field metadata from ir.model.fields."""
        try:
            ir_fields = self.client.search_read(
                "ir.model.fields",
                [("model", "=", model)],
                ["name", "ttype", "state", "store", "compute"],
            )
            
            for ir_field in ir_fields:
                field_name = ir_field["name"]
                if field_name in model_meta.fields:
                    field = model_meta.fields[field_name]
                    
                    # Update is_custom based on state
                    if ir_field.get("state") == "manual":
                        # Create new FieldMeta with is_custom=True
                        model_meta.fields[field_name] = FieldMeta(
                            model=field.model,
                            name=field.name,
                            label=field.label,
                            field_type=field.field_type,
                            required=field.required,
                            readonly=field.readonly,
                            classification=field.classification,
                            importable=field.importable,
                            exportable=field.exportable,
                            stored=field.stored,
                            computed=field.computed,
                            has_inverse=field.has_inverse,
                            related=field.related,
                            company_dependent=field.company_dependent,
                            relation=field.relation,
                            relation_field=field.relation_field,
                            selection=field.selection,
                            help_text=field.help_text,
                            is_custom=True,  # Mark as custom
                            is_system=field.is_system,
                        )
        except Exception:
            pass  # ir.model.fields may not be accessible
    
    def _check_access(self, model: str, operation: str) -> bool:
        """Check if user has access for an operation."""
        try:
            return self.client.check_access_rights(model, operation, raise_exception=False)
        except Exception:
            return False
    
    def _try_load_from_cache(self, model: str) -> bool:
        """Try to load model from cache."""
        try:
            version = self._get_odoo_version()
            modules = self._get_installed_modules()
            
            cached_models = self.cache.load(
                database=self.client.db,
                odoo_version=version,
                modules=modules,
            )
            
            if cached_models and model in cached_models:
                model_meta = ModelMeta.from_dict(cached_models[model])
                self._models[model] = model_meta
                return True
            
            return False
            
        except Exception:
            return False
    
    def _save_to_cache(self) -> None:
        """Save current models to cache."""
        try:
            version = self._get_odoo_version()
            modules = self._get_installed_modules()
            
            models_dict = {
                name: model.to_dict()
                for name, model in self._models.items()
            }
            
            self.cache.save(
                database=self.client.db,
                odoo_version=version,
                modules=modules,
                models=models_dict,
                server_version_info=self._server_version_info,
            )
        except Exception:
            pass  # Caching failure shouldn't break functionality
    
    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------
    
    def get_field_mapping_suggestions(
        self,
        model: str,
        source_columns: list[str],
    ) -> dict[str, str | None]:
        """
        Suggest field mappings based on column names.
        
        Args:
            model: Target Odoo model
            source_columns: List of source column names
            
        Returns:
            Dict of source_column -> suggested_field_name (or None)
        """
        model_meta = self.get_model(model)
        if not model_meta:
            return {col: None for col in source_columns}
        
        importable = model_meta.importable_fields
        field_names = {f.name.lower(): f.name for f in importable}
        field_labels = {f.label.lower(): f.name for f in importable}
        
        suggestions: dict[str, str | None] = {}
        
        for col in source_columns:
            col_lower = col.lower().strip()
            
            # Exact name match
            if col_lower in field_names:
                suggestions[col] = field_names[col_lower]
            # Exact label match
            elif col_lower in field_labels:
                suggestions[col] = field_labels[col_lower]
            # Partial match in name
            elif any(col_lower in name for name in field_names):
                for name in field_names:
                    if col_lower in name:
                        suggestions[col] = field_names[name]
                        break
            # Partial match in label
            elif any(col_lower in label for label in field_labels):
                for label in field_labels:
                    if col_lower in label:
                        suggestions[col] = field_labels[label]
                        break
            else:
                suggestions[col] = None
        
        return suggestions
    
    def validate_mapping(
        self,
        model: str,
        mapping: dict[str, str],
    ) -> tuple[list[str], list[str]]:
        """
        Validate a column-to-field mapping.
        
        Args:
            model: Target model
            mapping: source_column -> field_name mapping
            
        Returns:
            Tuple of (valid_fields, invalid_fields)
        """
        model_meta = self.get_model(model)
        if not model_meta:
            return [], list(mapping.values())
        
        valid = []
        invalid = []
        
        for source, field_name in mapping.items():
            field = model_meta.get_field(field_name)
            if field and field.importable:
                valid.append(field_name)
            else:
                invalid.append(field_name)
        
        return valid, invalid
    
    def get_missing_required_fields(
        self,
        model: str,
        mapped_fields: list[str],
    ) -> list[FieldMeta]:
        """
        Find required fields that are not in the mapping.
        
        Args:
            model: Target model
            mapped_fields: List of mapped field names
            
        Returns:
            List of missing required FieldMeta
        """
        model_meta = self.get_model(model)
        if not model_meta:
            return []
        
        mapped_set = set(mapped_fields)
        return [
            f for f in model_meta.required_fields
            if f.name not in mapped_set
        ]
