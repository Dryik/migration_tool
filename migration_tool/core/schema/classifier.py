"""
Field Classifier

Deterministic classification logic for Odoo fields.
Determines whether fields are importable, export-only, or ignored.
"""

from migration_tool.core.schema.models import (
    FieldMeta,
    FieldType,
    FieldClassification,
)


# System fields that should always be ignored
SYSTEM_FIELDS = frozenset({
    "id",
    "create_uid",
    "create_date",
    "write_uid",
    "write_date",
    "__last_update",
    "display_name",
    "activity_ids",
    "activity_state",
    "activity_user_id",
    "activity_type_id",
    "activity_date_deadline",
    "activity_summary",
    "activity_exception_decoration",
    "activity_exception_icon",
    "message_ids",
    "message_follower_ids",
    "message_partner_ids",
    "message_channel_ids",
    "message_attachment_count",
    "message_has_error",
    "message_has_error_counter",
    "message_has_sms_error",
    "message_needaction",
    "message_needaction_counter",
    "message_is_follower",
    "message_main_attachment_id",
    "website_message_ids",
    "has_message",
    "rating_ids",
    "rating_last_value",
    "rating_count",
    "rating_avg",
    "rating_last_image",
    "rating_last_feedback",
})

# Field types that are never importable
UNSUPPORTED_IMPORT_TYPES = frozenset({
    FieldType.ONE2MANY,
    FieldType.MANY2MANY,
    FieldType.BINARY,
    FieldType.REFERENCE,
    FieldType.PROPERTIES,
    FieldType.PROPERTIES_DEFINITION,
})

# Field types that are standard importable
SIMPLE_IMPORT_TYPES = frozenset({
    FieldType.CHAR,
    FieldType.TEXT,
    FieldType.HTML,
    FieldType.INTEGER,
    FieldType.FLOAT,
    FieldType.MONETARY,
    FieldType.BOOLEAN,
    FieldType.DATE,
    FieldType.DATETIME,
    FieldType.SELECTION,
})


class FieldClassifier:
    """
    Classifies Odoo fields for import/export compatibility.
    
    Classification Rules:
    
    ✅ IMPORTABLE if:
       - stored == True
       - readonly == False
       - NOT computed (unless has inverse)
       - type in simple types or many2one
    
    📤 EXPORT_ONLY if:
       - stored == True
       - readonly == True OR computed without inverse
    
    🚫 IGNORED if:
       - type in (one2many, many2many, binary, reference)
       - system/technical fields
       - not stored
    """
    
    def __init__(self, strict_mode: bool = True):
        """
        Initialize classifier.
        
        Args:
            strict_mode: If True, be conservative about importability
        """
        self.strict_mode = strict_mode
    
    def classify(
        self,
        model: str,
        name: str,
        label: str,
        field_info: dict,
    ) -> FieldMeta:
        """
        Classify a single field and create FieldMeta.
        
        Args:
            model: Model technical name
            name: Field technical name
            label: Field label/string
            field_info: Raw field_info from Odoo fields_get
            
        Returns:
            Classified FieldMeta
        """
        # Extract basic info
        field_type = FieldType.from_string(field_info.get("type", "unknown"))
        required = field_info.get("required", False)
        readonly = field_info.get("readonly", False)
        stored = field_info.get("store", True)
        
        # Computed field detection
        compute = field_info.get("compute")
        inverse = field_info.get("inverse")
        related = field_info.get("related")
        
        computed = bool(compute or related)
        has_inverse = bool(inverse)
        
        # Company-dependent fields
        company_dependent = field_info.get("company_dependent", False)
        
        # Relational info
        relation = field_info.get("relation")
        relation_field = field_info.get("relation_field")
        
        # Selection options
        selection = field_info.get("selection")
        if selection and not isinstance(selection, list):
            selection = None  # Dynamic selection, can't cache
        
        # Check if system field
        is_system = name in SYSTEM_FIELDS or name.startswith("_")
        
        # Check if custom field
        is_custom = name.startswith("x_")
        
        # Additional metadata
        help_text = field_info.get("help")
        size = field_info.get("size")
        digits = field_info.get("digits")
        
        # ---- Classification Logic ----
        classification, importable, exportable = self._determine_classification(
            name=name,
            field_type=field_type,
            required=required,
            readonly=readonly,
            stored=stored,
            computed=computed,
            has_inverse=has_inverse,
            is_system=is_system,
        )
        
        return FieldMeta(
            model=model,
            name=name,
            label=label,
            field_type=field_type,
            required=required,
            readonly=readonly,
            classification=classification,
            importable=importable,
            exportable=exportable,
            stored=stored,
            computed=computed,
            has_inverse=has_inverse,
            related=related,
            company_dependent=company_dependent,
            relation=relation,
            relation_field=relation_field,
            selection=selection,
            help_text=help_text,
            is_custom=is_custom,
            is_system=is_system,
            size=size,
            digits=tuple(digits) if digits and len(digits) == 2 else None,
        )
    
    def _determine_classification(
        self,
        name: str,
        field_type: FieldType,
        required: bool,
        readonly: bool,
        stored: bool,
        computed: bool,
        has_inverse: bool,
        is_system: bool,
    ) -> tuple[FieldClassification, bool, bool]:
        """
        Determine classification, importable, and exportable flags.
        
        Returns:
            Tuple of (classification, importable, exportable)
        """
        # System fields -> IGNORED
        if is_system:
            return FieldClassification.IGNORED, False, False
        
        # Unsupported types -> IGNORED
        if field_type in UNSUPPORTED_IMPORT_TYPES:
            return FieldClassification.IGNORED, False, stored
        
        # Not stored -> generally not importable
        if not stored:
            return FieldClassification.IGNORED, False, False
        
        # Computed without inverse -> EXPORT_ONLY
        if computed and not has_inverse:
            return FieldClassification.EXPORT_ONLY, False, True
        
        # Readonly -> EXPORT_ONLY (unless has inverse)
        if readonly and not has_inverse:
            return FieldClassification.EXPORT_ONLY, False, True
        
        # Many2one -> RELATIONAL (importable with special handling)
        if field_type == FieldType.MANY2ONE:
            return FieldClassification.RELATIONAL, True, True
        
        # Simple types that pass all checks -> IMPORTABLE
        if field_type in SIMPLE_IMPORT_TYPES:
            return FieldClassification.IMPORTABLE, True, True
        
        # Unknown type -> be conservative
        if field_type == FieldType.UNKNOWN:
            if self.strict_mode:
                return FieldClassification.IGNORED, False, False
            else:
                return FieldClassification.IMPORTABLE, True, True
        
        # Default: exportable but not importable
        return FieldClassification.EXPORT_ONLY, False, True
    
    def is_importable(self, field: FieldMeta) -> bool:
        """Check if a field is importable."""
        return field.importable
    
    def is_exportable(self, field: FieldMeta) -> bool:
        """Check if a field is exportable."""
        return field.exportable
    
    def get_import_fields(self, fields: list[FieldMeta]) -> list[FieldMeta]:
        """Filter to only importable fields."""
        return [f for f in fields if f.importable]
    
    def get_export_fields(self, fields: list[FieldMeta]) -> list[FieldMeta]:
        """Filter to only exportable fields."""
        return [f for f in fields if f.exportable]
    
    def get_required_fields(self, fields: list[FieldMeta]) -> list[FieldMeta]:
        """Get required importable fields."""
        return [f for f in fields if f.required and f.importable]
    
    def get_relational_fields(self, fields: list[FieldMeta]) -> list[FieldMeta]:
        """Get importable many2one fields."""
        return [
            f for f in fields
            if f.field_type == FieldType.MANY2ONE and f.importable
        ]
