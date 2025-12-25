"""
Schema Data Models

Typed dataclasses for representing Odoo field and model metadata.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class FieldType(Enum):
    """Odoo field types."""
    
    # Simple types - generally importable
    CHAR = "char"
    TEXT = "text"
    HTML = "html"
    INTEGER = "integer"
    FLOAT = "float"
    MONETARY = "monetary"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    SELECTION = "selection"
    
    # Relational types
    MANY2ONE = "many2one"
    ONE2MANY = "one2many"
    MANY2MANY = "many2many"
    
    # Special types
    BINARY = "binary"
    REFERENCE = "reference"
    PROPERTIES = "properties"
    PROPERTIES_DEFINITION = "properties_definition"
    
    # Unknown
    UNKNOWN = "unknown"
    
    @classmethod
    def from_string(cls, type_str: str) -> "FieldType":
        """Convert Odoo type string to enum."""
        try:
            return cls(type_str)
        except ValueError:
            return cls.UNKNOWN


class FieldClassification(Enum):
    """Classification of field import/export capability."""
    
    IMPORTABLE = "importable"      # Can be imported
    EXPORT_ONLY = "export_only"    # Read-only, can export but not import
    IGNORED = "ignored"            # System fields, not for import/export
    RELATIONAL = "relational"      # Requires special handling (many2one)


@dataclass(frozen=True)
class FieldMeta:
    """
    Metadata for a single Odoo field.
    
    This is the core data structure used throughout the migration tool
    for dynamic field discovery and validation.
    """
    
    # Identity
    model: str                                    # e.g., "res.partner"
    name: str                                     # Technical field name
    label: str                                    # Human-readable label
    
    # Type information
    field_type: FieldType                         # Odoo field type
    
    # Constraints
    required: bool = False                        # Required for creation
    readonly: bool = False                        # Read-only field
    
    # Classification
    classification: FieldClassification = FieldClassification.IGNORED
    importable: bool = False                      # Can be imported
    exportable: bool = False                      # Can be exported
    
    # Storage
    stored: bool = True                           # Stored in database
    computed: bool = False                        # Is computed field
    has_inverse: bool = False                     # Computed with inverse (writable)
    related: str | None = None                    # Related field path
    company_dependent: bool = False               # Multi-company field
    
    # Relational
    relation: str | None = None                   # Target model for relational fields
    relation_field: str | None = None             # Inverse field name
    
    # Selection options
    selection: list[tuple[str, str]] | None = None  # [(value, label), ...]
    
    # Additional metadata
    help_text: str | None = None                  # Field help/description
    default: Any = None                           # Default value
    size: int | None = None                       # Max length for char
    digits: tuple[int, int] | None = None         # Precision for float/monetary
    
    # Technical flags
    is_custom: bool = False                       # Custom field (x_ prefix)
    is_system: bool = False                       # System field (id, create_*, write_*)
    
    def __repr__(self) -> str:
        return (
            f"FieldMeta({self.model}.{self.name}, "
            f"type={self.field_type.value}, "
            f"{'importable' if self.importable else 'export_only' if self.exportable else 'ignored'})"
        )
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "model": self.model,
            "name": self.name,
            "label": self.label,
            "type": self.field_type.value,
            "required": self.required,
            "readonly": self.readonly,
            "classification": self.classification.value,
            "importable": self.importable,
            "exportable": self.exportable,
            "stored": self.stored,
            "computed": self.computed,
            "has_inverse": self.has_inverse,
            "related": self.related,
            "company_dependent": self.company_dependent,
            "relation": self.relation,
            "relation_field": self.relation_field,
            "selection": self.selection,
            "help_text": self.help_text,
            "is_custom": self.is_custom,
            "is_system": self.is_system,
        }
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FieldMeta":
        """Create from dictionary."""
        return cls(
            model=data["model"],
            name=data["name"],
            label=data["label"],
            field_type=FieldType.from_string(data["type"]),
            required=data.get("required", False),
            readonly=data.get("readonly", False),
            classification=FieldClassification(data.get("classification", "ignored")),
            importable=data.get("importable", False),
            exportable=data.get("exportable", False),
            stored=data.get("stored", True),
            computed=data.get("computed", False),
            has_inverse=data.get("has_inverse", False),
            related=data.get("related"),
            company_dependent=data.get("company_dependent", False),
            relation=data.get("relation"),
            relation_field=data.get("relation_field"),
            selection=data.get("selection"),
            help_text=data.get("help_text"),
            is_custom=data.get("is_custom", False),
            is_system=data.get("is_system", False),
        )


@dataclass
class ModelMeta:
    """
    Metadata for an Odoo model.
    """
    
    name: str                                     # Technical model name
    label: str                                    # Human-readable name
    fields: dict[str, FieldMeta] = field(default_factory=dict)
    
    # Model classification
    is_transient: bool = False                    # TransientModel (wizard)
    is_abstract: bool = False                     # AbstractModel
    
    # Access information
    can_create: bool = False
    can_read: bool = False
    can_write: bool = False
    can_unlink: bool = False
    
    # Statistics
    record_count: int | None = None
    
    @property
    def importable_fields(self) -> list[FieldMeta]:
        """Get all importable fields."""
        return [f for f in self.fields.values() if f.importable]
    
    @property
    def exportable_fields(self) -> list[FieldMeta]:
        """Get all exportable fields."""
        return [f for f in self.fields.values() if f.exportable]
    
    @property
    def required_fields(self) -> list[FieldMeta]:
        """Get all required fields."""
        return [f for f in self.fields.values() if f.required and f.importable]
    
    @property
    def relational_fields(self) -> list[FieldMeta]:
        """Get all many2one fields that need resolution."""
        return [
            f for f in self.fields.values()
            if f.field_type == FieldType.MANY2ONE and f.importable
        ]
    
    def get_field(self, name: str) -> FieldMeta | None:
        """Get field by name."""
        return self.fields.get(name)
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "label": self.label,
            "is_transient": self.is_transient,
            "is_abstract": self.is_abstract,
            "can_create": self.can_create,
            "can_read": self.can_read,
            "can_write": self.can_write,
            "can_unlink": self.can_unlink,
            "fields": {name: f.to_dict() for name, f in self.fields.items()},
        }
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ModelMeta":
        """Create from dictionary."""
        model = cls(
            name=data["name"],
            label=data["label"],
            is_transient=data.get("is_transient", False),
            is_abstract=data.get("is_abstract", False),
            can_create=data.get("can_create", False),
            can_read=data.get("can_read", False),
            can_write=data.get("can_write", False),
            can_unlink=data.get("can_unlink", False),
        )
        
        for name, field_data in data.get("fields", {}).items():
            model.fields[name] = FieldMeta.from_dict(field_data)
        
        return model
