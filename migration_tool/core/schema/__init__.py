"""
Schema Introspection Module

Dynamic Odoo schema discovery and classification via XML-RPC.
"""

from migration_tool.core.schema.models import (
    FieldMeta,
    ModelMeta,
    FieldType,
    FieldClassification,
)
from migration_tool.core.schema.inspector import SchemaInspector
from migration_tool.core.schema.classifier import FieldClassifier
from migration_tool.core.schema.cache import SchemaCache

__all__ = [
    "FieldMeta",
    "ModelMeta",
    "FieldType",
    "FieldClassification",
    "SchemaInspector",
    "FieldClassifier",
    "SchemaCache",
]
