"""
Core data processing modules.
"""

from migration_tool.core.reader import DataReader, ReadResult
from migration_tool.core.cleaner import DataCleaner
from migration_tool.core.validator import ValidationEngine, ValidationResult, ValidationIssue
from migration_tool.core.deduplicator import Deduplicator, DedupeResult, DedupeAction
from migration_tool.core.batcher import BatchProcessor, BatchResult
from migration_tool.core.logger import MigrationLogger, AuditEntry # Re-added AuditEntry to import as it's in __all__
from migration_tool.core.schema import (
    FieldMeta,
    ModelMeta,
    FieldType,
    FieldClassification,
    SchemaInspector,
    FieldClassifier,
    SchemaCache,
)


__all__ = [
    "DataReader",
    "ReadResult",
    "DataCleaner",
    "ValidationEngine",
    "ValidationResult",
    "ValidationIssue",
    "Deduplicator",
    "DedupeResult",
    "DedupeAction",
    "BatchProcessor",
    "BatchResult",
    "MigrationLogger",
    # Schema introspection
    "FieldMeta",
    "ModelMeta",
    "FieldType",
    "FieldClassification",
    "SchemaInspector",
    "FieldClassifier",
    "SchemaCache",
    "AuditEntry", # Moved AuditEntry to the end to match the instruction's intent
]
