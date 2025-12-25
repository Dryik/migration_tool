"""
Configuration management for the migration tool.
"""

from migration_tool.config.loader import (
    ConfigLoader,
    MigrationConfig,
    OdooConfig,
    ModelConfig,
    DedupeConfig,
    ImportConfig,
)

__all__ = [
    "ConfigLoader",
    "MigrationConfig",
    "OdooConfig",
    "ModelConfig",
    "DedupeConfig",
    "ImportConfig",
]
