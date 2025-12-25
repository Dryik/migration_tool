"""
Multi-Model Import Orchestrator

Handles sequential imports across multiple related models with reference linking.
"""

from dataclasses import dataclass, field
from typing import Optional, Callable
from pathlib import Path

from migration_tool.odoo import OdooClient
from migration_tool.core.reader import DataReader
from migration_tool.core.cleaner import DataCleaner
from migration_tool.odoo.adapters import get_adapter, ReferenceCache


@dataclass
class ModelImportConfig:
    """Configuration for importing a single model."""
    model: str
    file_path: Path
    mappings: dict[str, str]  # {file_column: odoo_field}
    defaults: dict[str, any] = field(default_factory=dict)
    parent_model: Optional[str] = None
    link_field: Optional[str] = None  # Field to link to parent (e.g., "parent_id")
    link_source_column: Optional[str] = None  # Column in file that references parent


@dataclass 
class ModelImportResult:
    """Result of importing a single model."""
    model: str
    total_records: int = 0
    created_count: int = 0
    failed_count: int = 0
    created_ids: list[int] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    
    @property
    def success(self) -> bool:
        return self.failed_count == 0


@dataclass
class MultiImportResult:
    """Result of a multi-model import operation."""
    model_results: dict[str, ModelImportResult] = field(default_factory=dict)
    
    @property
    def total_created(self) -> int:
        return sum(r.created_count for r in self.model_results.values())
    
    @property
    def total_failed(self) -> int:
        return sum(r.failed_count for r in self.model_results.values())
    
    @property
    def all_created_ids(self) -> dict[str, list[int]]:
        return {model: r.created_ids for model, r in self.model_results.items()}
    
    @property
    def success(self) -> bool:
        return all(r.success for r in self.model_results.values())


class MultiModelImporter:
    """
    Orchestrates imports across multiple related Odoo models.
    
    Handles:
    - Model dependency ordering
    - Reference linking between parent/child records
    - Rollback of all created records on failure
    """
    
    def __init__(
        self,
        client: OdooClient,
        on_progress: Callable[[str, int, int], None] = None,
    ):
        self.client = client
        self.on_progress = on_progress  # callback(model, current, total)
        self.configs: list[ModelImportConfig] = []
        self.reference_cache = ReferenceCache()
        self._parent_id_map: dict[str, dict[str, int]] = {}  # {model: {ref_value: odoo_id}}
    
    def add_model(
        self,
        model: str,
        file_path: Path,
        mappings: dict[str, str],
        defaults: dict[str, any] = None,
    ) -> "MultiModelImporter":
        """Add a model to the import queue."""
        config = ModelImportConfig(
            model=model,
            file_path=file_path,
            mappings=mappings,
            defaults=defaults or {},
        )
        self.configs.append(config)
        return self
    
    def set_dependency(
        self,
        child_model: str,
        parent_model: str,
        link_field: str,
        link_source_column: str,
    ) -> "MultiModelImporter":
        """
        Set a dependency between models.
        
        Args:
            child_model: The model that references the parent
            parent_model: The model being referenced
            link_field: Odoo field in child that links to parent (e.g., "parent_id")
            link_source_column: Column in child's file that contains parent reference
        """
        for config in self.configs:
            if config.model == child_model:
                config.parent_model = parent_model
                config.link_field = link_field
                config.link_source_column = link_source_column
                break
        return self
    
    def _get_import_order(self) -> list[ModelImportConfig]:
        """Order configs so parents are imported before children."""
        ordered = []
        remaining = list(self.configs)
        imported_models = set()
        
        while remaining:
            # Find configs with no unmet dependencies
            for config in remaining[:]:
                if config.parent_model is None or config.parent_model in imported_models:
                    ordered.append(config)
                    imported_models.add(config.model)
                    remaining.remove(config)
                    break
            else:
                # Circular dependency or missing parent - add remaining as-is
                ordered.extend(remaining)
                break
        
        return ordered
    
    def execute(self, batch_size: int = 50) -> MultiImportResult:
        """Execute the multi-model import."""
        result = MultiImportResult()
        ordered_configs = self._get_import_order()
        
        for config in ordered_configs:
            model_result = self._import_model(config, batch_size)
            result.model_results[config.model] = model_result
            
            # Stop if this model failed and it has dependents
            if not model_result.success:
                has_dependents = any(
                    c.parent_model == config.model for c in ordered_configs
                )
                if has_dependents:
                    break
        
        return result
    
    def _import_model(
        self,
        config: ModelImportConfig,
        batch_size: int,
    ) -> ModelImportResult:
        """Import a single model."""
        result = ModelImportResult(model=config.model)
        
        try:
            # Read and clean data
            reader = DataReader()
            read_result = reader.read_file(config.file_path, mapping=config.mappings)
            
            if read_result.errors:
                result.errors.extend(read_result.errors)
                return result
            
            cleaner = DataCleaner()
            df = cleaner.clean(read_result.data)
            records = df.to_dict("records")
            result.total_records = len(records)
            
            # Apply defaults
            for record in records:
                for field_name, default_value in config.defaults.items():
                    if field_name not in record or record[field_name] is None:
                        record[field_name] = default_value
            
            # Get adapter
            adapter = get_adapter(config.model, self.client, self.reference_cache)
            
            # Process records
            for i, record in enumerate(records):
                try:
                    # Link to parent if configured
                    if config.link_field and config.parent_model:
                        parent_ref = record.get(config.link_source_column)
                        if parent_ref and config.parent_model in self._parent_id_map:
                            parent_id = self._parent_id_map[config.parent_model].get(str(parent_ref))
                            if parent_id:
                                record[config.link_field] = parent_id
                    
                    # Prepare and create
                    prepared = adapter.prepare_record(record)
                    if prepared:
                        created_id = self.client.create(config.model, prepared)
                        if created_id:
                            result.created_ids.append(created_id)
                            result.created_count += 1
                            
                            # Store for child linking (use first unique field as key)
                            if config.model not in self._parent_id_map:
                                self._parent_id_map[config.model] = {}
                            # Use name or first mapped field as reference
                            ref_value = record.get("name") or record.get(list(config.mappings.keys())[0])
                            if ref_value:
                                self._parent_id_map[config.model][str(ref_value)] = created_id
                        else:
                            result.failed_count += 1
                    else:
                        result.failed_count += 1
                        
                except Exception as e:
                    result.failed_count += 1
                    result.errors.append(f"Row {i+2}: {str(e)[:100]}")
                
                # Progress callback
                if self.on_progress and (i + 1) % 10 == 0:
                    self.on_progress(config.model, i + 1, result.total_records)
            
            # Final progress
            if self.on_progress:
                self.on_progress(config.model, result.total_records, result.total_records)
                
        except Exception as e:
            result.errors.append(f"Import error: {str(e)}")
        
        return result
    
    def rollback(self, result: MultiImportResult) -> dict[str, int]:
        """
        Delete all records created during the import.
        
        Returns dict of {model: deleted_count}
        """
        deleted = {}
        
        # Delete in reverse order (children before parents)
        for model in reversed(list(result.model_results.keys())):
            model_result = result.model_results[model]
            if model_result.created_ids:
                try:
                    self.client.execute(model, "unlink", [model_result.created_ids])
                    deleted[model] = len(model_result.created_ids)
                except Exception:
                    deleted[model] = 0
        
        return deleted
