"""
Deduplicator

Handles duplicate detection within import batches and against existing Odoo records.
"""

from typing import Any, Literal
from dataclasses import dataclass, field
from enum import Enum

import pandas as pd


class DedupeAction(Enum):
    """Action to take when duplicate is found."""
    SKIP = "skip"         # Don't import this record
    UPDATE = "update"     # Update existing record
    CREATE = "create"     # Create new record anyway


@dataclass
class DuplicateMatch:
    """Represents a duplicate match."""
    
    source_row: int                # Row in import file
    match_type: Literal["odoo", "batch"]  # Where duplicate was found
    odoo_id: int | None = None     # ID in Odoo if existing
    batch_row: int | None = None   # Row in batch if internal duplicate
    matched_keys: dict[str, Any] = field(default_factory=dict)  # Key values that matched
    confidence: float = 1.0        # Match confidence (1.0 = exact)


@dataclass
class DedupeResult:
    """Result of deduplication."""
    
    unique_records: list[dict[str, Any]] = field(default_factory=list)
    duplicate_records: list[dict[str, Any]] = field(default_factory=list)
    matches: list[DuplicateMatch] = field(default_factory=list)
    
    # Records marked for update (with odoo_id attached)
    update_records: list[dict[str, Any]] = field(default_factory=list)
    
    @property
    def total_duplicates(self) -> int:
        return len(self.duplicate_records)
    
    @property
    def odoo_duplicates(self) -> int:
        return sum(1 for m in self.matches if m.match_type == "odoo")
    
    @property
    def batch_duplicates(self) -> int:
        return sum(1 for m in self.matches if m.match_type == "batch")
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert matches to DataFrame for reporting."""
        if not self.matches:
            return pd.DataFrame(columns=[
                "source_row", "match_type", "odoo_id", "batch_row", "matched_keys"
            ])
        
        return pd.DataFrame([
            {
                "source_row": m.source_row,
                "match_type": m.match_type,
                "odoo_id": m.odoo_id or "",
                "batch_row": m.batch_row or "",
                "matched_keys": str(m.matched_keys),
                "confidence": m.confidence,
            }
            for m in self.matches
        ])


class Deduplicator:
    """
    Detects and handles duplicates in import data.
    
    Features:
    - Configurable dedupe keys per model
    - Match against existing Odoo records
    - Match within import batch
    - Multiple strategies: skip, update, create
    - Case-insensitive matching option
    
    Example:
        >>> deduper = Deduplicator(client)
        >>> result = deduper.find_duplicates(
        ...     records,
        ...     model="res.partner",
        ...     keys=["name", "phone"],
        ...     strategy=DedupeAction.UPDATE
        ... )
    """
    
    # Default dedupe keys per model
    DEFAULT_KEYS: dict[str, list[str]] = {
        "res.partner": ["name", "phone"],
        "product.template": ["default_code"],
        "product.product": ["default_code", "barcode"],
        "product.category": ["complete_name"],
        "account.account": ["code"],
        "uom.uom": ["name", "category_id"],
    }
    
    def __init__(self, odoo_client: Any | None = None):
        """
        Initialize deduplicator.
        
        Args:
            odoo_client: Odoo client for checking existing records
        """
        self.client = odoo_client
        self._odoo_cache: dict[str, dict[str, int]] = {}  # model -> {key_hash -> id}
    
    def find_duplicates(
        self,
        records: list[dict[str, Any]],
        model: str,
        keys: list[str] | None = None,
        strategy: DedupeAction = DedupeAction.SKIP,
        case_sensitive: bool = False,
        check_odoo: bool = True,
        check_batch: bool = True,
    ) -> DedupeResult:
        """
        Find duplicates in a list of records.
        
        Args:
            records: Records to check for duplicates
            model: Odoo model name (for default keys and Odoo lookup)
            keys: Fields to use for duplicate detection
            strategy: How to handle duplicates
            case_sensitive: Whether to use case-sensitive matching
            check_odoo: Check against existing Odoo records
            check_batch: Check for duplicates within the batch
            
        Returns:
            DedupeResult with categorized records
        """
        result = DedupeResult()
        dedupe_keys = keys or self.DEFAULT_KEYS.get(model, ["name"])
        
        # Track seen keys within batch
        seen_keys: dict[str, int] = {}  # key_hash -> row number
        
        # Load existing Odoo records if needed
        if check_odoo and self.client:
            self._load_odoo_records(model, dedupe_keys)
        
        for record in records:
            row_num = record.get("__source_row__", 0)
            key_hash = self._make_key_hash(record, dedupe_keys, case_sensitive)
            key_values = {k: record.get(k) for k in dedupe_keys}
            
            is_duplicate = False
            match: DuplicateMatch | None = None
            
            # Check against Odoo first
            if check_odoo and self.client:
                odoo_id = self._check_odoo_duplicate(model, key_hash)
                if odoo_id:
                    is_duplicate = True
                    match = DuplicateMatch(
                        source_row=row_num,
                        match_type="odoo",
                        odoo_id=odoo_id,
                        matched_keys=key_values,
                    )
            
            # Check within batch
            if check_batch and not is_duplicate:
                if key_hash in seen_keys:
                    is_duplicate = True
                    match = DuplicateMatch(
                        source_row=row_num,
                        match_type="batch",
                        batch_row=seen_keys[key_hash],
                        matched_keys=key_values,
                    )
            
            # Handle based on strategy
            if is_duplicate and match:
                result.matches.append(match)
                
                if strategy == DedupeAction.SKIP:
                    result.duplicate_records.append(record)
                    
                elif strategy == DedupeAction.UPDATE:
                    if match.match_type == "odoo" and match.odoo_id:
                        # Mark for update
                        update_record = record.copy()
                        update_record["__odoo_id__"] = match.odoo_id
                        update_record["__action__"] = "update"
                        result.update_records.append(update_record)
                    else:
                        # Batch duplicate - skip later occurrence
                        result.duplicate_records.append(record)
                        
                elif strategy == DedupeAction.CREATE:
                    # Create anyway
                    result.unique_records.append(record)
            else:
                # No duplicate found
                result.unique_records.append(record)
                if key_hash:  # Only track non-empty keys
                    seen_keys[key_hash] = row_num
        
        return result
    
    def _make_key_hash(
        self,
        record: dict[str, Any],
        keys: list[str],
        case_sensitive: bool,
    ) -> str:
        """Create a hash string from key values."""
        values = []
        for key in keys:
            value = record.get(key)
            if value is None or value == "":
                continue
            str_value = str(value).strip()
            if not case_sensitive:
                str_value = str_value.lower()
            values.append(f"{key}:{str_value}")
        
        return "|".join(sorted(values)) if values else ""
    
    def _load_odoo_records(self, model: str, keys: list[str]) -> None:
        """Load existing Odoo records for duplicate checking."""
        if model in self._odoo_cache:
            return
        
        self._odoo_cache[model] = {}
        
        try:
            # Only load fields that exist
            # Get all records with the key fields
            fields_to_read = [k for k in keys if k != "id"]
            
            # Fetch in batches to avoid memory issues
            offset = 0
            batch_size = 5000
            
            while True:
                records = self.client.search_read(
                    model,
                    [],  # All records
                    ["id"] + fields_to_read,
                    offset=offset,
                    limit=batch_size,
                )
                
                if not records:
                    break
                
                for record in records:
                    key_hash = self._make_key_hash(record, keys, case_sensitive=False)
                    if key_hash:
                        self._odoo_cache[model][key_hash] = record["id"]
                
                if len(records) < batch_size:
                    break
                offset += batch_size
                
        except Exception:
            # On error, proceed without Odoo check
            pass
    
    def _check_odoo_duplicate(self, model: str, key_hash: str) -> int | None:
        """Check if a key hash exists in Odoo cache."""
        if not key_hash:
            return None
        return self._odoo_cache.get(model, {}).get(key_hash)
    
    def clear_cache(self, model: str | None = None) -> None:
        """Clear the Odoo record cache."""
        if model:
            self._odoo_cache.pop(model, None)
        else:
            self._odoo_cache.clear()
    
    def get_duplicate_report(
        self,
        result: DedupeResult,
    ) -> str:
        """Generate a human-readable duplicate report."""
        lines = [
            "=" * 60,
            "DUPLICATE DETECTION REPORT",
            "=" * 60,
            f"Total records processed: {len(result.unique_records) + len(result.duplicate_records)}",
            f"Unique records: {len(result.unique_records)}",
            f"Duplicate records: {result.total_duplicates}",
            f"  - Existing in Odoo: {result.odoo_duplicates}",
            f"  - Within batch: {result.batch_duplicates}",
            f"Records marked for update: {len(result.update_records)}",
            "",
        ]
        
        if result.matches:
            lines.append("DUPLICATE DETAILS:")
            lines.append("-" * 40)
            
            for match in result.matches[:50]:  # Limit to first 50
                if match.match_type == "odoo":
                    lines.append(
                        f"Row {match.source_row}: Exists in Odoo (ID: {match.odoo_id})"
                    )
                else:
                    lines.append(
                        f"Row {match.source_row}: Duplicate of row {match.batch_row}"
                    )
                lines.append(f"  Keys: {match.matched_keys}")
            
            if len(result.matches) > 50:
                lines.append(f"  ... and {len(result.matches) - 50} more")
        
        return "\n".join(lines)
