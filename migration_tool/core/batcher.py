"""
Batch Processor

Handles chunked import with progress tracking, state persistence, and resume capability.
"""

import json
import time
from pathlib import Path
from typing import Any, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class BatchStatus(Enum):
    """Status of a batch."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class BatchInfo:
    """Information about a single batch."""
    
    batch_id: int
    start_index: int
    end_index: int
    record_count: int
    status: BatchStatus = BatchStatus.PENDING
    created_ids: list[int] = field(default_factory=list)
    error_message: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    retry_count: int = 0
    
    @property
    def duration_seconds(self) -> float | None:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


@dataclass
class BatchResult:
    """Result of batch processing."""
    
    total_records: int = 0
    processed_records: int = 0
    created_records: int = 0
    updated_records: int = 0
    failed_records: int = 0
    skipped_records: int = 0
    
    batches: list[BatchInfo] = field(default_factory=list)
    created_ids: list[int] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    
    started_at: datetime | None = None
    completed_at: datetime | None = None
    
    @property
    def success_rate(self) -> float:
        if self.processed_records == 0:
            return 0.0
        return (self.created_records + self.updated_records) / self.processed_records * 100
    
    @property
    def duration_seconds(self) -> float | None:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None
    
    @property
    def records_per_second(self) -> float | None:
        duration = self.duration_seconds
        if duration and duration > 0:
            return self.processed_records / duration
        return None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "total_records": self.total_records,
            "processed_records": self.processed_records,
            "created_records": self.created_records,
            "updated_records": self.updated_records,
            "failed_records": self.failed_records,
            "skipped_records": self.skipped_records,
            "success_rate": self.success_rate,
            "duration_seconds": self.duration_seconds,
            "records_per_second": self.records_per_second,
            "created_ids": self.created_ids,
            "errors": self.errors,
            "batches": [
                {
                    "batch_id": b.batch_id,
                    "record_count": b.record_count,
                    "status": b.status.value,
                    "created_count": len(b.created_ids),
                    "error": b.error_message,
                }
                for b in self.batches
            ],
        }


@dataclass
class BatchState:
    """Persistent state for resume capability."""
    
    model: str
    total_records: int
    chunk_size: int
    last_completed_batch: int
    created_ids: list[int]
    source_file: str
    started_at: str
    config_hash: str = ""
    
    def save(self, path: Path) -> None:
        """Save state to file."""
        state_dict = {
            "model": self.model,
            "total_records": self.total_records,
            "chunk_size": self.chunk_size,
            "last_completed_batch": self.last_completed_batch,
            "created_ids": self.created_ids,
            "source_file": self.source_file,
            "started_at": self.started_at,
            "config_hash": self.config_hash,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state_dict, f, indent=2)
    
    @classmethod
    def load(cls, path: Path) -> "BatchState":
        """Load state from file."""
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls(**data)


class BatchProcessor:
    """
    Processes records in batches with progress tracking and resume capability.
    
    Features:
    - Configurable chunk size
    - Retry on failure with exponential backoff
    - State persistence for resume
    - Progress callbacks
    - Dry-run mode
    
    Example:
        >>> processor = BatchProcessor(client, chunk_size=500)
        >>> result = processor.process(
        ...     records,
        ...     model="res.partner",
        ...     adapter=partner_adapter,
        ...     dry_run=False
        ... )
    """
    
    def __init__(
        self,
        odoo_client: Any,
        chunk_size: int = 500,
        retry_attempts: int = 3,
        retry_delay: float = 2.0,
        state_dir: Path | None = None,
    ):
        """
        Initialize batch processor.
        
        Args:
            odoo_client: Odoo client for API calls
            chunk_size: Records per batch
            retry_attempts: Max retries per batch
            retry_delay: Initial delay between retries
            state_dir: Directory for state persistence
        """
        self.client = odoo_client
        self.chunk_size = chunk_size
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.state_dir = state_dir or Path("./migration_state")
        
        self._progress_callback: Callable[[int, int, str], None] | None = None
        self._stop_requested = False
    
    def process(
        self,
        records: list[dict[str, Any]],
        model: str,
        adapter: Any | None = None,
        dry_run: bool = False,
        stop_on_error: bool = False,
        on_progress: Callable[[int, int, str], None] | None = None,
    ) -> BatchResult:
        """
        Process records in batches.
        
        Args:
            records: Records to import
            model: Odoo model name
            adapter: Model adapter for record preparation
            dry_run: Validate without creating
            stop_on_error: Stop on first error
            on_progress: Callback(processed, total, message)
            
        Returns:
            BatchResult with processing summary
        """
        self._progress_callback = on_progress
        self._stop_requested = False
        
        result = BatchResult(
            total_records=len(records),
            started_at=datetime.now(),
        )
        
        # Create batches
        batches = self._create_batches(records)
        result.batches = batches
        
        self._notify_progress(0, len(records), "Starting batch processing...")
        
        for batch_info in batches:
            if self._stop_requested:
                batch_info.status = BatchStatus.SKIPPED
                continue
            
            batch_records = records[batch_info.start_index:batch_info.end_index]
            
            self._notify_progress(
                batch_info.start_index,
                len(records),
                f"Processing batch {batch_info.batch_id + 1}/{len(batches)}..."
            )
            
            # Process batch with retry
            success = self._process_batch(
                batch_info,
                batch_records,
                model,
                adapter,
                dry_run,
            )
            
            # Update result
            if batch_info.status == BatchStatus.COMPLETED:
                result.processed_records += batch_info.record_count
                result.created_records += len(batch_info.created_ids)
                result.created_ids.extend(batch_info.created_ids)
            elif batch_info.status == BatchStatus.FAILED:
                result.failed_records += batch_info.record_count
                if batch_info.error_message:
                    result.errors.append({
                        "batch_id": batch_info.batch_id,
                        "message": batch_info.error_message,
                        "record_range": f"{batch_info.start_index}-{batch_info.end_index}",
                    })
                
                if stop_on_error:
                    self._stop_requested = True
            
            # Save state after each batch
            if not dry_run:
                self._save_state(
                    model=model,
                    records=records,
                    last_batch=batch_info.batch_id,
                    created_ids=result.created_ids,
                )
        
        result.completed_at = datetime.now()
        
        self._notify_progress(
            len(records),
            len(records),
            f"Completed: {result.created_records} created, {result.failed_records} failed"
        )
        
        return result
    
    def _create_batches(self, records: list[dict[str, Any]]) -> list[BatchInfo]:
        """Create batch info objects for all records."""
        batches = []
        total = len(records)
        
        for i, start in enumerate(range(0, total, self.chunk_size)):
            end = min(start + self.chunk_size, total)
            batches.append(BatchInfo(
                batch_id=i,
                start_index=start,
                end_index=end,
                record_count=end - start,
            ))
        
        return batches
    
    def _process_batch(
        self,
        batch_info: BatchInfo,
        records: list[dict[str, Any]],
        model: str,
        adapter: Any | None,
        dry_run: bool,
    ) -> bool:
        """Process a single batch with retry logic."""
        batch_info.status = BatchStatus.IN_PROGRESS
        batch_info.started_at = datetime.now()
        
        for attempt in range(self.retry_attempts):
            batch_info.retry_count = attempt
            
            try:
                # Prepare records with adapter
                prepared_records = []
                for record in records:
                    if adapter:
                        prepared = adapter.prepare_record(record)
                    else:
                        prepared = {
                            k: v for k, v in record.items()
                            if not k.startswith("__")
                        }
                    prepared_records.append(prepared)
                
                if dry_run:
                    # Validate without creating
                    batch_info.status = BatchStatus.COMPLETED
                    batch_info.completed_at = datetime.now()
                    return True
                
                # Check for updates vs creates
                creates = []
                updates = []
                
                for record in prepared_records:
                    if record.get("__action__") == "update" and record.get("__odoo_id__"):
                        updates.append(record)
                    else:
                        # Remove internal fields
                        clean = {k: v for k, v in record.items() if not k.startswith("__")}
                        creates.append(clean)
                
                # Perform creates
                if creates:
                    created_ids = self.client.create_batch(
                        model, creates, chunk_size=len(creates)
                    )
                    batch_info.created_ids.extend(created_ids)
                
                # Perform updates
                for record in updates:
                    odoo_id = record["__odoo_id__"]
                    clean = {k: v for k, v in record.items() if not k.startswith("__")}
                    self.client.write(model, [odoo_id], clean)
                
                batch_info.status = BatchStatus.COMPLETED
                batch_info.completed_at = datetime.now()
                return True
                
            except Exception as e:
                batch_info.error_message = str(e)
                
                if attempt < self.retry_attempts - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    time.sleep(delay)
                else:
                    batch_info.status = BatchStatus.FAILED
                    batch_info.completed_at = datetime.now()
                    return False
        
        return False
    
    def _save_state(
        self,
        model: str,
        records: list[dict[str, Any]],
        last_batch: int,
        created_ids: list[int],
    ) -> None:
        """Save processing state for resume."""
        state = BatchState(
            model=model,
            total_records=len(records),
            chunk_size=self.chunk_size,
            last_completed_batch=last_batch,
            created_ids=created_ids,
            source_file=records[0].get("__source_file__", "") if records else "",
            started_at=datetime.now().isoformat(),
        )
        
        state_file = self.state_dir / f"{model.replace('.', '_')}_state.json"
        state.save(state_file)
    
    def resume(
        self,
        records: list[dict[str, Any]],
        model: str,
        adapter: Any | None = None,
        dry_run: bool = False,
        on_progress: Callable[[int, int, str], None] | None = None,
    ) -> BatchResult:
        """
        Resume processing from last saved state.
        
        Args:
            records: All records (will skip already processed)
            model: Odoo model name
            adapter: Model adapter
            dry_run: Validate without creating
            on_progress: Progress callback
            
        Returns:
            BatchResult for remaining records
        """
        state_file = self.state_dir / f"{model.replace('.', '_')}_state.json"
        
        if not state_file.exists():
            # No state, process all
            return self.process(records, model, adapter, dry_run, False, on_progress)
        
        state = BatchState.load(state_file)
        
        # Calculate resume point
        resume_index = (state.last_completed_batch + 1) * self.chunk_size
        
        if resume_index >= len(records):
            # Already complete
            return BatchResult(
                total_records=len(records),
                processed_records=len(records),
                created_records=len(state.created_ids),
                created_ids=state.created_ids,
            )
        
        # Process remaining
        remaining_records = records[resume_index:]
        result = self.process(
            remaining_records, model, adapter, dry_run, False, on_progress
        )
        
        # Merge with previous state
        result.created_ids = state.created_ids + result.created_ids
        result.created_records = len(result.created_ids)
        
        return result
    
    def _notify_progress(self, current: int, total: int, message: str) -> None:
        """Call progress callback if set."""
        if self._progress_callback:
            self._progress_callback(current, total, message)
    
    def request_stop(self) -> None:
        """Request graceful stop after current batch."""
        self._stop_requested = True
    
    def clear_state(self, model: str | None = None) -> None:
        """Clear saved state files."""
        if model:
            state_file = self.state_dir / f"{model.replace('.', '_')}_state.json"
            if state_file.exists():
                state_file.unlink()
        else:
            # Clear all state files
            if self.state_dir.exists():
                for f in self.state_dir.glob("*_state.json"):
                    f.unlink()
