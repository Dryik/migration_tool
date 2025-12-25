"""
Migration Logger

Comprehensive logging and audit trail for migration operations.
"""

import json
import csv
from pathlib import Path
from typing import Any
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum


class LogLevel(Enum):
    """Log level for entries."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    SUCCESS = "SUCCESS"


@dataclass
class AuditEntry:
    """A single audit log entry for a record."""
    
    timestamp: str
    source_file: str
    source_row: int
    target_model: str
    action: str  # create, update, skip, fail
    
    # Payload and response
    payload: dict[str, Any] = field(default_factory=dict)
    odoo_id: int | None = None
    odoo_response: Any = None
    
    # Status
    success: bool = True
    error_message: str | None = None
    
    # Additional metadata
    duration_ms: float | None = None
    batch_id: int | None = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp,
            "source_file": self.source_file,
            "source_row": self.source_row,
            "target_model": self.target_model,
            "action": self.action,
            "odoo_id": self.odoo_id,
            "success": self.success,
            "error_message": self.error_message,
            "duration_ms": self.duration_ms,
            "batch_id": self.batch_id,
            "payload": self.payload,
        }


@dataclass
class SummaryReport:
    """Summary of a migration run."""
    
    migration_name: str
    started_at: datetime
    completed_at: datetime | None = None
    
    total_records: int = 0
    successful_records: int = 0
    failed_records: int = 0
    skipped_records: int = 0
    updated_records: int = 0
    
    models_processed: list[str] = field(default_factory=list)
    error_summary: dict[str, int] = field(default_factory=dict)  # error_type -> count
    
    dry_run: bool = False
    
    @property
    def success_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return self.successful_records / self.total_records * 100
    
    @property
    def duration(self) -> str:
        if not self.completed_at:
            return "In progress"
        delta = self.completed_at - self.started_at
        minutes = int(delta.total_seconds() // 60)
        seconds = int(delta.total_seconds() % 60)
        return f"{minutes}m {seconds}s"
    
    def to_text(self) -> str:
        """Generate human-readable summary."""
        lines = [
            "=" * 60,
            f"MIGRATION SUMMARY: {self.migration_name}",
            "=" * 60,
            f"{'Status:':<20} {'DRY RUN' if self.dry_run else 'COMPLETED'}",
            f"{'Started:':<20} {self.started_at.strftime('%Y-%m-%d %H:%M:%S')}",
            f"{'Duration:':<20} {self.duration}",
            "",
            "RECORD STATISTICS",
            "-" * 40,
            f"{'Total Records:':<20} {self.total_records:,}",
            f"{'Successful:':<20} {self.successful_records:,} ({self.success_rate:.1f}%)",
            f"{'Failed:':<20} {self.failed_records:,}",
            f"{'Skipped:':<20} {self.skipped_records:,}",
            f"{'Updated:':<20} {self.updated_records:,}",
            "",
            "MODELS PROCESSED",
            "-" * 40,
        ]
        
        for model in self.models_processed:
            lines.append(f"  • {model}")
        
        if self.error_summary:
            lines.extend([
                "",
                "ERROR SUMMARY",
                "-" * 40,
            ])
            for error_type, count in sorted(
                self.error_summary.items(), key=lambda x: -x[1]
            ):
                lines.append(f"  {error_type}: {count}")
        
        lines.append("=" * 60)
        return "\n".join(lines)


class MigrationLogger:
    """
    Comprehensive logger for migration operations.
    
    Features:
    - Per-record audit trail
    - JSON and CSV export
    - Human-readable summary reports
    - Console progress with Rich
    
    Example:
        >>> logger = MigrationLogger("./logs")
        >>> logger.start_migration("customer_import")
        >>> logger.log_record(entry)
        >>> summary = logger.end_migration()
    """
    
    def __init__(
        self,
        output_dir: str | Path = "./logs",
        console_output: bool = True,
    ):
        """
        Initialize logger.
        
        Args:
            output_dir: Directory for log files
            console_output: Whether to print to console
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.console_output = console_output
        
        self._entries: list[AuditEntry] = []
        self._current_migration: str | None = None
        self._summary: SummaryReport | None = None
        self._log_file: Path | None = None
        
        # Try to import Rich for pretty console output
        try:
            from rich.console import Console
            from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
            self._console = Console()
            self._has_rich = True
        except ImportError:
            self._console = None
            self._has_rich = False
    
    def start_migration(self, name: str, dry_run: bool = False) -> None:
        """
        Start a new migration logging session.
        
        Args:
            name: Migration name/identifier
            dry_run: Whether this is a dry run
        """
        self._current_migration = name
        self._entries = []
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._log_file = self.output_dir / f"{name}_{timestamp}.json"
        
        self._summary = SummaryReport(
            migration_name=name,
            started_at=datetime.now(),
            dry_run=dry_run,
        )
        
        self._log_message(LogLevel.INFO, f"Starting migration: {name}")
    
    def log_record(self, entry: AuditEntry) -> None:
        """
        Log a single record operation.
        
        Args:
            entry: Audit entry for the record
        """
        self._entries.append(entry)
        
        # Update summary
        if self._summary:
            self._summary.total_records += 1
            
            if entry.success:
                if entry.action == "create":
                    self._summary.successful_records += 1
                elif entry.action == "update":
                    self._summary.updated_records += 1
                elif entry.action == "skip":
                    self._summary.skipped_records += 1
            else:
                self._summary.failed_records += 1
                
                # Track error types
                error_type = entry.error_message or "Unknown error"
                # Truncate long error messages for categorization
                if len(error_type) > 50:
                    error_type = error_type[:50] + "..."
                self._summary.error_summary[error_type] = (
                    self._summary.error_summary.get(error_type, 0) + 1
                )
            
            # Track models
            if entry.target_model not in self._summary.models_processed:
                self._summary.models_processed.append(entry.target_model)
    
    def log_batch_start(self, batch_id: int, record_count: int, model: str) -> None:
        """Log the start of a batch."""
        self._log_message(
            LogLevel.INFO,
            f"Batch {batch_id}: Processing {record_count} {model} records"
        )
    
    def log_batch_complete(
        self,
        batch_id: int,
        created: int,
        failed: int,
        duration_ms: float,
    ) -> None:
        """Log batch completion."""
        level = LogLevel.SUCCESS if failed == 0 else LogLevel.WARNING
        self._log_message(
            level,
            f"Batch {batch_id}: Created {created}, Failed {failed} ({duration_ms:.0f}ms)"
        )
    
    def log_error(self, message: str, record: dict | None = None) -> None:
        """Log an error message."""
        self._log_message(LogLevel.ERROR, message)
        
        if record:
            entry = AuditEntry(
                timestamp=datetime.now().isoformat(),
                source_file=record.get("__source_file__", ""),
                source_row=record.get("__source_row__", 0),
                target_model="",
                action="fail",
                payload=record,
                success=False,
                error_message=message,
            )
            self._entries.append(entry)
    
    def log_warning(self, message: str) -> None:
        """Log a warning message."""
        self._log_message(LogLevel.WARNING, message)
    
    def log_info(self, message: str) -> None:
        """Log an info message."""
        self._log_message(LogLevel.INFO, message)
    
    def end_migration(self) -> SummaryReport:
        """
        End the migration session and generate reports.
        
        Returns:
            Summary report
        """
        if not self._summary:
            raise RuntimeError("No migration in progress")
        
        self._summary.completed_at = datetime.now()
        
        # Export logs
        self.export_json()
        self.export_csv()
        
        # Print summary
        if self.console_output:
            print(self._summary.to_text())
        
        # Save summary
        summary_file = self.output_dir / f"{self._current_migration}_summary.txt"
        with open(summary_file, "w", encoding="utf-8") as f:
            f.write(self._summary.to_text())
        
        self._log_message(LogLevel.SUCCESS, "Migration completed")
        
        return self._summary
    
    def export_json(self, filepath: Path | None = None) -> Path:
        """
        Export audit log to JSON file.
        
        Args:
            filepath: Custom output path (uses default if None)
            
        Returns:
            Path to exported file
        """
        output_path = filepath or self._log_file or (
            self.output_dir / f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        data = {
            "migration": self._current_migration,
            "started_at": self._summary.started_at.isoformat() if self._summary else None,
            "completed_at": self._summary.completed_at.isoformat() if self._summary and self._summary.completed_at else None,
            "entries": [e.to_dict() for e in self._entries],
        }
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        return output_path
    
    def export_csv(self, filepath: Path | None = None) -> Path:
        """
        Export audit log to CSV file.
        
        Args:
            filepath: Custom output path
            
        Returns:
            Path to exported file
        """
        output_path = filepath or (
            self.output_dir / f"{self._current_migration}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        
        if not self._entries:
            return output_path
        
        fieldnames = [
            "timestamp", "source_file", "source_row", "target_model",
            "action", "odoo_id", "success", "error_message", "duration_ms"
        ]
        
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for entry in self._entries:
                writer.writerow(entry.to_dict())
        
        return output_path
    
    def get_errors(self) -> list[AuditEntry]:
        """Get all error entries."""
        return [e for e in self._entries if not e.success]
    
    def get_entries_for_model(self, model: str) -> list[AuditEntry]:
        """Get all entries for a specific model."""
        return [e for e in self._entries if e.target_model == model]
    
    def _log_message(self, level: LogLevel, message: str) -> None:
        """Log a message to console."""
        if not self.console_output:
            return
        
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        if self._has_rich and self._console:
            colors = {
                LogLevel.DEBUG: "dim",
                LogLevel.INFO: "blue",
                LogLevel.WARNING: "yellow",
                LogLevel.ERROR: "red bold",
                LogLevel.SUCCESS: "green bold",
            }
            color = colors.get(level, "white")
            self._console.print(f"[dim]{timestamp}[/] [{color}]{level.value}[/] {message}")
        else:
            print(f"{timestamp} [{level.value}] {message}")
    
    def create_progress_bar(self, total: int, description: str = "Processing"):
        """
        Create a progress bar for batch processing.
        
        Returns a context manager if Rich is available, otherwise None.
        """
        if self._has_rich:
            from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
            return Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeRemainingColumn(),
                console=self._console,
            )
        return None
