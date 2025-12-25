"""
API Request/Response Models

Pydantic models for FastAPI endpoints.
"""

from pydantic import BaseModel, Field
from typing import Any
from enum import Enum


# ============================================================================
# Connection
# ============================================================================

class ConnectRequest(BaseModel):
    """Request to test Odoo connection."""
    url: str = Field(..., description="Odoo server URL")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Username")
    password: str = Field(..., description="Password or API key")


class ConnectResponse(BaseModel):
    """Response from connection test."""
    success: bool
    message: str | None = None
    odoo_version: str | None = None
    server_version_info: list[Any] | None = None  # Can contain int, str, etc.
    uid: int | None = None
    database: str | None = None


# ============================================================================
# Fields / Schema
# ============================================================================

class FieldMetaResponse(BaseModel):
    """Single field metadata."""
    name: str
    label: str
    type: str
    required: bool = False
    readonly: bool = False
    importable: bool = False
    exportable: bool = False
    relation: str | None = None
    selection: list[tuple[str, str]] | None = None
    help_text: str | None = None
    is_custom: bool = False


class ModelMetaResponse(BaseModel):
    """Model metadata with fields."""
    name: str
    label: str
    can_create: bool = False
    can_read: bool = False
    can_write: bool = False
    fields: list[FieldMetaResponse] = []
    importable_count: int = 0
    required_count: int = 0


class FieldsResponse(BaseModel):
    """Response for /fields endpoint."""
    success: bool
    model: str
    data: ModelMetaResponse | None = None
    error: str | None = None


class ModelsListResponse(BaseModel):
    """Response for /models endpoint."""
    success: bool
    models: list[str] = []
    error: str | None = None


# ============================================================================
# Column Mapping
# ============================================================================

class ColumnMapping(BaseModel):
    """Mapping from source column to Odoo field."""
    source_column: str
    target_field: str
    transform: str | None = None  # e.g., "normalize_phone", "uppercase"


class MappingConfig(BaseModel):
    """Full mapping configuration."""
    model: str
    mappings: list[ColumnMapping]
    defaults: dict[str, Any] = {}


# ============================================================================
# File Preview
# ============================================================================

class FilePreviewRequest(BaseModel):
    """Request to preview file contents."""
    file_path: str
    sheet: str | None = None
    rows: int = 10


class FilePreviewResponse(BaseModel):
    """Response with file preview."""
    success: bool
    columns: list[str] = []
    rows: list[dict[str, Any]] = []
    total_rows: int = 0
    error: str | None = None


# ============================================================================
# Dry Run
# ============================================================================

class ValidationIssue(BaseModel):
    """A single validation issue."""
    row: int
    field: str | None = None
    message: str
    severity: str = "error"  # error, warning, info


class DryRunRequest(BaseModel):
    """Request for dry-run validation."""
    file_path: str
    model: str
    mapping: MappingConfig
    sheet: str | None = None


class DryRunResponse(BaseModel):
    """Response from dry-run."""
    success: bool
    valid: bool = False
    total_records: int = 0
    valid_records: int = 0
    error_count: int = 0
    warning_count: int = 0
    issues: list[ValidationIssue] = []
    duplicate_count: int = 0
    error: str | None = None


# ============================================================================
# Import Job
# ============================================================================

class JobStatus(str, Enum):
    """Status of an import job."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ImportRequest(BaseModel):
    """Request to start import job."""
    file_path: str
    model: str
    mapping: MappingConfig
    sheet: str | None = None
    batch_size: int = 100
    stop_on_error: bool = False


class ImportResponse(BaseModel):
    """Response when import job is started."""
    success: bool
    job_id: str | None = None
    message: str | None = None
    error: str | None = None


class JobStatusResponse(BaseModel):
    """Status of a running/completed job."""
    job_id: str
    status: JobStatus
    progress: float = 0.0  # 0.0 to 100.0
    total_records: int = 0
    processed_records: int = 0
    created_records: int = 0
    failed_records: int = 0
    current_batch: int = 0
    total_batches: int = 0
    errors: list[str] = []
    started_at: str | None = None
    completed_at: str | None = None
    duration_seconds: float | None = None


# ============================================================================
# Logs & Reports
# ============================================================================

class LogEntry(BaseModel):
    """A single log entry."""
    timestamp: str
    level: str
    message: str
    details: dict[str, Any] | None = None


class ExportLogsRequest(BaseModel):
    """Request to export logs."""
    job_id: str
    format: str = "json"  # json, csv


class ExportLogsResponse(BaseModel):
    """Response with log data."""
    success: bool
    data: list[LogEntry] = []
    error: str | None = None
