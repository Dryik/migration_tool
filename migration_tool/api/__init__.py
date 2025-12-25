"""
REST API Module

FastAPI-based REST API for the migration tool.
Provides endpoints for the C# WPF GUI.
"""

from migration_tool.api.main import app
from migration_tool.api.models import (
    ConnectRequest,
    ConnectResponse,
    FieldsResponse,
    DryRunRequest,
    DryRunResponse,
    ImportRequest,
    ImportResponse,
    JobStatusResponse,
)

__all__ = [
    "app",
    "ConnectRequest",
    "ConnectResponse",
    "FieldsResponse",
    "DryRunRequest",
    "DryRunResponse",
    "ImportRequest",
    "ImportResponse",
    "JobStatusResponse",
]
