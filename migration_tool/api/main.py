"""
FastAPI Main Application

REST API for the Odoo Migration Tool.
Provides endpoints for the C# WPF GUI.
"""

from pathlib import Path
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from migration_tool.api.models import (
    ConnectRequest,
    ConnectResponse,
    FieldsResponse,
    FieldMetaResponse,
    ModelMetaResponse,
    ModelsListResponse,
    FilePreviewRequest,
    FilePreviewResponse,
    DryRunRequest,
    DryRunResponse,
    ValidationIssue,
    ImportRequest,
    ImportResponse,
    JobStatusResponse,
    JobStatus,
)
from migration_tool.api.jobs import job_manager, Job
from migration_tool.odoo import OdooClient, OdooConnectionError
from migration_tool.core.schema import SchemaInspector, SchemaCache
from migration_tool.core.reader import DataReader
from migration_tool.core.cleaner import DataCleaner
from migration_tool.core.validator import ValidationEngine
from migration_tool.core.deduplicator import Deduplicator
from migration_tool.core.batcher import BatchProcessor
from migration_tool.odoo.adapters import get_adapter, ReferenceCache


# ============================================================================
# App Configuration
# ============================================================================

app = FastAPI(
    title="Odoo Migration API",
    description="REST API for the Odoo Data Migration Tool",
    version="1.0.0",
)

# Enable CORS for local development
# NOTE: In production, configure allowed origins via environment variable
import os
CORS_ORIGINS = os.environ.get("CORS_ORIGINS", "http://localhost:3000,http://localhost:8080,http://127.0.0.1:3000,http://127.0.0.1:8080").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global state for authenticated sessions
_sessions: dict[str, OdooClient] = {}


# ============================================================================
# Health & Info
# ============================================================================

@app.get("/")
async def root():
    """API root - health check."""
    return {"status": "ok", "service": "Odoo Migration API"}


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


# ============================================================================
# Connection Management
# ============================================================================

@app.post("/connect", response_model=ConnectResponse)
async def connect(request: ConnectRequest):
    """
    Test connection to Odoo server.
    
    Returns connection status, Odoo version, and user ID.
    """
    try:
        client = OdooClient(
            url=request.url,
            db=request.database,
            username=request.username,
            password=request.password,
        )
        
        # Get version info (no auth needed)
        version_info = client.version()
        odoo_version = version_info.get("server_version", "unknown")
        version_list = version_info.get("server_version_info", [])
        
        # Authenticate
        uid = client.authenticate()
        
        # Store session for later use
        session_key = f"{request.url}:{request.database}:{request.username}"
        _sessions[session_key] = client
        
        return ConnectResponse(
            success=True,
            message="Connection successful",
            odoo_version=odoo_version,
            server_version_info=version_list,
            uid=uid,
            database=request.database,
        )
        
    except OdooConnectionError as e:
        return ConnectResponse(
            success=False,
            message=str(e),
        )
    except Exception as e:
        return ConnectResponse(
            success=False,
            message=f"Connection failed: {str(e)}",
        )


def _get_client(url: str, database: str, username: str, password: str) -> OdooClient:
    """Get or create authenticated client."""
    session_key = f"{url}:{database}:{username}"
    
    if session_key in _sessions:
        return _sessions[session_key]
    
    client = OdooClient(
        url=url,
        db=database,
        username=username,
        password=password,
    )
    client.authenticate()
    _sessions[session_key] = client
    return client


# ============================================================================
# Schema / Fields
# ============================================================================

@app.get("/models", response_model=ModelsListResponse)
async def get_models(
    url: str = Query(...),
    database: str = Query(...),
    username: str = Query(...),
    password: str = Query(...),
):
    """Get list of available Odoo models."""
    try:
        client = _get_client(url, database, username, password)
        inspector = SchemaInspector(client, cache=SchemaCache())
        
        models = inspector.get_models()
        
        return ModelsListResponse(success=True, models=models)
        
    except Exception as e:
        return ModelsListResponse(success=False, error=str(e))


@app.get("/fields", response_model=FieldsResponse)
async def get_fields(
    model: str = Query(..., description="Odoo model name"),
    url: str = Query(...),
    database: str = Query(...),
    username: str = Query(...),
    password: str = Query(...),
    refresh: bool = Query(False, description="Force refresh from Odoo"),
):
    """
    Get field metadata for an Odoo model.
    
    Returns all fields with their types, importability, etc.
    """
    try:
        client = _get_client(url, database, username, password)
        cache = SchemaCache()
        inspector = SchemaInspector(client, cache=cache)
        
        model_meta = inspector.get_model(model, refresh=refresh)
        
        if not model_meta:
            return FieldsResponse(
                success=False,
                model=model,
                error=f"Model '{model}' not found or not accessible",
            )
        
        # Convert to response format
        fields_response = [
            FieldMetaResponse(
                name=f.name,
                label=f.label,
                type=f.field_type.value,
                required=f.required,
                readonly=f.readonly,
                importable=f.importable,
                exportable=f.exportable,
                relation=f.relation,
                selection=f.selection,
                help_text=f.help_text,
                is_custom=f.is_custom,
            )
            for f in model_meta.fields.values()
        ]
        
        model_response = ModelMetaResponse(
            name=model_meta.name,
            label=model_meta.label,
            can_create=model_meta.can_create,
            can_read=model_meta.can_read,
            can_write=model_meta.can_write,
            fields=fields_response,
            importable_count=len(model_meta.importable_fields),
            required_count=len(model_meta.required_fields),
        )
        
        return FieldsResponse(
            success=True,
            model=model,
            data=model_response,
        )
        
    except Exception as e:
        return FieldsResponse(
            success=False,
            model=model,
            error=str(e),
        )


# ============================================================================
# File Preview
# ============================================================================

@app.post("/preview", response_model=FilePreviewResponse)
async def preview_file(request: FilePreviewRequest):
    """
    Preview file contents.
    
    Returns columns and first N rows.
    """
    try:
        file_path = Path(request.file_path)
        if not file_path.exists():
            return FilePreviewResponse(
                success=False,
                error=f"File not found: {request.file_path}",
            )
        
        reader = DataReader()
        result = reader.read_file(
            file_path,
            sheet=request.sheet,
        )
        
        if result.errors:
            return FilePreviewResponse(
                success=False,
                error="; ".join(result.errors),
            )
        
        # Get preview rows
        preview_df = result.data.head(request.rows)
        columns = list(preview_df.columns)
        rows = preview_df.to_dict("records")
        
        return FilePreviewResponse(
            success=True,
            columns=columns,
            rows=rows,
            total_rows=result.total_rows,
        )
        
    except Exception as e:
        return FilePreviewResponse(
            success=False,
            error=str(e),
        )


# ============================================================================
# Dry Run
# ============================================================================

@app.post("/dry-run", response_model=DryRunResponse)
async def dry_run(request: DryRunRequest):
    """
    Run validation without importing.
    
    Validates data, checks for duplicates, and reports issues.
    """
    try:
        file_path = Path(request.file_path)
        if not file_path.exists():
            return DryRunResponse(
                success=False,
                error=f"File not found: {request.file_path}",
            )
        
        # Read file
        reader = DataReader()
        
        # Build column mapping from request
        column_mapping = {
            m.source_column: m.target_field
            for m in request.mapping.mappings
        }
        
        read_result = reader.read_file(
            file_path,
            mapping=column_mapping,
            sheet=request.sheet,
        )
        
        if read_result.errors:
            return DryRunResponse(
                success=False,
                error="; ".join(read_result.errors),
            )
        
        # Clean data
        cleaner = DataCleaner()
        df = cleaner.clean(read_result.data)
        records = df.to_dict("records")
        
        # Apply defaults
        for record in records:
            for field, default in request.mapping.defaults.items():
                if field not in record or record[field] is None:
                    record[field] = default
        
        # Validate
        validator = ValidationEngine()  # No Odoo client for dry-run
        
        # Get required fields from mapping
        required = [
            m.target_field for m in request.mapping.mappings
            # You could enhance this to check against schema
        ]
        
        validation_result = validator.validate(records, required=[])
        
        # Convert issues
        issues = [
            ValidationIssue(
                row=issue.row,
                field=issue.field,
                message=issue.message,
                severity=issue.severity.value if hasattr(issue.severity, 'value') else str(issue.severity),
            )
            for issue in validation_result.issues
        ]
        
        return DryRunResponse(
            success=True,
            valid=validation_result.is_valid,
            total_records=len(records),
            valid_records=len(validation_result.valid_records),
            error_count=validation_result.error_count,
            warning_count=validation_result.warning_count,
            issues=issues[:100],  # Limit issues returned
        )
        
    except Exception as e:
        return DryRunResponse(
            success=False,
            error=str(e),
        )


# ============================================================================
# Import Job
# ============================================================================

@app.post("/import", response_model=ImportResponse)
async def start_import(
    request: ImportRequest,
    url: str = Query(...),
    database: str = Query(...),
    username: str = Query(...),
    password: str = Query(...),
):
    """
    Start an import job.
    
    Returns job_id for status polling.
    """
    try:
        # Validate file exists
        file_path = Path(request.file_path)
        if not file_path.exists():
            return ImportResponse(
                success=False,
                error=f"File not found: {request.file_path}",
            )
        
        # Get client
        client = _get_client(url, database, username, password)
        
        # Create job
        job = job_manager.create_job(request_data={
            "file_path": request.file_path,
            "model": request.model,
            "mapping": request.mapping.model_dump(),
            "sheet": request.sheet,
            "batch_size": request.batch_size,
            "stop_on_error": request.stop_on_error,
            "url": url,
            "database": database,
            "username": username,
            "password": password,
        })
        
        # Define import task
        def run_import(job: Job):
            _execute_import(job, client, request)
        
        # Start job
        job_manager.start_job(job.id, run_import)
        
        return ImportResponse(
            success=True,
            job_id=job.id,
            message="Import job started",
        )
        
    except Exception as e:
        return ImportResponse(
            success=False,
            error=str(e),
        )


def _execute_import(job: Job, client: OdooClient, request: ImportRequest):
    """Execute the import job with detailed error logging."""
    import traceback
    
    try:
        # Read file
        reader = DataReader()
        column_mapping = {
            m.source_column: m.target_field
            for m in request.mapping.mappings
        }
        
        if not column_mapping:
            job.errors.append("No column mappings provided")
            return
        
        read_result = reader.read_file(
            Path(request.file_path),
            mapping=column_mapping,
            sheet=request.sheet,
        )
        
        if read_result.errors:
            job.errors.extend(read_result.errors)
            return
        
        # Clean
        cleaner = DataCleaner()
        df = cleaner.clean(read_result.data)
        records = df.to_dict("records")
        
        # Apply defaults
        for record in records:
            for field, default in request.mapping.defaults.items():
                if field not in record or record[field] is None:
                    record[field] = default
        
        job.total_records = len(records)
        
        if job.total_records == 0:
            job.errors.append("No records to import after processing file")
            return
        
        # Get adapter
        cache = ReferenceCache()
        
        try:
            adapter = get_adapter(request.model, client, cache)
        except Exception as e:
            job.errors.append(f"Failed to get adapter for model '{request.model}': {str(e)}")
            return
        
        # Process in batches
        batch_size = request.batch_size or 100
        batches = [
            records[i:i + batch_size]
            for i in range(0, len(records), batch_size)
        ]
        job.total_batches = len(batches)
        
        for batch_idx, batch in enumerate(batches):
            if job.status.value == "cancelled":
                break
            
            job.current_batch = batch_idx + 1
            
            for row_idx, record in enumerate(batch):
                global_row = batch_idx * batch_size + row_idx + 2  # +2 for header row
                try:
                    # Prepare record using adapter
                    prepared = adapter.prepare(record)
                    
                    if not prepared:
                        job.failed_records += 1
                        job.errors.append(f"Row {global_row}: Empty record after preparation")
                        continue
                    
                    # Create record in Odoo
                    result = client.create(request.model, prepared)
                    
                    if result:
                        job.created_records += 1
                    else:
                        job.failed_records += 1
                        job.errors.append(f"Row {global_row}: Create returned no ID")
                        
                except Exception as e:
                    job.failed_records += 1
                    error_msg = str(e)
                    
                    # Try to extract meaningful error from Odoo
                    if "DETAIL:" in error_msg:
                        # PostgreSQL constraint error
                        detail = error_msg.split("DETAIL:")[-1].strip()
                        job.errors.append(f"Row {global_row}: {detail[:200]}")
                    elif "ValidationError" in error_msg:
                        job.errors.append(f"Row {global_row}: Validation failed - {error_msg[:200]}")
                    elif "psycopg2" in error_msg.lower() or "IntegrityError" in error_msg:
                        # Database constraint error
                        if "duplicate key" in error_msg.lower():
                            job.errors.append(f"Row {global_row}: Duplicate record already exists")
                        elif "null value" in error_msg.lower():
                            job.errors.append(f"Row {global_row}: Required field is empty")
                        else:
                            job.errors.append(f"Row {global_row}: Database error - {error_msg[:150]}")
                    else:
                        # Generic error - include original record data for debugging
                        record_preview = str(record)[:100] + "..." if len(str(record)) > 100 else str(record)
                        job.errors.append(f"Row {global_row}: {error_msg[:150]} | Data: {record_preview}")
                    
                    if request.stop_on_error:
                        return
                
                job.processed_records += 1
                job_manager.update_progress(
                    job,
                    processed=job.processed_records,
                    total=job.total_records,
                    created=job.created_records,
                    failed=job.failed_records,
                    current_batch=job.current_batch,
                    total_batches=job.total_batches,
                )
        
    except Exception as e:
        job.errors.append(f"Import error: {str(e)}")
        job.errors.append(f"Traceback: {traceback.format_exc()[:500]}")


@app.get("/status/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Get status of an import job."""
    job = job_manager.get_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    
    return JobStatusResponse(**job.to_dict())


@app.post("/cancel/{job_id}")
async def cancel_job(job_id: str):
    """Cancel a running job."""
    if job_manager.cancel_job(job_id):
        return {"success": True, "message": f"Job {job_id} cancelled"}
    else:
        raise HTTPException(status_code=400, detail=f"Cannot cancel job {job_id}")


# ============================================================================
# Entry Point
# ============================================================================

def run_server(host: str = "127.0.0.1", port: int = 8000):
    """Run the API server."""
    import uvicorn
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    run_server()
