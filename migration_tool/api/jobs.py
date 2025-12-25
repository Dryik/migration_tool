"""
Background Job Manager

Manages long-running import jobs with status tracking.
"""

import uuid
import threading
from datetime import datetime
from typing import Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor


class JobStatus(Enum):
    """Status of an import job."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Job:
    """Represents an import job."""
    
    id: str
    status: JobStatus = JobStatus.PENDING
    progress: float = 0.0
    
    # Record counts
    total_records: int = 0
    processed_records: int = 0
    created_records: int = 0
    failed_records: int = 0
    
    # Batch info
    current_batch: int = 0
    total_batches: int = 0
    
    # Errors
    errors: list[str] = field(default_factory=list)
    
    # Timing
    started_at: datetime | None = None
    completed_at: datetime | None = None
    
    # Request data (for retry)
    request_data: dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration_seconds(self) -> float | None:
        if not self.started_at:
            return None
        end = self.completed_at or datetime.now()
        return (end - self.started_at).total_seconds()
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.id,
            "status": self.status.value,
            "progress": self.progress,
            "total_records": self.total_records,
            "processed_records": self.processed_records,
            "created_records": self.created_records,
            "failed_records": self.failed_records,
            "current_batch": self.current_batch,
            "total_batches": self.total_batches,
            "errors": self.errors[:50],  # Limit errors returned
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
        }


class JobManager:
    """
    Manages background import jobs.
    
    Thread-safe job creation, execution, and status tracking.
    """
    
    def __init__(self, max_workers: int = 2):
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
    
    def create_job(self, request_data: dict[str, Any] = None) -> Job:
        """Create a new pending job."""
        job_id = str(uuid.uuid4())[:8]
        job = Job(id=job_id, request_data=request_data or {})
        
        with self._lock:
            self._jobs[job_id] = job
        
        return job
    
    def get_job(self, job_id: str) -> Job | None:
        """Get job by ID."""
        with self._lock:
            return self._jobs.get(job_id)
    
    def list_jobs(self, status: JobStatus | None = None) -> list[Job]:
        """List all jobs, optionally filtered by status."""
        with self._lock:
            jobs = list(self._jobs.values())
        
        if status:
            jobs = [j for j in jobs if j.status == status]
        
        return sorted(jobs, key=lambda j: j.started_at or datetime.min, reverse=True)
    
    def start_job(
        self,
        job_id: str,
        task: Callable[[Job], None],
    ) -> bool:
        """Start a job in background."""
        job = self.get_job(job_id)
        if not job:
            return False
        
        if job.status != JobStatus.PENDING:
            return False
        
        job.status = JobStatus.RUNNING
        job.started_at = datetime.now()
        
        def run_task():
            try:
                task(job)
                if job.status == JobStatus.RUNNING:
                    job.status = JobStatus.COMPLETED
            except Exception as e:
                job.status = JobStatus.FAILED
                job.errors.append(str(e))
            finally:
                job.completed_at = datetime.now()
                job.progress = 100.0
        
        self._executor.submit(run_task)
        return True
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job."""
        job = self.get_job(job_id)
        if not job:
            return False
        
        if job.status in (JobStatus.PENDING, JobStatus.RUNNING):
            job.status = JobStatus.CANCELLED
            job.completed_at = datetime.now()
            return True
        
        return False
    
    def update_progress(
        self,
        job: Job,
        processed: int,
        total: int,
        created: int = 0,
        failed: int = 0,
        current_batch: int = 0,
        total_batches: int = 0,
    ) -> None:
        """Update job progress."""
        job.processed_records = processed
        job.total_records = total
        job.created_records = created
        job.failed_records = failed
        job.current_batch = current_batch
        job.total_batches = total_batches
        
        if total > 0:
            job.progress = (processed / total) * 100.0
    
    def add_error(self, job: Job, error: str) -> None:
        """Add error to job."""
        job.errors.append(error)
    
    def cleanup_old_jobs(self, max_age_hours: int = 24) -> int:
        """Remove completed jobs older than max_age_hours."""
        cutoff = datetime.now()
        removed = 0
        
        with self._lock:
            to_remove = []
            for job_id, job in self._jobs.items():
                if job.completed_at:
                    age = (cutoff - job.completed_at).total_seconds() / 3600
                    if age > max_age_hours:
                        to_remove.append(job_id)
            
            for job_id in to_remove:
                del self._jobs[job_id]
                removed += 1
        
        return removed


# Global job manager instance
job_manager = JobManager()
