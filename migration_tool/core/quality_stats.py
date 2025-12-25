"""
Data Quality Statistics

Provides quality analysis for import data including validation stats,
duplicate detection, and reference resolution tracking.
"""

from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


class IssueType(Enum):
    VALIDATION_ERROR = "validation_error"
    VALIDATION_WARNING = "validation_warning"
    DUPLICATE = "duplicate"
    MISSING_REQUIRED = "missing_required"
    REFERENCE_UNRESOLVED = "reference_unresolved"
    REFERENCE_RESOLVED = "reference_resolved"


@dataclass
class QualityIssue:
    """Represents a single quality issue in the data."""
    issue_type: IssueType
    row_index: int
    field_name: str
    message: str
    original_value: str = ""
    suggested_value: Optional[str] = None
    duplicate_of: Optional[int] = None  # Row index of duplicate


@dataclass
class QualityStats:
    """Aggregated quality statistics for import data."""
    total_rows: int = 0
    valid_rows: int = 0
    error_rows: int = 0
    warning_rows: int = 0
    duplicate_rows: int = 0
    
    # Field-level stats
    validation_errors: int = 0
    validation_warnings: int = 0
    missing_required: int = 0
    references_resolved: int = 0
    references_unresolved: int = 0
    
    # Detailed issues
    issues: list[QualityIssue] = field(default_factory=list)
    
    # Duplicate groups (list of row indices that are duplicates)
    duplicate_groups: list[list[int]] = field(default_factory=list)
    
    @property
    def has_errors(self) -> bool:
        return self.error_rows > 0 or self.validation_errors > 0
    
    @property
    def has_warnings(self) -> bool:
        return self.warning_rows > 0 or self.validation_warnings > 0
    
    @property
    def quality_score(self) -> float:
        """Calculate a quality score from 0-100."""
        if self.total_rows == 0:
            return 100.0
        
        # Deduct points for issues
        score = 100.0
        score -= (self.error_rows / self.total_rows) * 50  # Errors cost more
        score -= (self.warning_rows / self.total_rows) * 20
        score -= (self.duplicate_rows / self.total_rows) * 10
        
        return max(0, min(100, score))
    
    def add_issue(self, issue: QualityIssue):
        """Add an issue and update counts."""
        self.issues.append(issue)
        
        if issue.issue_type == IssueType.VALIDATION_ERROR:
            self.validation_errors += 1
        elif issue.issue_type == IssueType.VALIDATION_WARNING:
            self.validation_warnings += 1
        elif issue.issue_type == IssueType.DUPLICATE:
            self.duplicate_rows += 1
        elif issue.issue_type == IssueType.MISSING_REQUIRED:
            self.missing_required += 1
        elif issue.issue_type == IssueType.REFERENCE_UNRESOLVED:
            self.references_unresolved += 1
        elif issue.issue_type == IssueType.REFERENCE_RESOLVED:
            self.references_resolved += 1
    
    def get_issues_for_row(self, row_index: int) -> list[QualityIssue]:
        """Get all issues for a specific row."""
        return [i for i in self.issues if i.row_index == row_index]
    
    def get_issues_by_type(self, issue_type: IssueType) -> list[QualityIssue]:
        """Get all issues of a specific type."""
        return [i for i in self.issues if i.issue_type == issue_type]


class QualityAnalyzer:
    """Analyzes data quality for import records."""
    
    def __init__(self, dedup_fields: list[str] = None):
        self.dedup_fields = dedup_fields or ["name", "email", "default_code", "ref"]
    
    def analyze(
        self,
        records: list[dict],
        required_fields: list[str] = None,
        validation_results: list = None,
    ) -> QualityStats:
        """Analyze a list of records and return quality stats."""
        stats = QualityStats(total_rows=len(records))
        required = required_fields or []
        
        # Track rows with issues
        rows_with_errors = set()
        rows_with_warnings = set()
        
        # Check required fields
        for i, record in enumerate(records):
            for field_name in required:
                value = record.get(field_name)
                if value is None or str(value).strip() == "":
                    stats.add_issue(QualityIssue(
                        issue_type=IssueType.MISSING_REQUIRED,
                        row_index=i,
                        field_name=field_name,
                        message=f"Missing required field: {field_name}",
                    ))
                    rows_with_errors.add(i)
        
        # Add validation results
        if validation_results:
            for result in validation_results:
                if hasattr(result, 'severity'):
                    from migration_tool.core.validation_rules import ValidationSeverity
                    if result.severity == ValidationSeverity.ERROR:
                        stats.add_issue(QualityIssue(
                            issue_type=IssueType.VALIDATION_ERROR,
                            row_index=result.row_index,
                            field_name=result.field_name,
                            message=result.message,
                            original_value=result.original_value,
                            suggested_value=result.suggested_value,
                        ))
                        rows_with_errors.add(result.row_index)
                    elif result.severity == ValidationSeverity.WARNING:
                        stats.add_issue(QualityIssue(
                            issue_type=IssueType.VALIDATION_WARNING,
                            row_index=result.row_index,
                            field_name=result.field_name,
                            message=result.message,
                            original_value=result.original_value,
                            suggested_value=result.suggested_value,
                        ))
                        rows_with_warnings.add(result.row_index)
        
        # Detect duplicates
        self._detect_duplicates(records, stats)
        
        # Calculate row-level stats
        stats.error_rows = len(rows_with_errors)
        stats.warning_rows = len(rows_with_warnings - rows_with_errors)
        stats.valid_rows = stats.total_rows - stats.error_rows - stats.duplicate_rows
        
        return stats
    
    def _detect_duplicates(self, records: list[dict], stats: QualityStats):
        """Detect duplicate records based on dedup fields."""
        seen = {}  # {key_value: first_row_index}
        
        for i, record in enumerate(records):
            for field_name in self.dedup_fields:
                if field_name in record:
                    value = str(record[field_name]).strip().lower()
                    if value:
                        key = f"{field_name}:{value}"
                        if key in seen:
                            # Found duplicate
                            stats.add_issue(QualityIssue(
                                issue_type=IssueType.DUPLICATE,
                                row_index=i,
                                field_name=field_name,
                                message=f"Duplicate of row {seen[key] + 2}",
                                original_value=value,
                                duplicate_of=seen[key],
                            ))
                            break  # Only count once per row
                        else:
                            seen[key] = i
