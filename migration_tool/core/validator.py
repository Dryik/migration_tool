"""
Validation Engine

Pre-import validation using Pydantic schemas and custom rules.
Validates data before any Odoo API calls.
"""

from typing import Any, Callable
from dataclasses import dataclass, field
from enum import Enum

import pandas as pd
from pydantic import BaseModel, ValidationError


class ValidationSeverity(Enum):
    """Severity level for validation issues."""
    ERROR = "error"      # Blocks import
    WARNING = "warning"  # Allows import with caution
    INFO = "info"        # Informational only


@dataclass
class ValidationIssue:
    """A single validation issue."""
    
    row: int              # Source row number
    field: str | None     # Field with issue (None for row-level)
    message: str          # Human-readable message
    severity: ValidationSeverity
    value: Any = None     # The problematic value
    code: str = ""        # Error code for programmatic handling


@dataclass
class ValidationResult:
    """Result of validating a batch of records."""
    
    valid_records: list[dict[str, Any]] = field(default_factory=list)
    invalid_records: list[dict[str, Any]] = field(default_factory=list)
    issues: list[ValidationIssue] = field(default_factory=list)
    
    @property
    def is_valid(self) -> bool:
        """Check if all records are valid (no errors)."""
        return not any(i.severity == ValidationSeverity.ERROR for i in self.issues)
    
    @property
    def error_count(self) -> int:
        """Count of error-level issues."""
        return sum(1 for i in self.issues if i.severity == ValidationSeverity.ERROR)
    
    @property
    def warning_count(self) -> int:
        """Count of warning-level issues."""
        return sum(1 for i in self.issues if i.severity == ValidationSeverity.WARNING)
    
    def get_issues_for_row(self, row: int) -> list[ValidationIssue]:
        """Get all issues for a specific row."""
        return [i for i in self.issues if i.row == row]
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert issues to a DataFrame for reporting."""
        if not self.issues:
            return pd.DataFrame(columns=["row", "field", "severity", "message", "value"])
        
        return pd.DataFrame([
            {
                "row": i.row,
                "field": i.field or "",
                "severity": i.severity.value,
                "message": i.message,
                "value": str(i.value) if i.value is not None else "",
                "code": i.code,
            }
            for i in self.issues
        ])


class ValidationEngine:
    """
    Validates records before import to Odoo.
    
    Features:
    - Pydantic schema validation
    - Required field checking
    - Type validation
    - Many2one reference validation
    - Custom validation rules
    - Dependency checking
    
    Example:
        >>> engine = ValidationEngine()
        >>> result = engine.validate(records, PartnerSchema, required=["name"])
        >>> if not result.is_valid:
        ...     print(result.issues)
    """
    
    def __init__(self, odoo_client: Any | None = None):
        """
        Initialize validation engine.
        
        Args:
            odoo_client: Optional Odoo client for reference validation
        """
        self.client = odoo_client
        self._custom_rules: dict[str, Callable[[Any, dict], ValidationIssue | None]] = {}
        self._reference_cache: dict[str, set[str]] = {}
    
    def validate(
        self,
        records: list[dict[str, Any]],
        schema: type[BaseModel] | None = None,
        required: list[str] | None = None,
        validation_config: dict[str, Any] | None = None,
    ) -> ValidationResult:
        """
        Validate a list of records.
        
        Args:
            records: List of record dictionaries
            schema: Optional Pydantic schema for type validation
            required: List of required field names
            validation_config: Additional validation configuration
            
        Returns:
            ValidationResult with valid/invalid records and issues
        """
        result = ValidationResult()
        config = validation_config or {}
        required_fields = required or config.get("required", [])
        
        for record in records:
            row_num = record.get("__source_row__", 0)
            row_issues: list[ValidationIssue] = []
            
            # Check required fields
            for field_name in required_fields:
                value = record.get(field_name)
                if value is None or value == "" or (isinstance(value, str) and not value.strip()):
                    row_issues.append(ValidationIssue(
                        row=row_num,
                        field=field_name,
                        message=f"Required field '{field_name}' is missing or empty",
                        severity=ValidationSeverity.ERROR,
                        value=value,
                        code="REQUIRED_FIELD_MISSING",
                    ))
            
            # Pydantic schema validation
            if schema:
                schema_issues = self._validate_schema(record, schema, row_num)
                row_issues.extend(schema_issues)
            
            # Custom rules
            for rule_name, rule_func in self._custom_rules.items():
                try:
                    issue = rule_func(record.get(rule_name.split(".")[-1]), record)
                    if issue:
                        issue.row = row_num
                        row_issues.append(issue)
                except Exception as e:
                    row_issues.append(ValidationIssue(
                        row=row_num,
                        field=None,
                        message=f"Rule '{rule_name}' failed: {e}",
                        severity=ValidationSeverity.WARNING,
                        code="RULE_EXECUTION_ERROR",
                    ))
            
            # Add to results
            result.issues.extend(row_issues)
            
            has_errors = any(i.severity == ValidationSeverity.ERROR for i in row_issues)
            if has_errors:
                result.invalid_records.append(record)
            else:
                result.valid_records.append(record)
        
        return result
    
    def _validate_schema(
        self,
        record: dict[str, Any],
        schema: type[BaseModel],
        row_num: int,
    ) -> list[ValidationIssue]:
        """Validate a record against a Pydantic schema."""
        issues: list[ValidationIssue] = []
        
        # Filter out internal fields
        clean_record = {
            k: v for k, v in record.items()
            if not k.startswith("__")
        }
        
        try:
            schema.model_validate(clean_record)
        except ValidationError as e:
            for error in e.errors():
                field_path = ".".join(str(loc) for loc in error["loc"])
                issues.append(ValidationIssue(
                    row=row_num,
                    field=field_path,
                    message=error["msg"],
                    severity=ValidationSeverity.ERROR,
                    value=clean_record.get(field_path),
                    code=error["type"],
                ))
        
        return issues
    
    def validate_references(
        self,
        records: list[dict[str, Any]],
        reference_fields: dict[str, tuple[str, str]],
    ) -> ValidationResult:
        """
        Validate that Many2one references exist in Odoo.
        
        Args:
            records: List of records to validate
            reference_fields: Field -> (model, search_field) mapping
            
        Returns:
            ValidationResult with reference issues
        """
        result = ValidationResult()
        
        if not self.client:
            # Can't validate without client
            return result
        
        # Collect all unique values per reference field
        values_to_check: dict[str, set[str]] = {}
        for field_name in reference_fields:
            values_to_check[field_name] = set()
        
        for record in records:
            for field_name in reference_fields:
                value = record.get(field_name)
                if value and not isinstance(value, int):
                    values_to_check[field_name].add(str(value))
        
        # Check each reference field
        for field_name, values in values_to_check.items():
            if not values:
                continue
            
            ref_model, search_field = reference_fields[field_name]
            existing = self._get_existing_references(ref_model, search_field, values)
            missing = values - existing
            
            # Add issues for missing references
            for record in records:
                value = record.get(field_name)
                if value and str(value) in missing:
                    row_num = record.get("__source_row__", 0)
                    result.issues.append(ValidationIssue(
                        row=row_num,
                        field=field_name,
                        message=f"Reference not found in {ref_model}: '{value}'",
                        severity=ValidationSeverity.ERROR,
                        value=value,
                        code="REFERENCE_NOT_FOUND",
                    ))
                    result.invalid_records.append(record)
        
        # Valid records are those not in invalid
        invalid_ids = {id(r) for r in result.invalid_records}
        result.valid_records = [r for r in records if id(r) not in invalid_ids]
        
        return result
    
    def _get_existing_references(
        self,
        model: str,
        search_field: str,
        values: set[str],
    ) -> set[str]:
        """Check which reference values exist in Odoo."""
        cache_key = f"{model}:{search_field}"
        
        if cache_key in self._reference_cache:
            return self._reference_cache[cache_key] & values
        
        try:
            # Search for all values at once
            domain = [(search_field, "in", list(values))]
            results = self.client.search_read(model, domain, [search_field])
            
            existing = {str(r.get(search_field, "")) for r in results}
            
            # Cache results
            if cache_key not in self._reference_cache:
                self._reference_cache[cache_key] = set()
            self._reference_cache[cache_key].update(existing)
            
            return existing
        except Exception:
            # On error, assume all exist (don't block import)
            return values
    
    def register_rule(
        self,
        name: str,
        rule_func: Callable[[Any, dict], ValidationIssue | None],
    ) -> None:
        """
        Register a custom validation rule.
        
        Args:
            name: Rule name (usually field.rule_name)
            rule_func: Function(value, record) -> ValidationIssue or None
        """
        self._custom_rules[name] = rule_func
    
    def clear_cache(self) -> None:
        """Clear the reference validation cache."""
        self._reference_cache.clear()


# Built-in custom validators

def validate_email_format(value: Any, record: dict) -> ValidationIssue | None:
    """Validate email format."""
    if not value:
        return None
    
    import re
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    if not re.match(pattern, str(value)):
        return ValidationIssue(
            row=0,  # Will be set by engine
            field="email",
            message=f"Invalid email format: {value}",
            severity=ValidationSeverity.ERROR,
            value=value,
            code="INVALID_EMAIL",
        )
    return None


def validate_phone_format(value: Any, record: dict) -> ValidationIssue | None:
    """Validate phone number has enough digits."""
    if not value:
        return None
    
    import re
    digits = re.sub(r"[^\d]", "", str(value))
    if len(digits) < 7:
        return ValidationIssue(
            row=0,
            field="phone",
            message=f"Phone number too short: {value}",
            severity=ValidationSeverity.WARNING,
            value=value,
            code="SHORT_PHONE",
        )
    return None


def validate_positive_number(field_name: str) -> Callable:
    """Create a validator for positive numbers."""
    def validator(value: Any, record: dict) -> ValidationIssue | None:
        if value is None:
            return None
        try:
            num = float(value)
            if num < 0:
                return ValidationIssue(
                    row=0,
                    field=field_name,
                    message=f"{field_name} must be positive: {value}",
                    severity=ValidationSeverity.ERROR,
                    value=value,
                    code="NEGATIVE_VALUE",
                )
        except (ValueError, TypeError):
            pass
        return None
    return validator
