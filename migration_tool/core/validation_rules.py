"""
Field Validation Rules

Provides regex-based validation for common field types like email, phone, tax ID.
"""

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable


class ValidationSeverity(Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationResult:
    """Result of validating a single field value."""
    is_valid: bool
    severity: ValidationSeverity = ValidationSeverity.INFO
    message: str = ""
    field_name: str = ""
    row_index: int = 0
    original_value: str = ""
    suggested_value: Optional[str] = None


# Common field validation patterns
FIELD_PATTERNS = {
    # Email patterns
    "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    "email_from": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    
    # Phone patterns (international format)
    "phone": r"^[\d\s\+\-\.\(\)]{7,20}$",
    "mobile": r"^[\d\s\+\-\.\(\)]{7,20}$",
    
    # Tax/VAT ID patterns
    "vat": r"^[A-Z]{2}[A-Z0-9]{2,13}$",
    "tax_id": r"^[A-Z0-9\-]{5,20}$",
    
    # Reference/Code patterns
    "ref": r"^[A-Za-z0-9\-_]+$",
    "default_code": r"^[A-Za-z0-9\-_\.]+$",
    "barcode": r"^[A-Za-z0-9\-]+$",
    
    # Website/URL
    "website": r"^https?://[^\s]+$",
    
    # Postal code (generic)
    "zip": r"^[A-Za-z0-9\s\-]{3,10}$",
}


class FieldValidator:
    """Validates field values against patterns and custom rules."""
    
    def __init__(self):
        self.patterns = FIELD_PATTERNS.copy()
        self.custom_validators: dict[str, Callable] = {}
    
    def add_pattern(self, field_name: str, pattern: str):
        """Add or override a validation pattern for a field."""
        self.patterns[field_name] = pattern
    
    def add_validator(self, field_name: str, validator: Callable[[str], ValidationResult]):
        """Add a custom validator function for a field."""
        self.custom_validators[field_name] = validator
    
    def validate_field(
        self,
        field_name: str,
        value: any,
        row_index: int = 0,
    ) -> ValidationResult:
        """Validate a single field value."""
        str_value = str(value).strip() if value is not None else ""
        
        # Empty values - just info, not error
        if not str_value:
            return ValidationResult(
                is_valid=True,
                severity=ValidationSeverity.INFO,
                field_name=field_name,
                row_index=row_index,
                original_value=str_value,
            )
        
        # Custom validator takes priority
        if field_name in self.custom_validators:
            result = self.custom_validators[field_name](str_value)
            result.field_name = field_name
            result.row_index = row_index
            return result
        
        # Pattern-based validation
        if field_name in self.patterns:
            pattern = self.patterns[field_name]
            if re.match(pattern, str_value):
                return ValidationResult(
                    is_valid=True,
                    field_name=field_name,
                    row_index=row_index,
                    original_value=str_value,
                )
            else:
                return ValidationResult(
                    is_valid=False,
                    severity=ValidationSeverity.WARNING,
                    message=f"Invalid format for {field_name}",
                    field_name=field_name,
                    row_index=row_index,
                    original_value=str_value,
                )
        
        # No pattern defined - always valid
        return ValidationResult(
            is_valid=True,
            field_name=field_name,
            row_index=row_index,
            original_value=str_value,
        )
    
    def validate_record(
        self,
        record: dict,
        row_index: int = 0,
    ) -> list[ValidationResult]:
        """Validate all fields in a record."""
        results = []
        for field_name, value in record.items():
            result = self.validate_field(field_name, value, row_index)
            if not result.is_valid:
                results.append(result)
        return results
    
    def validate_records(
        self,
        records: list[dict],
    ) -> list[ValidationResult]:
        """Validate a list of records."""
        all_results = []
        for i, record in enumerate(records):
            results = self.validate_record(record, row_index=i)
            all_results.extend(results)
        return all_results


def validate_email(value: str) -> ValidationResult:
    """Custom email validator with suggestions."""
    pattern = FIELD_PATTERNS["email"]
    if re.match(pattern, value):
        return ValidationResult(is_valid=True, original_value=value)
    
    # Common fixes
    suggested = value.lower().strip()
    suggested = re.sub(r'\s+', '', suggested)  # Remove spaces
    
    if re.match(pattern, suggested):
        return ValidationResult(
            is_valid=False,
            severity=ValidationSeverity.WARNING,
            message="Email has formatting issues",
            original_value=value,
            suggested_value=suggested,
        )
    
    return ValidationResult(
        is_valid=False,
        severity=ValidationSeverity.ERROR,
        message="Invalid email format",
        original_value=value,
    )


def validate_phone(value: str) -> ValidationResult:
    """Custom phone validator that normalizes formats."""
    # Remove common non-digit chars for check
    digits = re.sub(r'[^\d]', '', value)
    
    if len(digits) < 7:
        return ValidationResult(
            is_valid=False,
            severity=ValidationSeverity.ERROR,
            message="Phone number too short",
            original_value=value,
        )
    
    if len(digits) > 15:
        return ValidationResult(
            is_valid=False,
            severity=ValidationSeverity.WARNING,
            message="Phone number unusually long",
            original_value=value,
        )
    
    return ValidationResult(is_valid=True, original_value=value)
