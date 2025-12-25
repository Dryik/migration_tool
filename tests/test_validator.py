"""
Tests for the ValidationEngine module.
"""

import pytest

from migration_tool.core.validator import (
    ValidationEngine,
    ValidationResult,
    ValidationSeverity,
    ValidationIssue,
)
from migration_tool.schemas.partner import PartnerCreateSchema


class TestValidationEngine:
    """Tests for ValidationEngine class."""
    
    @pytest.fixture
    def engine(self):
        return ValidationEngine()
    
    def test_validate_required_fields(self, engine):
        """Test required field validation."""
        records = [
            {"__source_row__": 1, "name": "John Doe", "email": "john@example.com"},
            {"__source_row__": 2, "name": "", "email": "jane@example.com"},
            {"__source_row__": 3, "email": "bob@example.com"},  # Missing name
        ]
        
        result = engine.validate(records, required=["name"])
        
        assert len(result.valid_records) == 1
        assert len(result.invalid_records) == 2
        assert result.error_count == 2
    
    def test_validate_empty_required(self, engine):
        """Test that empty strings are treated as missing."""
        records = [
            {"__source_row__": 1, "name": "   "},  # Whitespace only
            {"__source_row__": 2, "name": None},
            {"__source_row__": 3, "name": "Valid Name"},
        ]
        
        result = engine.validate(records, required=["name"])
        
        assert len(result.valid_records) == 1
        assert len(result.invalid_records) == 2
    
    def test_validate_with_schema(self, engine):
        """Test validation with Pydantic schema."""
        records = [
            {"__source_row__": 1, "name": "John Doe", "email": "john@example.com"},
            {"__source_row__": 2, "name": "Jane Doe", "email": "invalid-email"},
        ]
        
        result = engine.validate(records, schema=PartnerCreateSchema)
        
        # First record should be valid, second has invalid email
        assert len(result.valid_records) >= 1
    
    def test_validate_returns_issues(self, engine):
        """Test that validation returns issue details."""
        records = [
            {"__source_row__": 5, "email": "test@test.com"},  # Missing required name
        ]
        
        result = engine.validate(records, required=["name"])
        
        assert len(result.issues) > 0
        issue = result.issues[0]
        assert issue.row == 5
        assert issue.field == "name"
        assert issue.severity == ValidationSeverity.ERROR
    
    def test_validation_result_is_valid(self, engine):
        """Test is_valid property."""
        records = [
            {"__source_row__": 1, "name": "Test"},
        ]
        
        result = engine.validate(records, required=["name"])
        
        assert result.is_valid is True
        assert result.error_count == 0
    
    def test_validation_result_to_dataframe(self, engine):
        """Test converting issues to DataFrame."""
        records = [
            {"__source_row__": 1},  # Missing name
            {"__source_row__": 2},  # Missing name
        ]
        
        result = engine.validate(records, required=["name"])
        df = result.to_dataframe()
        
        assert len(df) == 2
        assert "row" in df.columns
        assert "field" in df.columns
        assert "message" in df.columns


class TestValidationIssue:
    """Tests for ValidationIssue dataclass."""
    
    def test_issue_creation(self):
        """Test creating validation issue."""
        issue = ValidationIssue(
            row=10,
            field="email",
            message="Invalid email format",
            severity=ValidationSeverity.ERROR,
            value="bad-email",
            code="INVALID_EMAIL",
        )
        
        assert issue.row == 10
        assert issue.field == "email"
        assert issue.severity == ValidationSeverity.ERROR
    
    def test_issue_severity_levels(self):
        """Test different severity levels."""
        assert ValidationSeverity.ERROR.value == "error"
        assert ValidationSeverity.WARNING.value == "warning"
        assert ValidationSeverity.INFO.value == "info"
