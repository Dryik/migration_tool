"""
Tests for the Deduplicator module.
"""

import pytest

from migration_tool.core.deduplicator import Deduplicator, DedupeResult, DedupeAction


class TestDeduplicator:
    """Tests for Deduplicator class."""
    
    @pytest.fixture
    def deduper(self):
        return Deduplicator()  # No client for batch-only tests
    
    def test_find_batch_duplicates(self, deduper):
        """Test finding duplicates within a batch."""
        records = [
            {"__source_row__": 1, "name": "John Doe", "phone": "555-0100"},
            {"__source_row__": 2, "name": "Jane Smith", "phone": "555-0200"},
            {"__source_row__": 3, "name": "John Doe", "phone": "555-0100"},  # Duplicate
        ]
        
        result = deduper.find_duplicates(
            records,
            model="res.partner",
            keys=["name", "phone"],
            check_odoo=False,
            check_batch=True,
        )
        
        assert len(result.unique_records) == 2
        assert len(result.duplicate_records) == 1
        assert result.batch_duplicates == 1
    
    def test_no_duplicates(self, deduper):
        """Test when there are no duplicates."""
        records = [
            {"__source_row__": 1, "name": "John Doe", "email": "john@test.com"},
            {"__source_row__": 2, "name": "Jane Smith", "email": "jane@test.com"},
            {"__source_row__": 3, "name": "Bob Wilson", "email": "bob@test.com"},
        ]
        
        result = deduper.find_duplicates(
            records,
            model="res.partner",
            keys=["name", "email"],
            check_odoo=False,
        )
        
        assert len(result.unique_records) == 3
        assert len(result.duplicate_records) == 0
        assert result.total_duplicates == 0
    
    def test_case_insensitive_matching(self, deduper):
        """Test case-insensitive duplicate detection."""
        records = [
            {"__source_row__": 1, "name": "John Doe"},
            {"__source_row__": 2, "name": "JOHN DOE"},  # Same name, different case
        ]
        
        result = deduper.find_duplicates(
            records,
            model="res.partner",
            keys=["name"],
            case_sensitive=False,
            check_odoo=False,
        )
        
        assert len(result.duplicate_records) == 1
    
    def test_case_sensitive_matching(self, deduper):
        """Test case-sensitive duplicate detection."""
        records = [
            {"__source_row__": 1, "name": "John Doe"},
            {"__source_row__": 2, "name": "JOHN DOE"},  # Different if case-sensitive
        ]
        
        result = deduper.find_duplicates(
            records,
            model="res.partner",
            keys=["name"],
            case_sensitive=True,
            check_odoo=False,
        )
        
        assert len(result.unique_records) == 2
        assert len(result.duplicate_records) == 0
    
    def test_dedupe_strategy_skip(self, deduper):
        """Test skip strategy for duplicates."""
        records = [
            {"__source_row__": 1, "default_code": "SKU001"},
            {"__source_row__": 2, "default_code": "SKU001"},  # Duplicate
        ]
        
        result = deduper.find_duplicates(
            records,
            model="product.template",
            keys=["default_code"],
            strategy=DedupeAction.SKIP,
            check_odoo=False,
        )
        
        assert len(result.unique_records) == 1
        assert len(result.duplicate_records) == 1
        assert len(result.update_records) == 0
    
    def test_dedupe_strategy_create(self, deduper):
        """Test create strategy for duplicates."""
        records = [
            {"__source_row__": 1, "name": "Test"},
            {"__source_row__": 2, "name": "Test"},  # Duplicate but will be created
        ]
        
        result = deduper.find_duplicates(
            records,
            model="res.partner",
            keys=["name"],
            strategy=DedupeAction.CREATE,
            check_odoo=False,
        )
        
        # With CREATE strategy, duplicates go to unique_records
        assert len(result.unique_records) == 2
    
    def test_multiple_keys(self, deduper):
        """Test deduplication with multiple keys."""
        records = [
            {"__source_row__": 1, "name": "John", "email": "john@test.com"},
            {"__source_row__": 2, "name": "John", "email": "john2@test.com"},  # Same name, diff email
            {"__source_row__": 3, "name": "John", "email": "john@test.com"},  # True duplicate
        ]
        
        result = deduper.find_duplicates(
            records,
            model="res.partner",
            keys=["name", "email"],
            check_odoo=False,
        )
        
        assert len(result.unique_records) == 2
        assert len(result.duplicate_records) == 1
    
    def test_empty_key_values(self, deduper):
        """Test handling of empty key values."""
        records = [
            {"__source_row__": 1, "name": "John", "phone": ""},
            {"__source_row__": 2, "name": "John", "phone": ""},  # Both have empty phone
        ]
        
        # Empty values should still match as duplicates
        result = deduper.find_duplicates(
            records,
            model="res.partner",
            keys=["name", "phone"],
            check_odoo=False,
        )
        
        assert len(result.duplicate_records) == 1
    
    def test_duplicate_report(self, deduper):
        """Test generating duplicate report."""
        records = [
            {"__source_row__": 1, "name": "John"},
            {"__source_row__": 2, "name": "John"},
        ]
        
        result = deduper.find_duplicates(
            records,
            model="res.partner",
            keys=["name"],
            check_odoo=False,
        )
        
        report = deduper.get_duplicate_report(result)
        
        assert "DUPLICATE DETECTION REPORT" in report
        assert "Total records processed" in report
