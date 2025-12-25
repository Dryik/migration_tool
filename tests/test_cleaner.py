"""
Tests for the DataCleaner module.
"""

import pytest

from migration_tool.core.cleaner import DataCleaner


class TestDataCleaner:
    """Tests for DataCleaner class."""
    
    @pytest.fixture
    def cleaner(self):
        return DataCleaner(default_country_code="+1")
    
    # Phone normalization tests
    def test_normalize_phone_basic(self, cleaner):
        """Test basic phone normalization."""
        assert cleaner.normalize_phone("555-123-4567") == "+15551234567"
        assert cleaner.normalize_phone("(555) 123-4567") == "+15551234567"
    
    def test_normalize_phone_with_country(self, cleaner):
        """Test phone with existing country code."""
        assert cleaner.normalize_phone("+44 20 7946 0958") == "+442079460958"
        assert cleaner.normalize_phone("+1-555-123-4567") == "+15551234567"
    
    def test_normalize_phone_empty(self, cleaner):
        """Test empty phone handling."""
        assert cleaner.normalize_phone(None) is None
        assert cleaner.normalize_phone("") is None
        assert cleaner.normalize_phone("   ") is None
    
    # Email normalization tests
    def test_normalize_email_basic(self, cleaner):
        """Test basic email normalization."""
        assert cleaner.normalize_email("John.Doe@Example.COM") == "john.doe@example.com"
        assert cleaner.normalize_email("  user@domain.com  ") == "user@domain.com"
    
    def test_normalize_email_empty(self, cleaner):
        """Test empty email handling."""
        assert cleaner.normalize_email(None) is None
        assert cleaner.normalize_email("") is None
    
    # Date normalization tests
    def test_normalize_date_iso(self, cleaner):
        """Test ISO date format."""
        assert cleaner.normalize_date("2024-12-25") == "2024-12-25"
    
    def test_normalize_date_european(self, cleaner):
        """Test European date format."""
        assert cleaner.normalize_date("25/12/2024") == "2024-12-25"
    
    def test_normalize_date_american(self, cleaner):
        """Test American date format."""
        assert cleaner.normalize_date("12/25/2024") == "2024-12-25"
    
    def test_normalize_date_empty(self, cleaner):
        """Test empty date handling."""
        assert cleaner.normalize_date(None) is None
        assert cleaner.normalize_date("") is None
    
    # Boolean normalization tests
    def test_normalize_boolean_true(self, cleaner):
        """Test true value variations."""
        assert cleaner.normalize_boolean("yes") is True
        assert cleaner.normalize_boolean("Yes") is True
        assert cleaner.normalize_boolean("YES") is True
        assert cleaner.normalize_boolean("true") is True
        assert cleaner.normalize_boolean("1") is True
        assert cleaner.normalize_boolean(1) is True
        assert cleaner.normalize_boolean("y") is True
    
    def test_normalize_boolean_false(self, cleaner):
        """Test false value variations."""
        assert cleaner.normalize_boolean("no") is False
        assert cleaner.normalize_boolean("No") is False
        assert cleaner.normalize_boolean("false") is False
        assert cleaner.normalize_boolean("0") is False
        assert cleaner.normalize_boolean(0) is False
        assert cleaner.normalize_boolean("n") is False
    
    def test_normalize_boolean_invalid(self, cleaner):
        """Test invalid boolean values."""
        assert cleaner.normalize_boolean("maybe") is None
        assert cleaner.normalize_boolean(None) is None
    
    # Numeric normalization tests
    def test_normalize_numeric_basic(self, cleaner):
        """Test basic numeric normalization."""
        assert cleaner.normalize_numeric("123.45") == 123.45
        assert cleaner.normalize_numeric("123") == 123.0
        assert cleaner.normalize_numeric(123) == 123.0
    
    def test_normalize_numeric_thousand_sep(self, cleaner):
        """Test numbers with thousand separators."""
        assert cleaner.normalize_numeric("1,234.56") == 1234.56
        assert cleaner.normalize_numeric("1,234,567.89") == 1234567.89
    
    def test_normalize_numeric_european(self, cleaner):
        """Test European number format."""
        assert cleaner.normalize_numeric("1.234,56") == 1234.56
    
    def test_normalize_numeric_empty(self, cleaner):
        """Test empty numeric handling."""
        assert cleaner.normalize_numeric(None) is None
        assert cleaner.normalize_numeric("") is None
    
    # UoM normalization tests
    def test_normalize_uom_aliases(self, cleaner):
        """Test UoM alias mapping."""
        assert cleaner.normalize_uom("pcs") == "Units"
        assert cleaner.normalize_uom("piece") == "Units"
        assert cleaner.normalize_uom("kg") == "kg"
        assert cleaner.normalize_uom("kilogram") == "kg"
    
    def test_normalize_uom_unknown(self, cleaner):
        """Test unknown UoM handling."""
        assert cleaner.normalize_uom("custom unit") == "Custom Unit"
    
    # Whitespace cleaning tests
    def test_clean_whitespace(self, cleaner):
        """Test whitespace normalization."""
        assert cleaner.clean_whitespace("  hello  ") == "hello"
        assert cleaner.clean_whitespace("hello   world") == "hello world"
        assert cleaner.clean_whitespace("\n\thello\n") == "hello"
    
    def test_clean_whitespace_empty(self, cleaner):
        """Test empty string handling."""
        assert cleaner.clean_whitespace("") is None
        assert cleaner.clean_whitespace("   ") is None
        assert cleaner.clean_whitespace(None) is None
    
    # HTML stripping tests
    def test_strip_html(self, cleaner):
        """Test HTML tag removal."""
        assert cleaner.strip_html("<p>Hello</p>") == "Hello"
        assert cleaner.strip_html("<b>Bold</b> text") == "Bold text"
        assert cleaner.strip_html("No&nbsp;break") == "No break"
