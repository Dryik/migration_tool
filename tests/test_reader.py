"""
Tests for the DataReader module.
"""

import pytest
import tempfile
from pathlib import Path

from migration_tool.core.reader import DataReader, DataReaderError


class TestDataReader:
    """Tests for DataReader class."""
    
    @pytest.fixture
    def reader(self):
        return DataReader()
    
    @pytest.fixture
    def sample_csv(self):
        """Create a temporary CSV file for testing."""
        content = """Name,Email,Phone
John Doe,john@example.com,+1-555-0100
Jane Smith,jane@example.com,+1-555-0200
Bob Wilson,bob@example.com,+1-555-0300
"""
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
            encoding="utf-8"
        ) as f:
            f.write(content)
            return Path(f.name)
    
    def test_read_csv_basic(self, reader, sample_csv):
        """Test basic CSV reading."""
        result = reader.read_file(sample_csv)
        
        assert result.total_rows == 3
        assert "Name" in result.columns
        assert "Email" in result.columns
        assert "Phone" in result.columns
        assert result.errors == []
    
    def test_read_csv_with_mapping(self, reader, sample_csv):
        """Test CSV reading with column mapping."""
        mapping = {
            "Name": "name",
            "Email": "email",
            "Phone": "phone",
        }
        
        result = reader.read_file(sample_csv, mapping=mapping)
        
        assert "name" in result.columns
        assert "email" in result.columns
        assert "phone" in result.columns
        assert "Name" not in result.columns
    
    def test_read_missing_file(self, reader):
        """Test reading non-existent file."""
        with pytest.raises(DataReaderError) as exc_info:
            reader.read_file("nonexistent.csv")
        
        assert "not found" in str(exc_info.value).lower()
    
    def test_read_unsupported_format(self, reader):
        """Test reading unsupported file format."""
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            with pytest.raises(DataReaderError) as exc_info:
                reader.read_file(f.name)
        
        assert "unsupported" in str(exc_info.value).lower()
    
    def test_source_tracking(self, reader, sample_csv):
        """Test that source file and row are tracked."""
        result = reader.read_file(sample_csv)
        
        assert "__source_file__" in result.data.columns
        assert "__source_row__" in result.data.columns
        
        # Row numbers should be 2, 3, 4 (1-indexed with header)
        row_nums = result.data["__source_row__"].tolist()
        assert row_nums == [2, 3, 4]
    
    def test_mapping_missing_columns_warning(self, reader, sample_csv):
        """Test warning when mapped column doesn't exist."""
        mapping = {
            "Name": "name",
            "NonExistent": "missing_field",
        }
        
        result = reader.read_file(sample_csv, mapping=mapping)
        
        assert len(result.errors) > 0 or len(result.warnings) > 0


class TestDataReaderEncodings:
    """Tests for encoding handling."""
    
    @pytest.fixture
    def reader(self):
        return DataReader()
    
    def test_utf8_encoding(self, reader):
        """Test UTF-8 encoded file."""
        content = """Name,City
José García,São Paulo
François Müller,Zürich
"""
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
            encoding="utf-8"
        ) as f:
            f.write(content)
            path = Path(f.name)
        
        result = reader.read_file(path)
        
        assert result.total_rows == 2
        assert "José García" in result.data["Name"].values
