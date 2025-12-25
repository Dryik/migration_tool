"""
Data Reader

Handles ingestion of CSV and Excel files with column mapping,
encoding detection, and source tracking.
"""

import csv
from pathlib import Path
from typing import Any, Literal
from dataclasses import dataclass, field
import io

import pandas as pd


class DataReaderError(Exception):
    """Raised when data reading fails."""
    pass


@dataclass
class SourceInfo:
    """Tracks the source of each record for audit purposes."""
    
    file_path: str
    sheet_name: str | None
    row_number: int  # 1-indexed, accounting for header


@dataclass
class ReadResult:
    """Result of reading a data file."""
    
    data: pd.DataFrame
    source_file: str
    sheet_name: str | None
    total_rows: int
    columns: list[str]
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class DataReader:
    """
    Reads and maps data from CSV and Excel files.
    
    Features:
    - UTF-8 safe with encoding detection
    - Column mapping from configuration
    - Source tracking for audit
    - Basic type inference
    
    Example:
        >>> reader = DataReader()
        >>> result = reader.read_file(
        ...     "customers.xlsx",
        ...     mapping={"Customer Name": "name", "Phone": "phone"},
        ...     sheet="Sheet1"
        ... )
        >>> print(result.data.head())
    """
    
    # Common encodings to try
    ENCODINGS = ["utf-8", "utf-8-sig", "latin-1", "cp1252", "iso-8859-1"]
    
    # Supported file extensions
    SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls"}
    
    def read_file(
        self,
        file_path: str | Path,
        mapping: dict[str, str] | None = None,
        sheet: str | None = None,
        skip_rows: int = 0,
        encoding: str | None = None,
    ) -> ReadResult:
        """
        Read a data file and apply column mapping.
        
        Args:
            file_path: Path to CSV or Excel file
            mapping: Column name to Odoo field mapping
            sheet: Sheet name for Excel files
            skip_rows: Number of rows to skip at the start
            encoding: Force specific encoding (auto-detect if None)
            
        Returns:
            ReadResult with data and metadata
            
        Raises:
            DataReaderError: If file cannot be read
        """
        path = Path(file_path)
        
        if not path.exists():
            raise DataReaderError(f"File not found: {path}")
        
        suffix = path.suffix.lower()
        if suffix not in self.SUPPORTED_EXTENSIONS:
            raise DataReaderError(
                f"Unsupported file type: {suffix}. "
                f"Supported: {', '.join(self.SUPPORTED_EXTENSIONS)}"
            )
        
        # Read based on file type
        if suffix == ".csv":
            df, detected_encoding = self._read_csv(path, encoding, skip_rows)
            sheet_name = None
        else:
            df = self._read_excel(path, sheet, skip_rows)
            sheet_name = sheet
            detected_encoding = None
        
        errors: list[str] = []
        warnings: list[str] = []
        
        # Apply column mapping
        if mapping:
            df, map_errors, map_warnings = self._apply_mapping(df, mapping)
            errors.extend(map_errors)
            warnings.extend(map_warnings)
        
        # Add source tracking column
        df["__source_file__"] = str(path)
        df["__source_row__"] = range(2 + skip_rows, len(df) + 2 + skip_rows)  # 1-indexed with header
        
        if detected_encoding and detected_encoding != "utf-8":
            warnings.append(f"File encoding detected as {detected_encoding}")
        
        return ReadResult(
            data=df,
            source_file=str(path),
            sheet_name=sheet_name,
            total_rows=len(df),
            columns=list(df.columns),
            errors=errors,
            warnings=warnings,
        )
    
    def _read_csv(
        self,
        path: Path,
        encoding: str | None,
        skip_rows: int,
    ) -> tuple[pd.DataFrame, str]:
        """Read CSV file with encoding detection."""
        if encoding:
            try:
                df = pd.read_csv(
                    path,
                    encoding=encoding,
                    skiprows=skip_rows,
                    dtype=str,  # Read all as strings initially
                    keep_default_na=False,  # Don't convert empty to NaN
                )
                return df, encoding
            except UnicodeDecodeError:
                raise DataReaderError(f"Cannot decode file with encoding: {encoding}")
        
        # Try encodings in order
        for enc in self.ENCODINGS:
            try:
                df = pd.read_csv(
                    path,
                    encoding=enc,
                    skiprows=skip_rows,
                    dtype=str,
                    keep_default_na=False,
                )
                return df, enc
            except UnicodeDecodeError:
                continue
        
        raise DataReaderError(
            f"Cannot decode CSV file. Tried encodings: {', '.join(self.ENCODINGS)}"
        )
    
    def _read_excel(
        self,
        path: Path,
        sheet: str | None,
        skip_rows: int,
    ) -> pd.DataFrame:
        """Read Excel file."""
        try:
            if sheet:
                df = pd.read_excel(
                    path,
                    sheet_name=sheet,
                    skiprows=skip_rows,
                    dtype=str,
                    keep_default_na=False,
                    engine="openpyxl",
                )
            else:
                # Read first sheet
                df = pd.read_excel(
                    path,
                    sheet_name=0,
                    skiprows=skip_rows,
                    dtype=str,
                    keep_default_na=False,
                    engine="openpyxl",
                )
            return df
        except ValueError as e:
            if "Worksheet" in str(e):
                raise DataReaderError(f"Sheet '{sheet}' not found in {path}")
            raise DataReaderError(f"Error reading Excel file: {e}")
        except Exception as e:
            raise DataReaderError(f"Error reading Excel file: {e}")
    
    def _apply_mapping(
        self,
        df: pd.DataFrame,
        mapping: dict[str, str],
    ) -> tuple[pd.DataFrame, list[str], list[str]]:
        """
        Apply column mapping to dataframe.
        
        Args:
            df: Source dataframe
            mapping: source_column -> target_field mapping
            
        Returns:
            Tuple of (mapped dataframe, errors, warnings)
        """
        errors: list[str] = []
        warnings: list[str] = []
        
        # Check for missing source columns
        df_columns = set(df.columns)
        missing_columns = set(mapping.keys()) - df_columns
        
        if missing_columns:
            errors.append(
                f"Source columns not found: {', '.join(sorted(missing_columns))}"
            )
        
        # Create rename mapping for existing columns
        valid_mapping = {
            src: tgt for src, tgt in mapping.items()
            if src in df_columns
        }
        
        # Rename columns
        df = df.rename(columns=valid_mapping)
        
        # Check for unmapped columns
        mapped_sources = set(valid_mapping.keys())
        unmapped = df_columns - mapped_sources - {"__source_file__", "__source_row__"}
        
        if unmapped:
            warnings.append(
                f"Unmapped columns (will be ignored): {', '.join(sorted(unmapped))}"
            )
        
        # Keep only mapped columns plus source tracking
        target_columns = list(valid_mapping.values())
        available_columns = [c for c in target_columns if c in df.columns]
        
        # Also keep source tracking if present
        for col in ["__source_file__", "__source_row__"]:
            if col in df.columns:
                available_columns.append(col)
        
        return df[available_columns], errors, warnings
    
    def get_sheet_names(self, file_path: str | Path) -> list[str]:
        """Get list of sheet names from an Excel file."""
        path = Path(file_path)
        
        if path.suffix.lower() not in {".xlsx", ".xls"}:
            raise DataReaderError("Sheet names only available for Excel files")
        
        try:
            xl = pd.ExcelFile(path, engine="openpyxl")
            return xl.sheet_names
        except Exception as e:
            raise DataReaderError(f"Cannot read Excel file: {e}")
    
    def preview(
        self,
        file_path: str | Path,
        rows: int = 5,
        sheet: str | None = None,
    ) -> pd.DataFrame:
        """
        Preview first N rows of a file.
        
        Useful for configuration and mapping setup.
        """
        result = self.read_file(file_path, sheet=sheet)
        return result.data.head(rows)
    
    def detect_delimiter(self, file_path: str | Path) -> str:
        """
        Detect CSV delimiter (comma, semicolon, tab, pipe).
        """
        path = Path(file_path)
        
        if path.suffix.lower() != ".csv":
            return ","
        
        try:
            with open(path, "r", encoding="utf-8") as f:
                sample = f.read(8192)
            
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample, delimiters=",;\t|")
            return dialect.delimiter
        except Exception:
            return ","  # Default to comma
