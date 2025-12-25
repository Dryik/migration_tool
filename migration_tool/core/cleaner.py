"""
Data Cleaner

Provides normalization functions for cleaning messy data before import.
"""

import re
from typing import Any, Callable
from datetime import datetime, date
from decimal import Decimal, InvalidOperation

import pandas as pd


class CleanerError(Exception):
    """Raised when cleaning fails."""
    pass


class DataCleaner:
    """
    Cleans and normalizes data for Odoo import.
    
    Features:
    - Whitespace normalization
    - Phone number standardization
    - Email validation and normalization
    - Date parsing from various formats
    - Currency and numeric handling
    - Boolean conversion
    - UoM normalization
    
    Example:
        >>> cleaner = DataCleaner()
        >>> df = cleaner.clean(df, transforms={"phone": "normalize_phone"})
    """
    
    # Boolean value mappings
    TRUE_VALUES = {"true", "yes", "1", "y", "t", "si", "oui", "ja", "نعم", "是"}
    FALSE_VALUES = {"false", "no", "0", "n", "f", "non", "nein", "لا", "否"}
    
    # Common date formats to try
    DATE_FORMATS = [
        "%Y-%m-%d",       # ISO format
        "%d/%m/%Y",       # European
        "%m/%d/%Y",       # American
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%Y/%m/%d",
        "%d.%m.%Y",       # German
        "%d %b %Y",       # 25 Dec 2024
        "%d %B %Y",       # 25 December 2024
        "%Y%m%d",         # Compact
    ]
    
    # UoM aliases
    UOM_ALIASES: dict[str, str] = {
        # Units
        "pcs": "Units",
        "pc": "Units",
        "piece": "Units",
        "pieces": "Units",
        "unit": "Units",
        "units": "Units",
        "ea": "Units",
        "each": "Units",
        # Weight
        "kg": "kg",
        "kgs": "kg",
        "kilogram": "kg",
        "kilograms": "kg",
        "g": "g",
        "gram": "g",
        "grams": "g",
        "lb": "lb(s)",
        "lbs": "lb(s)",
        "pound": "lb(s)",
        "pounds": "lb(s)",
        "oz": "oz",
        "ounce": "oz",
        "ounces": "oz",
        # Length
        "m": "m",
        "meter": "m",
        "meters": "m",
        "metre": "m",
        "metres": "m",
        "cm": "cm",
        "centimeter": "cm",
        "centimeters": "cm",
        "mm": "mm",
        "millimeter": "mm",
        "ft": "ft",
        "foot": "ft",
        "feet": "ft",
        "in": "inch(es)",
        "inch": "inch(es)",
        "inches": "inch(es)",
        # Volume
        "l": "Liters",
        "liter": "Liters",
        "liters": "Liters",
        "litre": "Liters",
        "litres": "Liters",
        "ml": "Milliliters",
        "milliliter": "Milliliters",
        "gal": "Gallons",
        "gallon": "Gallons",
        # Time
        "h": "Hours",
        "hr": "Hours",
        "hrs": "Hours",
        "hour": "Hours",
        "hours": "Hours",
        "day": "Days",
        "days": "Days",
    }
    
    def __init__(self, default_country_code: str = "+1"):
        """
        Initialize cleaner.
        
        Args:
            default_country_code: Default country code for phone normalization
        """
        self.default_country_code = default_country_code
        
        # Register transform functions
        self._transforms: dict[str, Callable[[Any], Any]] = {
            "normalize_phone": self.normalize_phone,
            "normalize_email": self.normalize_email,
            "normalize_date": self.normalize_date,
            "normalize_boolean": self.normalize_boolean,
            "normalize_numeric": self.normalize_numeric,
            "normalize_currency": self.normalize_currency,
            "normalize_uom": self.normalize_uom,
            "clean_whitespace": self.clean_whitespace,
            "uppercase": lambda x: str(x).upper() if x else x,
            "lowercase": lambda x: str(x).lower() if x else x,
            "titlecase": lambda x: str(x).title() if x else x,
            "strip_html": self.strip_html,
        }
    
    def clean(
        self,
        df: pd.DataFrame,
        transforms: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """
        Clean a dataframe by applying transforms to specified columns.
        
        Args:
            df: Dataframe to clean
            transforms: Column to transform function mapping
            
        Returns:
            Cleaned dataframe
        """
        result = df.copy()
        
        # Always clean whitespace on all string columns
        for col in result.columns:
            if result[col].dtype == object:
                result[col] = result[col].apply(self._safe_strip)
        
        # Apply specified transforms
        if transforms:
            for column, transform_name in transforms.items():
                if column not in result.columns:
                    continue
                
                transform_func = self._transforms.get(transform_name)
                if not transform_func:
                    raise CleanerError(f"Unknown transform: {transform_name}")
                
                result[column] = result[column].apply(
                    lambda x: self._safe_transform(x, transform_func)
                )
        
        return result
    
    def _safe_strip(self, value: Any) -> Any:
        """Safely strip whitespace from a value."""
        if pd.isna(value) or value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            # Normalize multiple spaces
            stripped = re.sub(r"\s+", " ", stripped)
            return stripped if stripped else None
        return value
    
    def _safe_transform(
        self,
        value: Any,
        func: Callable[[Any], Any],
    ) -> Any:
        """Safely apply a transform function."""
        if pd.isna(value) or value is None or value == "":
            return None
        try:
            return func(value)
        except Exception:
            return value  # Return original on error
    
    def clean_whitespace(self, value: Any) -> str | None:
        """
        Clean and normalize whitespace.
        
        - Strips leading/trailing whitespace
        - Normalizes multiple spaces to single space
        - Returns None for empty strings
        """
        if value is None or pd.isna(value):
            return None
        
        text = str(value).strip()
        text = re.sub(r"\s+", " ", text)
        return text if text else None
    
    def normalize_phone(self, value: Any) -> str | None:
        """
        Normalize phone number format.
        
        - Removes non-numeric characters (except +)
        - Adds default country code if missing
        - Formats with spaces for readability
        """
        if value is None or pd.isna(value):
            return None
        
        phone = str(value).strip()
        if not phone:
            return None
        
        # Keep the leading + if present
        has_plus = phone.startswith("+")
        
        # Remove all non-numeric characters
        digits = re.sub(r"[^\d]", "", phone)
        
        if not digits:
            return None
        
        # Add country code if not present
        if has_plus:
            phone = f"+{digits}"
        elif len(digits) <= 10:  # Likely local number
            phone = f"{self.default_country_code}{digits}"
        else:
            phone = f"+{digits}"
        
        return phone
    
    def normalize_email(self, value: Any) -> str | None:
        """
        Normalize email address.
        
        - Lowercase
        - Strip whitespace
        - Basic format validation
        """
        if value is None or pd.isna(value):
            return None
        
        email = str(value).strip().lower()
        if not email:
            return None
        
        # Basic email validation
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        if re.match(email_pattern, email):
            return email
        
        # Return as-is if not valid (let validator handle it)
        return email
    
    def normalize_date(self, value: Any) -> str | None:
        """
        Parse and normalize date to ISO format (YYYY-MM-DD).
        
        Tries multiple common formats.
        """
        if value is None or pd.isna(value):
            return None
        
        if isinstance(value, (datetime, date)):
            return value.strftime("%Y-%m-%d")
        
        date_str = str(value).strip()
        if not date_str:
            return None
        
        # Try each format
        for fmt in self.DATE_FORMATS:
            try:
                parsed = datetime.strptime(date_str, fmt)
                return parsed.strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        # Try pandas parser as last resort
        try:
            parsed = pd.to_datetime(date_str, dayfirst=True)
            if pd.notna(parsed):
                return parsed.strftime("%Y-%m-%d")
        except Exception:
            pass
        
        return date_str  # Return original if can't parse
    
    def normalize_boolean(self, value: Any) -> bool | None:
        """
        Convert various boolean representations to Python bool.
        
        Handles: yes/no, true/false, 1/0, y/n, etc.
        """
        if value is None or pd.isna(value):
            return None
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, (int, float)):
            return bool(value)
        
        str_value = str(value).strip().lower()
        
        if str_value in self.TRUE_VALUES:
            return True
        if str_value in self.FALSE_VALUES:
            return False
        
        return None  # Unknown value
    
    def normalize_numeric(self, value: Any) -> float | None:
        """
        Parse numeric value handling various formats.
        
        - Handles thousand separators (comma or space)
        - Handles both comma and period as decimal separator
        """
        if value is None or pd.isna(value):
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        num_str = str(value).strip()
        if not num_str:
            return None
        
        # Remove currency symbols and spaces
        num_str = re.sub(r"[^\d.,\-]", "", num_str)
        
        if not num_str or num_str == "-":
            return None
        
        # Determine decimal separator
        # If both . and , exist, the last one is decimal
        last_comma = num_str.rfind(",")
        last_period = num_str.rfind(".")
        
        if last_comma > last_period:
            # European format: 1.234,56
            num_str = num_str.replace(".", "").replace(",", ".")
        else:
            # American format: 1,234.56
            num_str = num_str.replace(",", "")
        
        try:
            return float(num_str)
        except ValueError:
            return None
    
    def normalize_currency(self, value: Any) -> float | None:
        """
        Parse currency value, removing symbols.
        
        Examples: "$1,234.56", "€1.234,56", "SAR 100"
        """
        if value is None or pd.isna(value):
            return None
        
        # Use numeric normalizer after stripping currency symbols
        return self.normalize_numeric(value)
    
    def normalize_uom(self, value: Any) -> str | None:
        """
        Normalize unit of measure to Odoo standard names.
        """
        if value is None or pd.isna(value):
            return None
        
        uom = str(value).strip().lower()
        if not uom:
            return None
        
        # Check aliases
        normalized = self.UOM_ALIASES.get(uom)
        if normalized:
            return normalized
        
        # Return title case of original
        return value.strip().title() if value else None
    
    def strip_html(self, value: Any) -> str | None:
        """Remove HTML tags from text."""
        if value is None or pd.isna(value):
            return None
        
        text = str(value)
        # Remove HTML tags
        text = re.sub(r"<[^>]+>", "", text)
        # Decode common entities
        text = text.replace("&nbsp;", " ")
        text = text.replace("&amp;", "&")
        text = text.replace("&lt;", "<")
        text = text.replace("&gt;", ">")
        text = text.replace("&quot;", '"')
        
        return self.clean_whitespace(text)
    
    def register_transform(
        self,
        name: str,
        func: Callable[[Any], Any],
    ) -> None:
        """
        Register a custom transform function.
        
        Args:
            name: Transform name to use in config
            func: Transform function (receives value, returns transformed value)
        """
        self._transforms[name] = func
    
    def get_available_transforms(self) -> list[str]:
        """Get list of available transform names."""
        return list(self._transforms.keys())
