"""
Configuration Loader

Handles loading and validating YAML/JSON configuration files for migrations.
Supports environment variable substitution for sensitive values.
"""

import os
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator
from dotenv import load_dotenv


# Load environment variables from .env file if present
load_dotenv()


class OdooConfig(BaseModel):
    """Odoo connection configuration."""
    
    url: str = Field(..., description="Odoo instance URL")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Username for authentication")
    password: str = Field(..., description="Password or API key")
    timeout: int = Field(default=120, ge=10, le=600)
    
    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Ensure URL is valid and normalize."""
        v = v.rstrip("/")
        if not v.startswith(("http://", "https://")):
            raise ValueError("URL must start with http:// or https://")
        return v


class DedupeConfig(BaseModel):
    """Deduplication configuration for a model."""
    
    keys: list[str] = Field(..., min_length=1, description="Fields to use for deduplication")
    strategy: Literal["skip", "update", "create"] = Field(
        default="skip",
        description="Action when duplicate found: skip, update existing, or create new"
    )
    case_sensitive: bool = Field(default=False, description="Case-sensitive matching")
    match_odoo: bool = Field(default=True, description="Check for duplicates in Odoo")
    match_batch: bool = Field(default=True, description="Check for duplicates within import batch")


class TransformConfig(BaseModel):
    """Field transformation configuration."""
    
    field: str = Field(..., description="Field name to transform")
    function: str = Field(..., description="Transform function name")
    params: dict[str, Any] = Field(default_factory=dict, description="Transform parameters")


class ValidationRule(BaseModel):
    """Custom validation rule."""
    
    field: str = Field(..., description="Field to validate")
    rule: str = Field(..., description="Validation rule type")
    params: dict[str, Any] = Field(default_factory=dict, description="Rule parameters")
    message: str = Field(default="", description="Custom error message")


class ModelConfig(BaseModel):
    """Configuration for importing a specific Odoo model."""
    
    model: str = Field(..., description="Odoo model name (e.g., res.partner)")
    source: str = Field(..., description="Source file path (CSV or Excel)")
    sheet: str | None = Field(default=None, description="Excel sheet name (if applicable)")
    enabled: bool = Field(default=True, description="Whether to import this model")
    
    # Column mapping: source column -> Odoo field
    mapping: dict[str, str] = Field(
        default_factory=dict,
        description="Column name to Odoo field mapping"
    )
    
    # Deduplication settings
    dedupe: DedupeConfig | None = Field(default=None, description="Deduplication settings")
    
    # Field transformations
    transforms: dict[str, str] = Field(
        default_factory=dict,
        description="Field to transform function mapping"
    )
    
    # Validation rules
    validation: dict[str, Any] = Field(
        default_factory=dict,
        description="Validation settings"
    )
    
    # Default values for missing fields
    defaults: dict[str, Any] = Field(
        default_factory=dict,
        description="Default values for fields"
    )
    
    # Fields to skip in import
    skip_fields: list[str] = Field(
        default_factory=list,
        description="Source fields to ignore"
    )
    
    # Processing order (lower = earlier)
    priority: int = Field(default=100, description="Import order priority")
    
    @field_validator("model")
    @classmethod
    def validate_model(cls, v: str) -> str:
        """Validate model name format."""
        if not re.match(r"^[a-z_]+\.[a-z_]+$", v):
            raise ValueError(f"Invalid model name format: {v}")
        return v


class ImportConfig(BaseModel):
    """Global import settings."""
    
    chunk_size: int = Field(default=500, ge=1, le=5000, description="Records per batch")
    retry_attempts: int = Field(default=3, ge=1, le=10, description="Retry attempts on failure")
    retry_delay: float = Field(default=2.0, ge=0.5, le=60.0, description="Delay between retries")
    parallel: bool = Field(default=False, description="Enable parallel processing")
    max_workers: int = Field(default=4, ge=1, le=16, description="Max parallel workers")
    stop_on_error: bool = Field(default=False, description="Stop on first error")
    dry_run: bool = Field(default=False, description="Validate without creating records")


class LoggingConfig(BaseModel):
    """Logging configuration."""
    
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(default="INFO")
    output_dir: str = Field(default="./logs", description="Log output directory")
    export_json: bool = Field(default=True, description="Export logs as JSON")
    export_csv: bool = Field(default=True, description="Export logs as CSV")
    console_progress: bool = Field(default=True, description="Show console progress")


class MigrationConfig(BaseModel):
    """Root configuration for a migration."""
    
    name: str = Field(default="migration", description="Migration name/identifier")
    version: str = Field(default="1.0", description="Configuration version")
    
    odoo: OdooConfig = Field(..., description="Odoo connection settings")
    import_settings: ImportConfig = Field(
        default_factory=ImportConfig,
        alias="import",
        description="Import settings"
    )
    logging: LoggingConfig = Field(
        default_factory=LoggingConfig,
        description="Logging settings"
    )
    models: list[ModelConfig] = Field(
        default_factory=list,
        description="Model import configurations"
    )
    
    @model_validator(mode="after")
    def sort_models_by_priority(self) -> "MigrationConfig":
        """Sort models by priority for correct import order."""
        self.models.sort(key=lambda m: m.priority)
        return self
    
    class Config:
        populate_by_name = True


class ConfigLoader:
    """
    Loads and validates migration configuration from YAML/JSON files.
    
    Supports environment variable substitution using ${VAR_NAME} syntax.
    
    Example:
        >>> loader = ConfigLoader()
        >>> config = loader.load("config/customer_import.yaml")
        >>> print(config.odoo.url)
    """
    
    # Pattern for environment variable substitution
    ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")
    
    def __init__(self, env_file: Path | None = None):
        """
        Initialize config loader.
        
        Args:
            env_file: Optional path to .env file
        """
        if env_file:
            load_dotenv(env_file)
    
    def load(self, config_path: str | Path) -> MigrationConfig:
        """
        Load configuration from file.
        
        Args:
            config_path: Path to YAML or JSON config file
            
        Returns:
            Validated MigrationConfig object
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config is invalid
        """
        path = Path(config_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")
        
        content = path.read_text(encoding="utf-8")
        
        # Substitute environment variables
        content = self._substitute_env_vars(content)
        
        # Parse YAML (also handles JSON as subset of YAML)
        try:
            data = yaml.safe_load(content)
        except yaml.YAMLError as e:
            raise ValueError(f"Failed to parse configuration: {e}") from e
        
        if not isinstance(data, dict):
            raise ValueError("Configuration must be a dictionary")
        
        # Validate with Pydantic
        try:
            return MigrationConfig.model_validate(data)
        except Exception as e:
            raise ValueError(f"Configuration validation failed: {e}") from e
    
    def _substitute_env_vars(self, content: str) -> str:
        """
        Replace ${VAR_NAME} with environment variable values.
        
        Args:
            content: Configuration content string
            
        Returns:
            Content with substituted values
        """
        def replace(match: re.Match[str]) -> str:
            var_name = match.group(1)
            value = os.environ.get(var_name)
            if value is None:
                raise ValueError(
                    f"Environment variable '{var_name}' is not set. "
                    f"Please set it or update the configuration."
                )
            return value
        
        return self.ENV_PATTERN.sub(replace, content)
    
    def validate_file(self, config_path: str | Path) -> list[str]:
        """
        Validate a configuration file and return any errors.
        
        Args:
            config_path: Path to config file
            
        Returns:
            List of error messages (empty if valid)
        """
        errors: list[str] = []
        
        try:
            self.load(config_path)
        except FileNotFoundError as e:
            errors.append(str(e))
        except ValueError as e:
            errors.append(str(e))
        except Exception as e:
            errors.append(f"Unexpected error: {e}")
        
        return errors
    
    @staticmethod
    def create_example_config(output_path: str | Path) -> None:
        """
        Create an example configuration file.
        
        Args:
            output_path: Where to write the example config
        """
        example = {
            "name": "customer_migration",
            "version": "1.0",
            "odoo": {
                "url": "${ODOO_URL}",
                "database": "${ODOO_DB}",
                "username": "${ODOO_USER}",
                "password": "${ODOO_PASSWORD}",
            },
            "import": {
                "chunk_size": 500,
                "retry_attempts": 3,
                "retry_delay": 2.0,
                "dry_run": False,
            },
            "logging": {
                "level": "INFO",
                "output_dir": "./logs",
                "export_json": True,
                "export_csv": True,
            },
            "models": [
                {
                    "model": "res.partner",
                    "source": "data/customers.xlsx",
                    "sheet": "Customers",
                    "priority": 10,
                    "mapping": {
                        "Customer Name": "name",
                        "Phone Number": "phone",
                        "Email Address": "email",
                        "Street Address": "street",
                        "City": "city",
                        "Country": "country_id",
                    },
                    "dedupe": {
                        "keys": ["name", "phone"],
                        "strategy": "update",
                    },
                    "transforms": {
                        "phone": "normalize_phone",
                        "email": "normalize_email",
                    },
                    "validation": {
                        "required": ["name"],
                    },
                    "defaults": {
                        "is_company": False,
                        "customer_rank": 1,
                    },
                },
            ],
        }
        
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(example, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
