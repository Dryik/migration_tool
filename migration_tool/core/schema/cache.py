"""
Schema Cache

Caches Odoo schema metadata for performance.
Supports JSON file storage with invalidation.
"""

import json
import hashlib
from pathlib import Path
from typing import Any
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class CacheMetadata:
    """Metadata about a cached schema."""
    
    database: str
    odoo_version: str
    server_version_info: tuple[int, ...] | None
    modules_hash: str
    created_at: str
    expires_at: str | None = None
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "database": self.database,
            "odoo_version": self.odoo_version,
            "server_version_info": list(self.server_version_info) if self.server_version_info else None,
            "modules_hash": self.modules_hash,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
        }
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CacheMetadata":
        return cls(
            database=data["database"],
            odoo_version=data["odoo_version"],
            server_version_info=tuple(data["server_version_info"]) if data.get("server_version_info") else None,
            modules_hash=data["modules_hash"],
            created_at=data["created_at"],
            expires_at=data.get("expires_at"),
        )


@dataclass
class CacheEntry:
    """A single cache entry with metadata and models."""
    
    metadata: CacheMetadata
    models: dict[str, dict[str, Any]] = field(default_factory=dict)  # model_name -> ModelMeta.to_dict()


class SchemaCache:
    """
    Caches Odoo schema to avoid repeated API calls.
    
    Cache is keyed by:
    - Database name
    - Odoo version
    - Installed modules hash
    
    Storage:
    - JSON files in cache directory
    - One file per database
    
    Example:
        >>> cache = SchemaCache("./cache")
        >>> cache.save("mydb", "16.0", models_dict, modules_list)
        >>> data = cache.load("mydb", "16.0", modules_list)
    """
    
    def __init__(
        self,
        cache_dir: str | Path = "./.schema_cache",
        ttl_hours: int | None = 24,
    ):
        """
        Initialize cache.
        
        Args:
            cache_dir: Directory for cache files
            ttl_hours: Cache TTL in hours (None = no expiration)
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_hours = ttl_hours
        
        # In-memory cache for current session
        self._memory_cache: dict[str, CacheEntry] = {}
    
    def _get_cache_file(self, database: str) -> Path:
        """Get cache file path for a database."""
        safe_name = database.replace("/", "_").replace("\\", "_")
        return self.cache_dir / f"{safe_name}_schema.json"
    
    def _compute_modules_hash(self, modules: list[tuple[str, str]]) -> str:
        """
        Compute hash of installed modules.
        
        Args:
            modules: List of (module_name, version/state) tuples
            
        Returns:
            SHA256 hash string
        """
        # Sort for deterministic hashing
        sorted_modules = sorted(modules, key=lambda x: x[0])
        content = json.dumps(sorted_modules, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def _make_cache_key(
        self,
        database: str,
        odoo_version: str,
        modules_hash: str,
    ) -> str:
        """Create unique cache key."""
        return f"{database}:{odoo_version}:{modules_hash}"
    
    def is_valid(
        self,
        database: str,
        odoo_version: str,
        modules: list[tuple[str, str]],
    ) -> bool:
        """
        Check if cache is valid for given parameters.
        
        Args:
            database: Database name
            odoo_version: Odoo version string
            modules: List of (module_name, version) tuples
            
        Returns:
            True if valid cache exists
        """
        modules_hash = self._compute_modules_hash(modules)
        cache_key = self._make_cache_key(database, odoo_version, modules_hash)
        
        # Check memory cache first
        if cache_key in self._memory_cache:
            entry = self._memory_cache[cache_key]
            if not self._is_expired(entry.metadata):
                return True
        
        # Check file cache
        cache_file = self._get_cache_file(database)
        if not cache_file.exists():
            return False
        
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            metadata = CacheMetadata.from_dict(data["metadata"])
            
            # Check version and modules hash
            if metadata.odoo_version != odoo_version:
                return False
            if metadata.modules_hash != modules_hash:
                return False
            if self._is_expired(metadata):
                return False
            
            return True
            
        except (json.JSONDecodeError, KeyError):
            return False
    
    def _is_expired(self, metadata: CacheMetadata) -> bool:
        """Check if cache entry is expired."""
        if not metadata.expires_at:
            return False
        
        try:
            expires = datetime.fromisoformat(metadata.expires_at)
            return datetime.now() > expires
        except ValueError:
            return True
    
    def load(
        self,
        database: str,
        odoo_version: str,
        modules: list[tuple[str, str]],
    ) -> dict[str, dict[str, Any]] | None:
        """
        Load cached schema if valid.
        
        Args:
            database: Database name
            odoo_version: Odoo version string
            modules: Installed modules list
            
        Returns:
            Dict of model_name -> model_dict, or None if no valid cache
        """
        modules_hash = self._compute_modules_hash(modules)
        cache_key = self._make_cache_key(database, odoo_version, modules_hash)
        
        # Check memory cache
        if cache_key in self._memory_cache:
            entry = self._memory_cache[cache_key]
            if not self._is_expired(entry.metadata):
                return entry.models
        
        # Load from file
        cache_file = self._get_cache_file(database)
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            metadata = CacheMetadata.from_dict(data["metadata"])
            
            # Validate
            if metadata.odoo_version != odoo_version:
                return None
            if metadata.modules_hash != modules_hash:
                return None
            if self._is_expired(metadata):
                return None
            
            models = data.get("models", {})
            
            # Store in memory cache
            self._memory_cache[cache_key] = CacheEntry(
                metadata=metadata,
                models=models,
            )
            
            return models
            
        except (json.JSONDecodeError, KeyError):
            return None
    
    def save(
        self,
        database: str,
        odoo_version: str,
        modules: list[tuple[str, str]],
        models: dict[str, dict[str, Any]],
        server_version_info: tuple[int, ...] | None = None,
    ) -> None:
        """
        Save schema to cache.
        
        Args:
            database: Database name
            odoo_version: Odoo version string
            modules: Installed modules list
            models: Dict of model_name -> model_dict
            server_version_info: Optional server version tuple
        """
        modules_hash = self._compute_modules_hash(modules)
        
        now = datetime.now()
        expires = None
        if self.ttl_hours:
            from datetime import timedelta
            expires = (now + timedelta(hours=self.ttl_hours)).isoformat()
        
        metadata = CacheMetadata(
            database=database,
            odoo_version=odoo_version,
            server_version_info=server_version_info,
            modules_hash=modules_hash,
            created_at=now.isoformat(),
            expires_at=expires,
        )
        
        cache_data = {
            "metadata": metadata.to_dict(),
            "models": models,
        }
        
        # Save to file
        cache_file = self._get_cache_file(database)
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)
        
        # Update memory cache
        cache_key = self._make_cache_key(database, odoo_version, modules_hash)
        self._memory_cache[cache_key] = CacheEntry(
            metadata=metadata,
            models=models,
        )
    
    def invalidate(self, database: str | None = None) -> None:
        """
        Invalidate cache.
        
        Args:
            database: Specific database to invalidate, or None for all
        """
        if database:
            # Clear specific database
            cache_file = self._get_cache_file(database)
            if cache_file.exists():
                cache_file.unlink()
            
            # Clear from memory
            keys_to_remove = [k for k in self._memory_cache if k.startswith(f"{database}:")]
            for key in keys_to_remove:
                del self._memory_cache[key]
        else:
            # Clear all
            for cache_file in self.cache_dir.glob("*_schema.json"):
                cache_file.unlink()
            self._memory_cache.clear()
    
    def get_cache_info(self, database: str) -> dict[str, Any] | None:
        """Get cache metadata for a database."""
        cache_file = self._get_cache_file(database)
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("metadata")
        except (json.JSONDecodeError, KeyError):
            return None
