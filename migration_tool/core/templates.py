"""
Template Manager for Import Mappings

Saves and loads reusable mapping configurations.
"""

import json
from pathlib import Path
from typing import Optional
from datetime import datetime


# Templates stored in user's config folder
TEMPLATES_DIR = Path.home() / ".odoo_migration_tool" / "templates"


class ImportTemplate:
    """Represents a saved import mapping configuration."""
    
    def __init__(
        self,
        name: str,
        model: str,
        mappings: dict[str, str],
        defaults: dict[str, any] = None,
        created_at: str = None,
        description: str = "",
    ):
        self.name = name
        self.model = model
        self.mappings = mappings  # {file_column: odoo_field}
        self.defaults = defaults or {}
        self.created_at = created_at or datetime.now().isoformat()
        self.description = description
    
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "model": self.model,
            "mappings": self.mappings,
            "defaults": self.defaults,
            "created_at": self.created_at,
            "description": self.description,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "ImportTemplate":
        return cls(
            name=data["name"],
            model=data["model"],
            mappings=data["mappings"],
            defaults=data.get("defaults", {}),
            created_at=data.get("created_at"),
            description=data.get("description", ""),
        )


class TemplateManager:
    """Manages import templates - save, load, list, delete."""
    
    def __init__(self, templates_dir: Path = None):
        self.templates_dir = templates_dir or TEMPLATES_DIR
        self.templates_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_template_path(self, name: str) -> Path:
        """Get file path for a template by name."""
        # Sanitize name for filesystem
        safe_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)
        return self.templates_dir / f"{safe_name}.json"
    
    def save_template(
        self,
        name: str,
        model: str,
        mappings: dict[str, str],
        defaults: dict[str, any] = None,
        description: str = "",
    ) -> ImportTemplate:
        """Save a new template or overwrite existing."""
        template = ImportTemplate(
            name=name,
            model=model,
            mappings=mappings,
            defaults=defaults,
            description=description,
        )
        
        path = self._get_template_path(name)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(template.to_dict(), f, indent=2)
        
        return template
    
    def load_template(self, name: str) -> Optional[ImportTemplate]:
        """Load a template by name."""
        path = self._get_template_path(name)
        if not path.exists():
            return None
        
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return ImportTemplate.from_dict(data)
    
    def list_templates(self, model: str = None) -> list[ImportTemplate]:
        """List all templates, optionally filtered by model."""
        templates = []
        
        for path in self.templates_dir.glob("*.json"):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                template = ImportTemplate.from_dict(data)
                
                if model is None or template.model == model:
                    templates.append(template)
            except Exception:
                continue  # Skip invalid files
        
        # Sort by creation date, newest first
        templates.sort(key=lambda t: t.created_at, reverse=True)
        return templates
    
    def delete_template(self, name: str) -> bool:
        """Delete a template by name."""
        path = self._get_template_path(name)
        if path.exists():
            path.unlink()
            return True
        return False
    
    def template_exists(self, name: str) -> bool:
        """Check if a template with the given name exists."""
        return self._get_template_path(name).exists()
