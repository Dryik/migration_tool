"""
Tests for the Schema Introspection module.
"""

import pytest

from migration_tool.core.schema.models import (
    FieldMeta,
    ModelMeta,
    FieldType,
    FieldClassification,
)
from migration_tool.core.schema.classifier import FieldClassifier, SYSTEM_FIELDS


class TestFieldType:
    """Tests for FieldType enum."""
    
    def test_from_string_valid(self):
        """Test converting valid type strings."""
        assert FieldType.from_string("char") == FieldType.CHAR
        assert FieldType.from_string("many2one") == FieldType.MANY2ONE
        assert FieldType.from_string("boolean") == FieldType.BOOLEAN
    
    def test_from_string_invalid(self):
        """Test converting invalid type strings."""
        assert FieldType.from_string("invalid") == FieldType.UNKNOWN
        assert FieldType.from_string("") == FieldType.UNKNOWN


class TestFieldMeta:
    """Tests for FieldMeta dataclass."""
    
    def test_creation(self):
        """Test creating field metadata."""
        field = FieldMeta(
            model="res.partner",
            name="phone",
            label="Phone",
            field_type=FieldType.CHAR,
            required=False,
            readonly=False,
            importable=True,
            exportable=True,
        )
        
        assert field.model == "res.partner"
        assert field.name == "phone"
        assert field.field_type == FieldType.CHAR
        assert field.importable is True
    
    def test_serialization(self):
        """Test to_dict and from_dict."""
        field = FieldMeta(
            model="res.partner",
            name="country_id",
            label="Country",
            field_type=FieldType.MANY2ONE,
            required=False,
            readonly=False,
            classification=FieldClassification.RELATIONAL,
            importable=True,
            exportable=True,
            relation="res.country",
        )
        
        data = field.to_dict()
        restored = FieldMeta.from_dict(data)
        
        assert restored.model == field.model
        assert restored.name == field.name
        assert restored.field_type == field.field_type
        assert restored.relation == field.relation


class TestModelMeta:
    """Tests for ModelMeta dataclass."""
    
    def test_importable_fields(self):
        """Test filtering importable fields."""
        model = ModelMeta(name="res.partner", label="Contact")
        
        model.fields["name"] = FieldMeta(
            model="res.partner", name="name", label="Name",
            field_type=FieldType.CHAR, importable=True, exportable=True,
        )
        model.fields["id"] = FieldMeta(
            model="res.partner", name="id", label="ID",
            field_type=FieldType.INTEGER, importable=False, exportable=True,
        )
        
        importable = model.importable_fields
        assert len(importable) == 1
        assert importable[0].name == "name"
    
    def test_required_fields(self):
        """Test filtering required fields."""
        model = ModelMeta(name="res.partner", label="Contact")
        
        model.fields["name"] = FieldMeta(
            model="res.partner", name="name", label="Name",
            field_type=FieldType.CHAR, required=True, importable=True,
        )
        model.fields["phone"] = FieldMeta(
            model="res.partner", name="phone", label="Phone",
            field_type=FieldType.CHAR, required=False, importable=True,
        )
        
        required = model.required_fields
        assert len(required) == 1
        assert required[0].name == "name"


class TestFieldClassifier:
    """Tests for FieldClassifier."""
    
    @pytest.fixture
    def classifier(self):
        return FieldClassifier()
    
    def test_classify_simple_char(self, classifier):
        """Test classifying a simple char field."""
        field = classifier.classify(
            model="res.partner",
            name="phone",
            label="Phone",
            field_info={
                "type": "char",
                "required": False,
                "readonly": False,
                "store": True,
            },
        )
        
        assert field.field_type == FieldType.CHAR
        assert field.importable is True
        assert field.exportable is True
        assert field.classification == FieldClassification.IMPORTABLE
    
    def test_classify_readonly_field(self, classifier):
        """Test classifying a readonly field."""
        field = classifier.classify(
            model="res.partner",
            name="display_name",
            label="Display Name",
            field_info={
                "type": "char",
                "required": False,
                "readonly": True,
                "store": True,
            },
        )
        
        assert field.importable is False
        assert field.exportable is True
        assert field.classification == FieldClassification.EXPORT_ONLY
    
    def test_classify_computed_with_inverse(self, classifier):
        """Test computed field with inverse is importable."""
        field = classifier.classify(
            model="res.partner",
            name="email_formatted",
            label="Email Formatted",
            field_info={
                "type": "char",
                "readonly": False,
                "store": True,
                "compute": "_compute_email_formatted",
                "inverse": "_inverse_email_formatted",
            },
        )
        
        assert field.computed is True
        assert field.has_inverse is True
        assert field.importable is True
    
    def test_classify_computed_without_inverse(self, classifier):
        """Test computed field without inverse is export-only."""
        field = classifier.classify(
            model="res.partner",
            name="partner_share",
            label="Share Partner",
            field_info={
                "type": "boolean",
                "store": True,
                "compute": "_compute_partner_share",
            },
        )
        
        assert field.computed is True
        assert field.has_inverse is False
        assert field.importable is False
        assert field.exportable is True
    
    def test_classify_many2one(self, classifier):
        """Test classifying many2one field."""
        field = classifier.classify(
            model="res.partner",
            name="country_id",
            label="Country",
            field_info={
                "type": "many2one",
                "relation": "res.country",
                "required": False,
                "readonly": False,
                "store": True,
            },
        )
        
        assert field.field_type == FieldType.MANY2ONE
        assert field.relation == "res.country"
        assert field.importable is True
        assert field.classification == FieldClassification.RELATIONAL
    
    def test_classify_one2many_ignored(self, classifier):
        """Test one2many fields are ignored."""
        field = classifier.classify(
            model="res.partner",
            name="child_ids",
            label="Contacts",
            field_info={
                "type": "one2many",
                "relation": "res.partner",
                "store": True,
            },
        )
        
        assert field.field_type == FieldType.ONE2MANY
        assert field.importable is False
        assert field.classification == FieldClassification.IGNORED
    
    def test_classify_system_field(self, classifier):
        """Test system fields are ignored."""
        field = classifier.classify(
            model="res.partner",
            name="create_uid",
            label="Created by",
            field_info={
                "type": "many2one",
                "relation": "res.users",
                "store": True,
            },
        )
        
        assert field.is_system is True
        assert field.importable is False
        assert field.classification == FieldClassification.IGNORED
    
    def test_classify_custom_field(self, classifier):
        """Test custom fields are marked correctly."""
        field = classifier.classify(
            model="res.partner",
            name="x_custom_field",
            label="Custom Field",
            field_info={
                "type": "char",
                "store": True,
            },
        )
        
        assert field.is_custom is True
        assert field.importable is True
    
    def test_classify_selection(self, classifier):
        """Test selection field with options."""
        field = classifier.classify(
            model="res.partner",
            name="type",
            label="Address Type",
            field_info={
                "type": "selection",
                "selection": [("contact", "Contact"), ("invoice", "Invoice")],
                "store": True,
            },
        )
        
        assert field.field_type == FieldType.SELECTION
        assert field.selection == [("contact", "Contact"), ("invoice", "Invoice")]
        assert field.importable is True
    
    def test_system_fields_constant(self):
        """Test system fields set is defined."""
        assert "id" in SYSTEM_FIELDS
        assert "create_uid" in SYSTEM_FIELDS
        assert "write_date" in SYSTEM_FIELDS
