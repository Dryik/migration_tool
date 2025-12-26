"""
Odoo Data Migration Tool - PyQt6 Edition with Full Functionality
Integrated with Odoo connection, file loading, and import logic
"""

import sys
import json
import time
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QComboBox, QLineEdit, QPushButton, QScrollArea,
    QFrame, QProgressBar, QSizePolicy, QSpacerItem, QFileDialog,
    QListView, QStackedWidget, QMessageBox, QTabWidget, QTableWidget,
    QTableWidgetItem, QHeaderView, QInputDialog
)
from PyQt6.QtCore import Qt, QSize, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont

# Import Odoo modules
from migration_tool.odoo import OdooClient, OdooConnectionError
from migration_tool.core.schema import SchemaInspector, SchemaCache
from migration_tool.core.reader import DataReader
from migration_tool.core.cleaner import DataCleaner
from migration_tool.odoo.adapters import get_adapter, ReferenceCache
from migration_tool.core.templates import TemplateManager
from migration_tool.core.quality_stats import QualityStats, QualityAnalyzer, IssueType
from migration_tool.core.validation_rules import FieldValidator


# ============== Settings ==============
SETTINGS_FILE = Path.home() / ".odoo_migration_tool" / "settings.json"


def load_settings() -> dict:
    if SETTINGS_FILE.exists():
        try:
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_settings(settings: dict):
    SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
        json.dump(settings, f, indent=2)


# ============== App Config ==============
ODOO_APPS = {
    "Sales (CRM)": ["res.partner", "sale.order", "crm.lead"],
    "Inventory": ["product.template", "product.product", "product.category"],
    "Accounting": ["account.account", "account.move", "account.payment"],
    "Purchase": ["purchase.order", "res.partner"],
    "Project": ["project.project", "project.task"],
}

MODEL_LABELS = {
    "res.partner": "Contacts",
    "product.template": "Products",
    "product.product": "Variants",
    "product.category": "Categories",
    "account.account": "Accounts",
    "account.move": "Invoices",
    "account.payment": "Payments",
    "sale.order": "Sales Orders",
    "purchase.order": "Purchase Orders",
    "crm.lead": "Leads/Opportunities",
    "project.project": "Projects",
    "project.task": "Project Tasks",
}

# Field icons for dropdown display
FIELD_ICONS = {
    # Specific field names
    "name": "≡",
    "display_name": "≡",
    "email": "✉",
    "email_from": "✉",
    "phone": "☎",
    "mobile": "📱",
    "street": "📍",
    "street2": "📍",
    "city": "📍",
    "zip": "📍",
    "country_id": "🌍",
    "state_id": "🌍",
    "parent_id": "🏢",
    "company_id": "🏢",
    "function": "💼",
    "title": "💼",
    "website": "🌐",
    "comment": "📝",
    "note": "📝",
    "description": "📝",
    "ref": "#",
    "code": "#",
    "barcode": "▮",
    "image": "🖼",
    "image_1920": "🖼",
    "date": "📅",
    "create_date": "📅",
    "write_date": "📅",
    "user_id": "👤",
    "partner_id": "👤",
    "categ_id": "📁",
    "tag_ids": "🏷",
    "active": "☑",
    "list_price": "💰",
    "standard_price": "💰",
    "price": "💰",
    "amount": "💰",
    # Default by type
    "_char": "≡",
    "_text": "📝",
    "_html": "📝",
    "_many2one": "🔗",
    "_many2many": "🔗",
    "_one2many": "🔗",
    "_integer": "#",
    "_float": "#",
    "_monetary": "💰",
    "_boolean": "☑",
    "_date": "📅",
    "_datetime": "📅",
    "_selection": "▼",
    "_binary": "📎",
}


# ============== Color Palette (Dark Theme) ==============
COLORS = {
    "bg_dark": "#0c1222",
    "bg_card": "#111827",
    "sidebar": "#111827",
    "border": "#1f2937",
    "border_light": "#374151",
    "text_primary": "#f3f4f6",
    "text_secondary": "#9ca3af",
    "text_muted": "#6b7280",
    "text_sidebar": "#f3f4f6",
    "accent": "#22c55e",
    "accent_hover": "#16a34a",
    "input_bg": "#1f2937",
    "row_odd": "#111827",
    "row_even": "#0c1222",
    "dropdown_bg": "#1f2937",
    "dropdown_hover": "#374151",
    "error": "#ef4444",
}


# ============== Global QSS Stylesheet ==============
STYLESHEET = f"""
* {{
    font-family: 'Segoe UI', 'Inter', -apple-system, sans-serif;
}}

QMainWindow {{
    background-color: {COLORS['bg_dark']};
}}

QWidget {{
    color: {COLORS['text_primary']};
    font-size: 13px;
}}

#sidebar {{
    background-color: {COLORS['sidebar']};
    border-right: 1px solid {COLORS['border']};
}}

QLabel#sectionLabel {{
    color: {COLORS['text_muted']};
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 1px;
    padding: 0px;
}}

QLabel#titleLabel {{
    font-size: 26px;
    font-weight: 600;
    color: {COLORS['text_primary']};
    padding: 0px;
}}

QLabel#subtitleLabel {{
    font-size: 13px;
    color: {COLORS['text_secondary']};
    padding: 0px;
}}

QLabel#statusConnected {{
    color: {COLORS['accent']};
    font-size: 13px;
    font-weight: 500;
}}

QLabel#statusDisconnected {{
    color: {COLORS['text_muted']};
    font-size: 13px;
}}

QLabel#statusError {{
    color: {COLORS['error']};
    font-size: 13px;
}}

QComboBox {{
    background-color: {COLORS['input_bg']};
    border: 1px solid {COLORS['border']};
    border-radius: 8px;
    padding: 12px 16px;
    padding-right: 35px;
    color: {COLORS['text_primary']};
    font-size: 13px;
    min-height: 18px;
}}

QComboBox:hover {{
    border-color: {COLORS['border_light']};
}}

QComboBox:focus {{
    border-color: {COLORS['accent']};
}}

QComboBox::drop-down {{
    border: none;
    width: 35px;
}}

QComboBox::down-arrow {{
    image: none;
    border-style: solid;
    border-width: 5px 5px 0 5px;
    border-color: {COLORS['text_secondary']} transparent transparent transparent;
}}

QComboBox QAbstractItemView {{
    background-color: {COLORS['dropdown_bg']};
    border: 1px solid {COLORS['border_light']};
    border-radius: 8px;
    padding: 6px;
    selection-background-color: {COLORS['dropdown_hover']};
    color: {COLORS['text_primary']};
    outline: none;
}}

QComboBox QAbstractItemView::item {{
    padding: 10px 14px;
    border-radius: 4px;
    min-height: 20px;
}}

QComboBox QAbstractItemView::item:hover {{
    background-color: {COLORS['dropdown_hover']};
}}

QComboBox QAbstractItemView::item:selected {{
    background-color: {COLORS['accent']};
    color: white;
}}

QComboBox#fieldDropdown {{
    background-color: {COLORS['dropdown_bg']};
    border: none;
    border-radius: 8px;
    padding: 10px 14px;
}}

QLineEdit {{
    background-color: {COLORS['input_bg']};
    border: 1px solid {COLORS['border']};
    border-radius: 8px;
    padding: 12px 16px;
    color: {COLORS['text_primary']};
    font-size: 13px;
}}

QLineEdit:focus {{
    border-color: {COLORS['accent']};
}}

QPushButton {{
    border-radius: 6px;
    padding: 10px 20px;
    font-size: 13px;
    font-weight: 500;
}}

QPushButton#primaryBtn {{
    background-color: {COLORS['accent']};
    color: white;
    border: none;
}}

QPushButton#primaryBtn:hover {{
    background-color: {COLORS['accent_hover']};
}}

QPushButton#primaryBtn:pressed {{
    background-color: #14532d;
}}

QPushButton#primaryBtn:disabled {{
    background-color: {COLORS['border']};
    color: {COLORS['text_muted']};
}}

QPushButton#outlineBtn {{
    background-color: transparent;
    color: {COLORS['text_primary']};
    border: 1px solid {COLORS['border_light']};
}}

QPushButton#outlineBtn:hover {{
    background-color: {COLORS['input_bg']};
    border-color: {COLORS['text_muted']};
}}

QPushButton#outlineBtn:pressed {{
    background-color: {COLORS['border']};
    border-color: {COLORS['accent']};
}}

QPushButton#outlineBtn:disabled {{
    color: {COLORS['text_muted']};
    border-color: {COLORS['border']};
}}

QPushButton#modelBtn {{
    background-color: transparent;
    color: {COLORS['text_secondary']};
    border: none;
    text-align: left;
    padding: 14px 16px;
    font-size: 13px;
    border-radius: 8px;
}}

QPushButton#modelBtn:hover {{
    background-color: {COLORS['input_bg']};
    color: {COLORS['text_primary']};
}}

QPushButton#modelBtnActive {{
    background-color: {COLORS['input_bg']};
    color: {COLORS['text_primary']};
    border: none;
    text-align: left;
    padding: 14px 16px;
    font-size: 13px;
    border-radius: 8px;
}}

QPushButton#iconBtn {{
    background-color: transparent;
    border: none;
    padding: 12px;
    border-radius: 8px;
    color: {COLORS['text_muted']};
    font-size: 18px;
}}

QPushButton#iconBtn:hover {{
    background-color: {COLORS['input_bg']};
}}

QPushButton#addMappingBtn {{
    background-color: transparent;
    color: {COLORS['text_secondary']};
    border: 1px solid {COLORS['border']};
    border-radius: 6px;
    padding: 10px 16px;
    font-size: 13px;
}}

QPushButton#addMappingBtn:hover {{
    background-color: {COLORS['input_bg']};
    color: {COLORS['text_primary']};
}}

QProgressBar {{
    background-color: {COLORS['border']};
    border: none;
    border-radius: 3px;
    height: 6px;
}}

QProgressBar::chunk {{
    background-color: {COLORS['accent']};
    border-radius: 3px;
}}

QScrollArea {{
    border: none;
    background-color: transparent;
}}

QScrollBar:vertical {{
    background-color: {COLORS['bg_dark']};
    width: 10px;
    border-radius: 5px;
}}

QScrollBar::handle:vertical {{
    background-color: {COLORS['border_light']};
    border-radius: 5px;
    min-height: 40px;
}}

QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
    height: 0px;
}}

#tableHeader {{
    background-color: {COLORS['sidebar']};
    border: none;
    border-radius: 8px 8px 0 0;
}}

#tableContainer {{
    background-color: {COLORS['bg_card']};
    border: none;
    border-radius: 0 0 8px 8px;
}}

#footer {{
    background-color: {COLORS['bg_card']};
    border: none;
    border-radius: 8px;
}}
"""


# ============== Worker Thread for Background Tasks ==============
class WorkerThread(QThread):
    finished = pyqtSignal(object)
    error = pyqtSignal(str)
    progress = pyqtSignal(int, int, str)  # current, total, message
    
    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs
    
    def run(self):
        try:
            result = self.func(*self.args, **self.kwargs)
            self.finished.emit(result)
        except Exception as e:
            self.error.emit(str(e))


# ============== Mapping Row Widget ==============
class MappingRow(QWidget):
    def __init__(self, file_col: str, sample_data: str, field_options: list, fields_data: dict = None, matched_field: str = "", is_odd: bool = True):
        super().__init__()
        self.file_col = file_col
        self.fields_data = fields_data or {}
        
        bg_color = COLORS['row_odd'] if is_odd else COLORS['row_even']
        self.setStyleSheet(f"background-color: {bg_color};")
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(24, 16, 24, 16)
        layout.setSpacing(0)
        
        file_label = QLabel(file_col)
        file_label.setFixedWidth(200)
        file_label.setStyleSheet(f"color: {COLORS['text_primary']}; font-size: 13px; font-weight: 500; background: transparent;")
        layout.addWidget(file_label)
        
        sample_label = QLabel(sample_data[:40] if sample_data else "—")
        sample_label.setFixedWidth(220)
        sample_label.setStyleSheet(f"color: {COLORS['text_secondary']}; font-size: 13px; background: transparent;")
        layout.addWidget(sample_label)
        
        layout.addSpacing(20)
        
        self.dropdown = QComboBox()
        self.dropdown.setObjectName("fieldDropdown")
        self.dropdown.setView(QListView())
        
        # Add items with icons
        self.dropdown.addItem("— Don't import —")
        for option in field_options:
            self.dropdown.addItem(option)
        
        if matched_field:
            idx = self.dropdown.findText(matched_field)
            if idx >= 0:
                self.dropdown.setCurrentIndex(idx)
        
        self.dropdown.setMinimumWidth(200)
        self.dropdown.setMaximumWidth(250)
        layout.addWidget(self.dropdown)
        layout.addStretch()
    
    def get_mapping(self) -> tuple:
        field = self.dropdown.currentText()
        if field == "— Don't import —":
            return (self.file_col, None)
        return (self.file_col, field)


# ============== Main Window ==============
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Odoo Migration Tool")
        self.setMinimumSize(900, 600)
        self.resize(1000, 700)
        self.setStyleSheet(STYLESHEET)
        
        # Center window on screen
        screen = QApplication.primaryScreen().geometry()
        x = (screen.width() - 1000) // 2
        y = (screen.height() - 700) // 2
        self.move(x, y)
        
        # State
        self.client: Optional[OdooClient] = None
        self.inspector: Optional[SchemaInspector] = None
        self.settings = load_settings()
        self.fields_data: dict = {}
        self.file_columns: list = []
        self.file_records: list = []
        self.current_model = ""
        self.file_path = ""
        self.mapping_rows: list = []
        self.worker: Optional[WorkerThread] = None
        
        # New: Template manager
        self.template_manager = TemplateManager()
        
        # New: Rollback support - track created IDs
        self.last_import_ids: list[int] = []
        self.last_import_model: str = ""
        
        # New: Preview data (cleaned)
        self.preview_data: list[dict] = []
        
        # New: Quality analysis
        self.quality_stats: Optional[QualityStats] = None
        self.field_validator = FieldValidator()
        self.quality_analyzer = QualityAnalyzer()
        
        # Build UI
        central = QWidget()
        self.setCentralWidget(central)
        
        main_layout = QHBoxLayout(central)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        self.sidebar = self._create_sidebar()
        main_layout.addWidget(self.sidebar)
        
        self.content_stack = QStackedWidget()
        self.import_view = self._create_import_view()
        self.settings_view = self._create_settings_view()
        self.content_stack.addWidget(self.import_view)
        self.content_stack.addWidget(self.settings_view)
        main_layout.addWidget(self.content_stack, 1)
        
        # Load saved settings
        self._load_settings()
    
    def _load_settings(self):
        if 'connection' in self.settings:
            c = self.settings['connection']
            self.url_input.setText(c.get('url', ''))
            self.db_input.setText(c.get('database', ''))
            self.user_input.setText(c.get('username', ''))
            self.pass_input.setText(c.get('password', ''))
    
    def _create_sidebar(self) -> QWidget:
        sidebar = QWidget()
        sidebar.setObjectName("sidebar")
        sidebar.setFixedWidth(280)
        
        layout = QVBoxLayout(sidebar)
        layout.setContentsMargins(24, 28, 24, 24)
        layout.setSpacing(0)
        
        # Connection status
        status_layout = QHBoxLayout()
        status_layout.setSpacing(12)
        
        self.conn_dot = QLabel("●")
        self.conn_dot.setStyleSheet(f"color: {COLORS['text_muted']}; font-size: 16px;")
        status_layout.addWidget(self.conn_dot)
        
        self.conn_status = QLabel("Disconnected")
        self.conn_status.setObjectName("statusDisconnected")
        status_layout.addWidget(self.conn_status)
        status_layout.addStretch()
        
        layout.addLayout(status_layout)
        layout.addSpacing(35)
        
        # APP SELECTOR
        app_label = QLabel("APP SELECTOR")
        app_label.setObjectName("sectionLabel")
        layout.addWidget(app_label)
        layout.addSpacing(12)
        
        self.app_combo = QComboBox()
        self.app_combo.setView(QListView())
        self.app_combo.addItems(list(ODOO_APPS.keys()))
        self.app_combo.currentTextChanged.connect(self._on_app_changed)
        layout.addWidget(self.app_combo)
        layout.addSpacing(30)
        
        # MODEL SELECTOR
        model_label = QLabel("MODEL SELECTOR")
        model_label.setObjectName("sectionLabel")
        layout.addWidget(model_label)
        layout.addSpacing(12)
        
        self.model_buttons_layout = QVBoxLayout()
        self.model_buttons_layout.setSpacing(2)
        self.model_buttons = []
        layout.addLayout(self.model_buttons_layout)
        
        layout.addStretch()
        
        # Bottom icons
        icon_layout = QHBoxLayout()
        icon_layout.setSpacing(8)
        
        db_btn = QPushButton("📄")
        db_btn.setObjectName("iconBtn")
        db_btn.setFixedSize(44, 44)
        db_btn.clicked.connect(lambda: self.content_stack.setCurrentIndex(0))
        icon_layout.addWidget(db_btn)
        
        settings_btn = QPushButton("⚙")
        settings_btn.setObjectName("iconBtn")
        settings_btn.setFixedSize(44, 44)
        settings_btn.clicked.connect(lambda: self.content_stack.setCurrentIndex(1))
        icon_layout.addWidget(settings_btn)
        
        icon_layout.addStretch()
        layout.addLayout(icon_layout)
        
        # Initialize with first app
        self._on_app_changed(self.app_combo.currentText())
        
        return sidebar
    
    def _create_import_view(self) -> QWidget:
        content = QWidget()
        content.setStyleSheet(f"background-color: {COLORS['bg_dark']};")
        
        layout = QVBoxLayout(content)
        layout.setContentsMargins(50, 40, 50, 35)
        layout.setSpacing(0)
        
        # Header
        title = QLabel("Odoo Data Migration Tool")
        title.setObjectName("titleLabel")
        layout.addWidget(title)
        layout.addSpacing(8)
        
        subtitle = QLabel("Map your file columns to Odoo fields. Select an app and model to begin.")
        subtitle.setObjectName("subtitleLabel")
        layout.addWidget(subtitle)
        layout.addSpacing(25)
        
        # File info row
        file_row = QHBoxLayout()
        self.file_info = QLabel("📄 No file loaded")
        self.file_info.setStyleSheet(f"color: {COLORS['text_secondary']}; font-size: 13px;")
        file_row.addWidget(self.file_info)
        file_row.addStretch()
        
        browse_btn = QPushButton("Browse")
        browse_btn.setObjectName("outlineBtn")
        browse_btn.clicked.connect(self._browse_file)
        file_row.addWidget(browse_btn)
        
        layout.addLayout(file_row)
        layout.addSpacing(15)
        
        # Quality Stats Dashboard
        self.quality_panel = QWidget()
        self.quality_panel.setStyleSheet(f"background-color: {COLORS['bg_card']}; border: 1px solid {COLORS['border']}; border-radius: 8px;")
        self.quality_panel.setFixedHeight(60)
        self.quality_panel.setVisible(False)  # Hidden until file loaded
        
        quality_layout = QHBoxLayout(self.quality_panel)
        quality_layout.setContentsMargins(20, 0, 20, 0)
        quality_layout.setSpacing(30)
        
        # Stats labels
        self.stat_valid = self._create_stat_badge("✓ Valid", "0", COLORS['accent'])
        self.stat_errors = self._create_stat_badge("✗ Errors", "0", COLORS['error'])
        self.stat_warnings = self._create_stat_badge("⚠ Warnings", "0", "#f59e0b")
        self.stat_duplicates = self._create_stat_badge("⊘ Duplicates", "0", "#8b5cf6")
        
        quality_layout.addWidget(self.stat_valid)
        quality_layout.addWidget(self.stat_errors)
        quality_layout.addWidget(self.stat_warnings)
        quality_layout.addWidget(self.stat_duplicates)
        quality_layout.addStretch()
        
        # Quality score
        self.quality_score_label = QLabel("Score: --")
        self.quality_score_label.setStyleSheet(f"color: {COLORS['text_secondary']}; font-size: 13px; font-weight: 600;")
        quality_layout.addWidget(self.quality_score_label)
        
        layout.addWidget(self.quality_panel)
        layout.addSpacing(15)
        
        # Table Header
        header = QWidget()
        header.setObjectName("tableHeader")
        header.setFixedHeight(50)
        
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(24, 0, 24, 0)
        header_layout.setSpacing(0)
        
        for text, width in [("FILE COLUMN", 200), ("SAMPLE DATA", 220)]:
            lbl = QLabel(text)
            lbl.setStyleSheet(f"color: {COLORS['text_muted']}; font-size: 11px; font-weight: 600; letter-spacing: 0.5px; background: transparent;")
            lbl.setFixedWidth(width)
            header_layout.addWidget(lbl)
        
        header_layout.addSpacing(20)
        lbl = QLabel("ODOO FIELD")
        lbl.setStyleSheet(f"color: {COLORS['text_muted']}; font-size: 11px; font-weight: 600; letter-spacing: 0.5px; background: transparent;")
        header_layout.addWidget(lbl)
        header_layout.addStretch()
        
        layout.addWidget(header)
        
        # Table Container
        table_container = QWidget()
        table_container.setObjectName("tableContainer")
        
        table_layout = QVBoxLayout(table_container)
        table_layout.setContentsMargins(0, 0, 0, 0)
        table_layout.setSpacing(0)
        
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        
        self.rows_widget = QWidget()
        self.rows_widget.setStyleSheet("background-color: transparent;")
        self.rows_layout = QVBoxLayout(self.rows_widget)
        self.rows_layout.setContentsMargins(0, 0, 0, 0)
        self.rows_layout.setSpacing(0)
        self.rows_layout.addStretch()
        
        scroll.setWidget(self.rows_widget)
        table_layout.addWidget(scroll)
        
        layout.addWidget(table_container, 1)
        layout.addSpacing(15)
        
        # Footer
        footer = QWidget()
        footer.setObjectName("footer")
        footer.setFixedHeight(70)
        footer.setStyleSheet(f"background-color: {COLORS['bg_card']}; border-top: 1px solid {COLORS['border']};")
        
        footer_layout = QHBoxLayout(footer)
        footer_layout.setContentsMargins(24, 12, 24, 12)
        footer_layout.setSpacing(12)
        
        # Progress bar (left side)
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setFixedWidth(200)
        self.progress_bar.setFixedHeight(6)
        self.progress_bar.setTextVisible(False)
        footer_layout.addWidget(self.progress_bar)
        
        # Progress text
        self.progress_text = QLabel("")
        self.progress_text.setStyleSheet(f"color: {COLORS['accent']}; font-size: 12px; font-weight: 500;")
        self.progress_text.setMinimumWidth(100)
        footer_layout.addWidget(self.progress_text)
        
        footer_layout.addStretch()
        
        # Status
        self.status_label = QLabel("Ready")
        self.status_label.setStyleSheet(f"color: {COLORS['text_secondary']}; font-size: 12px;")
        footer_layout.addWidget(self.status_label)
        
        footer_layout.addSpacing(20)
        
        # Buttons - fixed sizes
        self.validate_btn = QPushButton("Validate")
        self.validate_btn.setObjectName("outlineBtn")
        self.validate_btn.setFixedSize(90, 36)
        self.validate_btn.setEnabled(False)
        self.validate_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.validate_btn.clicked.connect(self._validate)
        footer_layout.addWidget(self.validate_btn)
        
        self.rollback_btn = QPushButton("Undo")
        self.rollback_btn.setObjectName("outlineBtn")
        self.rollback_btn.setFixedSize(70, 36)
        self.rollback_btn.setEnabled(False)
        self.rollback_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.rollback_btn.clicked.connect(self._rollback_import)
        footer_layout.addWidget(self.rollback_btn)
        
        self.import_btn = QPushButton("Import")
        self.import_btn.setObjectName("primaryBtn")
        self.import_btn.setFixedSize(90, 36)
        self.import_btn.setEnabled(False)
        self.import_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.import_btn.clicked.connect(self._start_import)
        footer_layout.addWidget(self.import_btn)
        
        layout.addWidget(footer)
        
        return content
    
    def _create_settings_view(self) -> QWidget:
        content = QWidget()
        content.setStyleSheet(f"background-color: {COLORS['bg_dark']};")
        
        layout = QVBoxLayout(content)
        layout.setContentsMargins(50, 40, 50, 35)
        layout.setSpacing(0)
        
        title = QLabel("Connection Settings")
        title.setObjectName("titleLabel")
        layout.addWidget(title)
        layout.addSpacing(8)
        
        subtitle = QLabel("Configure your Odoo server credentials.")
        subtitle.setObjectName("subtitleLabel")
        layout.addWidget(subtitle)
        layout.addSpacing(35)
        
        form_layout = QVBoxLayout()
        form_layout.setSpacing(15)
        
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("Server URL (e.g., http://localhost:8069)")
        self.url_input.setFixedWidth(450)
        form_layout.addWidget(self.url_input)
        
        self.db_input = QLineEdit()
        self.db_input.setPlaceholderText("Database Name")
        self.db_input.setFixedWidth(450)
        form_layout.addWidget(self.db_input)
        
        self.user_input = QLineEdit()
        self.user_input.setPlaceholderText("Username")
        self.user_input.setFixedWidth(450)
        form_layout.addWidget(self.user_input)
        
        self.pass_input = QLineEdit()
        self.pass_input.setPlaceholderText("Password / API Key")
        self.pass_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.pass_input.setFixedWidth(450)
        form_layout.addWidget(self.pass_input)
        
        layout.addLayout(form_layout)
        layout.addSpacing(30)
        
        btn_layout = QHBoxLayout()
        
        connect_btn = QPushButton("Test & Connect")
        connect_btn.setObjectName("primaryBtn")
        connect_btn.clicked.connect(self._connect)
        btn_layout.addWidget(connect_btn)
        
        save_btn = QPushButton("Save")
        save_btn.setObjectName("outlineBtn")
        save_btn.clicked.connect(self._save_settings)
        btn_layout.addWidget(save_btn)
        
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        layout.addStretch()
        
        return content
    
    # ============== Event Handlers ==============
    
    def _on_app_changed(self, app_name: str):
        # Clear existing model buttons
        for btn in self.model_buttons:
            btn.deleteLater()
        self.model_buttons.clear()
        
        # Add new model buttons
        if app_name in ODOO_APPS:
            for i, model in enumerate(ODOO_APPS[app_name]):
                label = MODEL_LABELS.get(model, model)
                btn = QPushButton(f"›   {label}")
                btn.setObjectName("modelBtn")
                btn.setProperty("model", model)
                btn.clicked.connect(lambda checked, m=model, l=label: self._select_model(m, l))
                self.model_buttons_layout.addWidget(btn)
                self.model_buttons.append(btn)
    
    def _filter_models(self, text: str):
        text = text.lower()
        for btn in self.model_buttons:
            btn.setVisible(text in btn.text().lower())
    
    def _select_model(self, model: str, label: str):
        # Update button styles
        for btn in self.model_buttons:
            if btn.property("model") == model:
                btn.setObjectName("modelBtnActive")
                btn.setText(f"✓   {label}")
            else:
                btn.setObjectName("modelBtn")
                lbl = MODEL_LABELS.get(btn.property("model"), btn.property("model"))
                btn.setText(f"›   {lbl}")
            btn.setStyleSheet(btn.styleSheet())  # Force refresh
        
        self.current_model = model
        
        if not self.client:
            self._set_status("Connect to Odoo first", error=True)
            return
        
        self._set_status(f"Loading {label}...")
        
        # Load fields in background
        def load_fields():
            meta = self.inspector.get_model(model)
            return {
                f.name: {"label": f.label, "type": f.field_type.value, "required": f.required}
                for f in meta.fields.values() if f.importable
            } if meta else {}
        
        self.worker = WorkerThread(load_fields)
        self.worker.finished.connect(self._on_fields_loaded)
        self.worker.error.connect(lambda e: self._set_status(f"Error: {e[:40]}", error=True))
        self.worker.start()
    
    def _on_fields_loaded(self, fields_data: dict):
        self.fields_data = fields_data
        self._set_status(f"Loaded {len(fields_data)} fields")
        if self.file_columns:
            self._build_mappings()
        
        # Refresh template list for this model
        self._refresh_templates()
    
    def _connect(self):
        self._set_status("Connecting...")
        
        def do_connect():
            client = OdooClient(
                url=self.url_input.text() or "http://localhost:8069",
                db=self.db_input.text(),
                username=self.user_input.text(),
                password=self.pass_input.text(),
            )
            version = client.version()
            client.authenticate()
            return client, version
        
        self.worker = WorkerThread(do_connect)
        self.worker.finished.connect(self._on_connected)
        self.worker.error.connect(self._on_connect_error)
        self.worker.start()
    
    def _on_connected(self, result):
        self.client, version = result
        self.inspector = SchemaInspector(self.client, cache=SchemaCache())
        
        self.conn_dot.setStyleSheet(f"color: {COLORS['accent']}; font-size: 16px;")
        v = version.get('server_version', '?').split('.')[0]
        self.conn_status.setText(f"Connected: Odoo v{v}")
        self.conn_status.setObjectName("statusConnected")
        self.conn_status.setStyleSheet(self.conn_status.styleSheet())
        
        self._set_status("Connected successfully")
        self.content_stack.setCurrentIndex(0)
    
    def _on_connect_error(self, error: str):
        self.conn_dot.setStyleSheet(f"color: {COLORS['error']}; font-size: 16px;")
        self.conn_status.setText("Connection Failed")
        self.conn_status.setObjectName("statusError")
        self._set_status(f"Error: {error[:40]}", error=True)
    
    def _save_settings(self):
        self.settings['connection'] = {
            'url': self.url_input.text(),
            'database': self.db_input.text(),
            'username': self.user_input.text(),
            'password': self.pass_input.text(),
        }
        save_settings(self.settings)
        self._set_status("Settings saved")
    
    def _browse_file(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Select File", "",
            "Spreadsheet Files (*.xlsx *.xls *.csv);;All Files (*)"
        )
        if path:
            self.file_path = path
            self._load_file(path)
    
    def _load_file(self, path: str):
        self._set_status("Loading file...")
        
        def do_load():
            reader = DataReader()
            result = reader.read_file(Path(path))
            columns = [c for c in result.data.columns if not c.startswith("_")]
            records = result.data.head(5).to_dict("records")
            return columns, records, result.total_rows
        
        self.worker = WorkerThread(do_load)
        self.worker.finished.connect(self._on_file_loaded)
        self.worker.error.connect(lambda e: self._set_status(f"Error: {e[:40]}", error=True))
        self.worker.start()
    
    def _on_file_loaded(self, result):
        self.file_columns, self.file_records, total_rows = result
        self.file_info.setText(f"📄 {Path(self.file_path).name} • {total_rows} rows • {len(self.file_columns)} columns")
        self.file_info.setStyleSheet(f"color: {COLORS['text_primary']}; font-size: 13px;")
        
        # Run quality analysis
        self._analyze_quality()
        
        self._set_status("File loaded")
        self.validate_btn.setEnabled(True)
        self.import_btn.setEnabled(True)
        
        if self.fields_data:
            self._build_mappings()
    
    def _build_mappings(self):
        # Clear existing rows
        for row in self.mapping_rows:
            row.deleteLater()
        self.mapping_rows.clear()
        
        # Build field options with icons
        options = []
        for name, info in sorted(self.fields_data.items(), key=lambda x: x[1]["label"]):
            # Get icon for this field
            icon = FIELD_ICONS.get(name, "")
            if not icon:
                # Try by type
                field_type = info.get("type", "char")
                icon = FIELD_ICONS.get(f"_{field_type}", "≡")
            
            prefix = "* " if info["required"] else ""
            options.append(f"{icon}  {prefix}{info['label']}")
        
        sample = self.file_records[0] if self.file_records else {}
        
        for i, col in enumerate(self.file_columns):
            matched = self._find_match(col)
            sample_val = str(sample.get(col, ""))
            
            row = MappingRow(col, sample_val, options, self.fields_data, matched, is_odd=(i % 2 == 0))
            self.rows_layout.insertWidget(i, row)
            self.mapping_rows.append(row)
    
    def _find_match(self, col: str) -> str:
        col_l = col.lower().strip().replace(" ", "_")
        for name, info in self.fields_data.items():
            if name == col or name.lower() == col_l or info["label"].lower() == col.lower():
                # Get icon
                icon = FIELD_ICONS.get(name, "")
                if not icon:
                    field_type = info.get("type", "char")
                    icon = FIELD_ICONS.get(f"_{field_type}", "≡")
                prefix = "* " if info["required"] else ""
                return f"{icon}  {prefix}{info['label']}"
        return ""
    
    def _validate(self):
        self._set_status("Validating...")
        
        try:
            mapping = {r.file_col: r.get_mapping()[1] for r in self.mapping_rows if r.get_mapping()[1]}
            reader = DataReader()
            result = reader.read_file(Path(self.file_path), mapping=mapping)
            cleaner = DataCleaner()
            df = cleaner.clean(result.data)
            self._set_status(f"Validation successful: {len(df)} valid records")
        except Exception as e:
            self._set_status(f"Validation failed: {str(e)[:40]}", error=True)
    
    def _start_import(self):
        if not self.client:
            self._set_status("Connect to Odoo first", error=True)
            return
        
        if not self.current_model:
            self._set_status("Select a model first", error=True)
            return
        
        self.import_btn.setEnabled(False)
        self.validate_btn.setEnabled(False)
        self._set_status("Importing...")
        
        # Get mappings
        mapping = {}
        for row in self.mapping_rows:
            file_col, field = row.get_mapping()
            if field:
                # Extract field name from label
                for name, info in self.fields_data.items():
                    if field.replace("* ", "") == info["label"]:
                        mapping[file_col] = name
                        break
        
        def do_import():
            reader = DataReader()
            result = reader.read_file(Path(self.file_path), mapping=mapping)
            cleaner = DataCleaner()
            records = cleaner.clean(result.data).to_dict("records")
            
            cache = ReferenceCache()
            adapter = get_adapter(self.current_model, self.client, cache)
            
            prepared = []
            for r in records:
                p = adapter.prepare_record(r)
                if p:
                    prepared.append(p)
            
            BATCH = 50
            total = len(prepared)
            created = 0
            created_ids = []  # Track IDs for rollback
            
            for b_start in range(0, total, BATCH):
                b_end = min(b_start + BATCH, total)
                batch = prepared[b_start:b_end]
                # Create records one by one to get IDs
                for record in batch:
                    try:
                        record_id = self.client.create(self.current_model, record)
                        if record_id:
                            created_ids.append(record_id)
                            created += 1
                    except Exception:
                        pass  # Continue on error
            
            return created, total, created_ids
        
        self.worker = WorkerThread(do_import)
        self.worker.finished.connect(self._on_import_finished)
        self.worker.error.connect(self._on_import_error)
        self.worker.start()
        
        # Progress timer
        self.progress_timer = QTimer()
        self.progress_timer.timeout.connect(self._update_progress)
        self.progress_timer.start(100)
    
    def _update_progress(self):
        # Animated progress for now
        v = self.progress_bar.value()
        if v < 95:
            self.progress_bar.setValue(v + 1)
            self.progress_text.setText(f"{v+1}% Importing Records...")
    
    def _on_import_finished(self, result):
        self.progress_timer.stop()
        created, total, created_ids = result
        
        # Store for rollback
        self.last_import_ids = created_ids
        self.last_import_model = self.current_model
        self.rollback_btn.setEnabled(len(created_ids) > 0)
        
        self.progress_bar.setValue(100)
        self.progress_text.setText(f"100% Complete: {created}/{total}")
        self._set_status(f"Import successful: {created} records created")
        self.import_btn.setEnabled(True)
        self.validate_btn.setEnabled(True)
    
    def _on_import_error(self, error: str):
        self.progress_timer.stop()
        self._set_status(f"Import failed: {error[:40]}", error=True)
        self.import_btn.setEnabled(True)
        self.validate_btn.setEnabled(True)
    
    def _set_status(self, text: str, error: bool = False):
        color = COLORS['error'] if error else COLORS['text_secondary']
        self.status_label.setText(f"Status: {text}")
        self.status_label.setStyleSheet(f"color: {color}; font-size: 13px;")
    
    def _create_stat_badge(self, label: str, value: str, color: str) -> QWidget:
        """Create a stat badge widget for the quality dashboard."""
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        
        value_lbl = QLabel(value)
        value_lbl.setObjectName("statValue")
        value_lbl.setStyleSheet(f"color: {color}; font-size: 18px; font-weight: 700; background: transparent;")
        layout.addWidget(value_lbl)
        
        label_lbl = QLabel(label)
        label_lbl.setStyleSheet(f"color: {COLORS['text_secondary']}; font-size: 12px; background: transparent;")
        layout.addWidget(label_lbl)
        
        widget.value_label = value_lbl  # Store reference for updates
        return widget
    
    def _update_stat_badge(self, badge: QWidget, value: int):
        """Update a stat badge value."""
        if hasattr(badge, 'value_label'):
            badge.value_label.setText(str(value))
    
    def _analyze_quality(self):
        """Run quality analysis on loaded file data."""
        if not self.file_records:
            return
        
        # Validate fields
        validation_results = self.field_validator.validate_records(self.file_records)
        
        # Get required fields from current model
        required_fields = []
        if self.fields_data:
            required_fields = [name for name, info in self.fields_data.items() if info.get("required")]
        
        # Analyze quality
        self.quality_stats = self.quality_analyzer.analyze(
            records=self.file_records,
            required_fields=required_fields,
            validation_results=validation_results,
        )
        
        # Update dashboard
        self._update_quality_dashboard()
    
    def _update_quality_dashboard(self):
        """Update the quality dashboard with current stats."""
        if not self.quality_stats:
            self.quality_panel.setVisible(False)
            return
        
        self.quality_panel.setVisible(True)
        
        stats = self.quality_stats
        self._update_stat_badge(self.stat_valid, stats.valid_rows)
        self._update_stat_badge(self.stat_errors, stats.error_rows)
        self._update_stat_badge(self.stat_warnings, stats.warning_rows)
        self._update_stat_badge(self.stat_duplicates, stats.duplicate_rows)
        
        # Quality score with color
        score = stats.quality_score
        if score >= 90:
            score_color = COLORS['accent']
        elif score >= 70:
            score_color = "#f59e0b"
        else:
            score_color = COLORS['error']
        
        self.quality_score_label.setText(f"Score: {score:.0f}%")
        self.quality_score_label.setStyleSheet(f"color: {score_color}; font-size: 13px; font-weight: 600;")

    
    # ============== Template Methods ==============
    
    def _refresh_templates(self):
        """Reload template dropdown for current model."""
        self.template_combo.blockSignals(True)
        self.template_combo.clear()
        self.template_combo.addItem("📋 Templates...")
        
        if self.current_model:
            templates = self.template_manager.list_templates(model=self.current_model)
            for t in templates:
                self.template_combo.addItem(f"📄 {t.name}")
        
        self.template_combo.blockSignals(False)
    
    def _on_template_selected(self, text: str):
        """Load selected template mappings."""
        if text.startswith("📄 "):
            name = text[2:].strip()
            template = self.template_manager.load_template(name)
            if template:
                self._apply_template(template)
    
    def _apply_template(self, template):
        """Apply template mappings to current rows."""
        for row in self.mapping_rows:
            file_col = row.file_col
            if file_col in template.mappings:
                target_field = template.mappings[file_col]
                # Find matching dropdown item
                for i in range(row.dropdown.count()):
                    item_text = row.dropdown.itemText(i)
                    if target_field in item_text:
                        row.dropdown.setCurrentIndex(i)
                        break
        self._set_status(f"Loaded template: {template.name}")
    
    def _save_template(self):
        """Save current mappings as template."""
        if not self.current_model:
            self._set_status("Select a model first", error=True)
            return
        
        name, ok = QInputDialog.getText(
            self, "Save Template", "Template name:",
            text=f"{MODEL_LABELS.get(self.current_model, self.current_model)} Import"
        )
        if not ok or not name:
            return
        
        # Gather mappings
        mappings = {}
        for row in self.mapping_rows:
            file_col, field = row.get_mapping()
            if field:
                # Extract field name from label
                for fname, info in self.fields_data.items():
                    if info["label"] in field:
                        mappings[file_col] = fname
                        break
        
        self.template_manager.save_template(
            name=name,
            model=self.current_model,
            mappings=mappings,
        )
        self._refresh_templates()
        self._set_status(f"Template saved: {name}")
    
    # ============== Rollback Method ==============
    
    def _rollback_import(self):
        """Delete all records from last import."""
        if not self.last_import_ids or not self.client:
            return
        
        count = len(self.last_import_ids)
        reply = QMessageBox.question(
            self,
            "Confirm Rollback",
            f"Delete {count} records created in the last import?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            self._set_status("Rolling back...")
            
            def do_rollback():
                self.client.unlink(self.last_import_model, self.last_import_ids)
                return len(self.last_import_ids)
            
            self.worker = WorkerThread(do_rollback)
            self.worker.finished.connect(self._on_rollback_finished)
            self.worker.error.connect(lambda e: self._set_status(f"Rollback failed: {e[:40]}", error=True))
            self.worker.start()
    
    def _on_rollback_finished(self, count):
        self.last_import_ids = []
        self.rollback_btn.setEnabled(False)
        self._set_status(f"Rollback complete: {count} records deleted")


def main():
    app = QApplication(sys.argv)
    font = QFont("Segoe UI", 10)
    app.setFont(font)
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
