# Odoo Data Migration Tool

A powerful CLI tool for cleaning, validating, and importing customer data into Odoo via XML-RPC/JSON-RPC APIs. Designed for Odoo implementation consultants to handle messy Excel/CSV data safely and efficiently.

## ✨ Features

- **🔒 Safe by Design**: Uses only Odoo's official XML-RPC API - no direct database writes
- **📊 Rich Data Processing**: Handles CSV and Excel files with automatic encoding detection
- **✅ Pre-Import Validation**: Pydantic schemas validate data before any API calls
- **🔍 Smart Deduplication**: Detects duplicates within batches and against existing Odoo records
- **🔄 Batch Processing**: Configurable chunk sizes with retry logic and resume capability
- **📝 Comprehensive Logging**: Per-record audit trail with JSON/CSV export
- **🎯 Dry-Run Mode**: Full validation without creating any records
- **🚀 Consultant-Friendly**: Clear error messages and human-readable reports

## 📦 Installation

### Requirements

- Python 3.11+
- Access to an Odoo instance (v16-v19)

### Install

```bash
# Clone or download the tool
cd migration_tool

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Or install as package
pip install -e .
```

## 🚀 Quick Start

### 1. Set Environment Variables

```bash
# Windows PowerShell
$env:ODOO_URL = "https://your-odoo.com"
$env:ODOO_DB = "your_database"
$env:ODOO_USER = "admin"
$env:ODOO_PASSWORD = "your_password"
```

Or create a `.env` file in the project root:

```env
ODOO_URL=https://your-odoo.com
ODOO_DB=your_database
ODOO_USER=admin
ODOO_PASSWORD=your_password
```

### 2. Test Connection

```bash
python -m migration_tool test-connection config/example.yaml
```

### 3. Create Configuration

```bash
# Generate example config
python -m migration_tool init-config my_config.yaml
```

### 4. Run Dry-Run First

```bash
# Validate without importing
python -m migration_tool run config/example.yaml --dry-run
```

### 5. Run Live Import

```bash
# Import data
python -m migration_tool run config/example.yaml
```

## 📋 Commands

| Command | Description |
|---------|-------------|
| `run <config>` | Run migration (use `--dry-run` for validation) |
| `validate <config>` | Validate data offline (no Odoo connection) |
| `dedupe <config>` | Show duplicate detection report |
| `test-connection <config>` | Test Odoo connectivity and access rights |
| `init-config <output>` | Create example configuration file |
| `version` | Show version information |

### Common Options

```bash
# Dry run (validate only)
python -m migration_tool run config.yaml --dry-run

# Import specific model only
python -m migration_tool run config.yaml --model res.partner

# Resume interrupted import
python -m migration_tool run config.yaml --resume

# Verbose output
python -m migration_tool run config.yaml --verbose
```

## ⚙️ Configuration

### Basic Structure

```yaml
name: my_migration
version: "1.0"

odoo:
  url: ${ODOO_URL}          # Environment variable
  database: ${ODOO_DB}
  username: ${ODOO_USER}
  password: ${ODOO_PASSWORD}

import:
  chunk_size: 500           # Records per batch
  retry_attempts: 3
  dry_run: false

models:
  - model: res.partner
    source: data/customers.xlsx
    sheet: Sheet1
    mapping:
      "Column Name": field_name
    # ... more options
```

### Model Configuration Options

```yaml
models:
  - model: res.partner
    source: customers.xlsx      # CSV or Excel file
    sheet: Sheet1               # Excel sheet name (optional)
    enabled: true               # Enable/disable
    priority: 10                # Import order (lower = first)
    
    # Column mapping: Source Column -> Odoo Field
    mapping:
      Customer Name: name
      Email Address: email
      Phone Number: phone
    
    # Deduplication
    dedupe:
      keys: [name, phone]       # Fields to match
      strategy: update          # skip | update | create
      case_sensitive: false
      match_odoo: true          # Check Odoo records
      match_batch: true         # Check within batch
    
    # Data transforms
    transforms:
      phone: normalize_phone
      email: normalize_email
      date: normalize_date
    
    # Validation
    validation:
      required: [name]
    
    # Default values
    defaults:
      customer_rank: 1
      is_company: false
```

### Available Transforms

| Transform | Description |
|-----------|-------------|
| `normalize_phone` | Standardize phone format |
| `normalize_email` | Lowercase, validate format |
| `normalize_date` | Parse to ISO format (YYYY-MM-DD) |
| `normalize_boolean` | Convert yes/no, 1/0, true/false |
| `normalize_numeric` | Handle various number formats |
| `normalize_currency` | Strip currency symbols |
| `normalize_uom` | Map UoM aliases to Odoo names |
| `clean_whitespace` | Trim and normalize spaces |
| `uppercase` / `lowercase` / `titlecase` | Case transforms |
| `strip_html` | Remove HTML tags |

## 📊 Supported Models

| Model | Description |
|-------|-------------|
| `res.partner` | Customers & Vendors |
| `product.template` | Products |
| `product.product` | Product Variants |
| `product.category` | Product Categories |
| `uom.uom` | Units of Measure |
| `account.account` | Chart of Accounts |
| `account.move` | Journal Entries (⚠️ use with caution) |

## 📁 Sample Data Format

### customers.csv

```csv
Customer Name,Email,Phone,Street,City,Country
Acme Corp,contact@acme.com,+1-555-0100,123 Main St,New York,United States
Beta Inc,info@beta.com,+1-555-0200,456 Oak Ave,Los Angeles,United States
```

### products.xlsx

| Product Name | SKU | Category | Sales Price | Cost |
|-------------|-----|----------|-------------|------|
| Widget A | WGT-001 | Electronics | 99.99 | 45.00 |
| Gadget B | GDT-002 | Accessories | 149.99 | 72.50 |

## 📝 Logging & Reports

Logs are saved to the `logs/` directory:

- `migration_YYYYMMDD_HHMMSS.json` - Full audit trail
- `migration_YYYYMMDD_HHMMSS.csv` - Tabular format
- `migration_summary.txt` - Human-readable summary

### Sample Summary

```
============================================================
MIGRATION SUMMARY: customer_import
============================================================
Status:              COMPLETED
Started:             2024-12-22 15:30:00
Duration:            2m 45s

RECORD STATISTICS
----------------------------------------
Total Records:       1,500
Successful:          1,485 (99.0%)
Failed:              10
Skipped:             5

MODELS PROCESSED
----------------------------------------
  • res.partner
  • product.template
============================================================
```

## 🔐 Security

- **No hardcoded credentials**: Use environment variables
- **Dry-run by default**: Test before live imports
- **Read-only validation**: Offline validation doesn't need Odoo access
- **Journal entry guards**: Extra confirmation required for account.move

## 🛠️ Troubleshooting

### Connection Issues

```bash
# Test connection
python -m migration_tool test-connection config.yaml

# Check environment variables
echo $env:ODOO_URL
```

### Validation Errors

```bash
# Validate offline (no Odoo needed)
python -m migration_tool validate config.yaml --output errors.csv
```

### Resume Failed Import

```bash
# Resume from last successful batch
python -m migration_tool run config.yaml --resume
```

## 📚 Best Practices

1. **Always dry-run first** - Catch errors before creating records
2. **Import in order** - Categories before products, partners before invoices
3. **Use deduplication** - Avoid duplicate records in Odoo
4. **Review logs** - Check the summary report after each import
5. **Backup first** - Always backup Odoo before large imports
6. **Start small** - Test with a subset of data first

## 📄 License

MIT License - Built for Odoo implementation consultants.
