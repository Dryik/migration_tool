"""
CLI Entry Point

Typer-based command line interface for the Odoo Migration Tool.
"""

from pathlib import Path
from typing import Optional
from datetime import datetime

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

from migration_tool.config import ConfigLoader, MigrationConfig
from migration_tool.odoo import OdooClient, OdooConnectionError
from migration_tool.odoo.adapters import get_adapter, ReferenceCache
from migration_tool.core.reader import DataReader
from migration_tool.core.cleaner import DataCleaner
from migration_tool.core.validator import ValidationEngine
from migration_tool.core.deduplicator import Deduplicator, DedupeAction
from migration_tool.core.batcher import BatchProcessor
from migration_tool.core.logger import MigrationLogger


# Initialize Typer app
app = typer.Typer(
    name="migrate",
    help="Odoo Data Migration Tool - Clean, validate, and import data into Odoo",
    add_completion=False,
)

console = Console()


def load_config(config_path: str) -> MigrationConfig:
    """Load and validate configuration file."""
    loader = ConfigLoader()
    try:
        return loader.load(config_path)
    except FileNotFoundError:
        console.print(f"[red]Error:[/] Configuration file not found: {config_path}")
        raise typer.Exit(1)
    except ValueError as e:
        console.print(f"[red]Configuration error:[/] {e}")
        raise typer.Exit(1)


def create_client(config: MigrationConfig) -> OdooClient:
    """Create and authenticate Odoo client."""
    try:
        client = OdooClient(
            url=config.odoo.url,
            db=config.odoo.database,
            username=config.odoo.username,
            password=config.odoo.password,
            timeout=config.odoo.timeout,
            retry_attempts=config.import_settings.retry_attempts,
            retry_delay=config.import_settings.retry_delay,
        )
        client.authenticate()
        return client
    except OdooConnectionError as e:
        console.print(f"[red]Connection error:[/] {e}")
        raise typer.Exit(1)


@app.command()
def run(
    config_path: str = typer.Argument(..., help="Path to configuration file (YAML or JSON)"),
    dry_run: bool = typer.Option(False, "--dry-run", "-n", help="Validate without creating records"),
    model: Optional[str] = typer.Option(None, "--model", "-m", help="Only import specific model"),
    resume: bool = typer.Option(False, "--resume", "-r", help="Resume from last state"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """
    Run a migration based on the configuration file.
    
    Example:
        migrate run config/customers.yaml --dry-run
    """
    # Load configuration
    config = load_config(config_path)
    
    # Filter models if specified
    models_to_import = config.models
    if model:
        models_to_import = [m for m in models_to_import if m.model == model]
        if not models_to_import:
            console.print(f"[red]Error:[/] Model '{model}' not found in configuration")
            raise typer.Exit(1)
    
    # Show run info
    mode_text = "[yellow]DRY RUN[/]" if dry_run else "[green]LIVE IMPORT[/]"
    console.print(Panel(
        f"[bold]Migration: {config.name}[/]\n"
        f"Mode: {mode_text}\n"
        f"Models: {', '.join(m.model for m in models_to_import)}\n"
        f"Target: {config.odoo.url} / {config.odoo.database}",
        title="Migration Configuration",
    ))
    
    if not dry_run:
        confirm = typer.confirm("Proceed with live import?", default=False)
        if not confirm:
            console.print("[yellow]Aborted by user[/]")
            raise typer.Exit(0)
    
    # Initialize components
    client = create_client(config)
    reader = DataReader()
    cleaner = DataCleaner()
    validator = ValidationEngine(client)
    deduplicator = Deduplicator(client)
    cache = ReferenceCache()
    
    # Setup logging
    logger = MigrationLogger(
        output_dir=config.logging.output_dir,
        console_output=config.logging.console_progress,
    )
    logger.start_migration(config.name, dry_run=dry_run)
    
    # Process each model
    for model_config in models_to_import:
        if not model_config.enabled:
            logger.log_info(f"Skipping disabled model: {model_config.model}")
            continue
        
        console.print(f"\n[bold blue]Processing: {model_config.model}[/]")
        
        try:
            # 1. Read data
            source_path = Path(model_config.source)
            if not source_path.is_absolute():
                source_path = Path(config_path).parent / source_path
            
            read_result = reader.read_file(
                source_path,
                mapping=model_config.mapping,
                sheet=model_config.sheet,
            )
            
            if read_result.errors:
                for error in read_result.errors:
                    console.print(f"  [red]Read error:[/] {error}")
                continue
            
            for warning in read_result.warnings:
                console.print(f"  [yellow]Warning:[/] {warning}")
            
            console.print(f"  Read {read_result.total_rows} records from {source_path.name}")
            
            # 2. Clean data
            df = cleaner.clean(read_result.data, transforms=model_config.transforms)
            records = df.to_dict("records")
            
            # 3. Apply defaults
            for record in records:
                for field, default_value in model_config.defaults.items():
                    if field not in record or record[field] is None:
                        record[field] = default_value
            
            # 4. Validate
            validation_result = validator.validate(
                records,
                required=model_config.validation.get("required", []),
            )
            
            if not validation_result.is_valid:
                console.print(f"  [red]Validation failed: {validation_result.error_count} errors[/]")
                for issue in validation_result.issues[:10]:  # Show first 10
                    console.print(f"    Row {issue.row}: {issue.message}")
                if validation_result.error_count > 10:
                    console.print(f"    ... and {validation_result.error_count - 10} more errors")
                continue
            
            console.print(f"  [green]Validation passed[/] ({validation_result.warning_count} warnings)")
            
            # 5. Deduplicate
            if model_config.dedupe:
                strategy = DedupeAction(model_config.dedupe.strategy)
                dedupe_result = deduplicator.find_duplicates(
                    validation_result.valid_records,
                    model=model_config.model,
                    keys=model_config.dedupe.keys,
                    strategy=strategy,
                    case_sensitive=model_config.dedupe.case_sensitive,
                    check_odoo=model_config.dedupe.match_odoo,
                    check_batch=model_config.dedupe.match_batch,
                )
                
                if dedupe_result.total_duplicates > 0:
                    console.print(
                        f"  [yellow]Duplicates found:[/] "
                        f"{dedupe_result.odoo_duplicates} in Odoo, "
                        f"{dedupe_result.batch_duplicates} in batch"
                    )
                
                records_to_import = dedupe_result.unique_records + dedupe_result.update_records
            else:
                records_to_import = validation_result.valid_records
            
            console.print(f"  [blue]Records to import: {len(records_to_import)}[/]")
            
            # 6. Batch import
            if records_to_import:
                adapter = get_adapter(model_config.model, client, cache)
                
                # Special handling for journal entries
                if model_config.model == "account.move":
                    console.print("  [yellow]⚠️  Journal entry import requires extra confirmation[/]")
                    if not dry_run:
                        confirm_je = typer.confirm(
                            "This will create journal entries. Are you sure?",
                            default=False
                        )
                        if not confirm_je:
                            console.print("  [yellow]Skipped journal entries[/]")
                            continue
                        adapter.confirm_safety()  # type: ignore
                
                batch_processor = BatchProcessor(
                    client,
                    chunk_size=config.import_settings.chunk_size,
                    retry_attempts=config.import_settings.retry_attempts,
                    retry_delay=config.import_settings.retry_delay,
                )
                
                def on_progress(current: int, total: int, msg: str) -> None:
                    if verbose:
                        console.print(f"    {msg}")
                
                if resume:
                    batch_result = batch_processor.resume(
                        records_to_import,
                        model=model_config.model,
                        adapter=adapter,
                        dry_run=dry_run,
                        on_progress=on_progress,
                    )
                else:
                    batch_result = batch_processor.process(
                        records_to_import,
                        model=model_config.model,
                        adapter=adapter,
                        dry_run=dry_run,
                        stop_on_error=config.import_settings.stop_on_error,
                        on_progress=on_progress,
                    )
                
                # Report results
                if dry_run:
                    console.print(f"  [green]✓ Dry run complete[/] - {batch_result.processed_records} validated")
                else:
                    console.print(
                        f"  [green]✓ Created: {batch_result.created_records}[/] | "
                        f"[yellow]Failed: {batch_result.failed_records}[/]"
                    )
                    if batch_result.records_per_second:
                        console.print(f"    Speed: {batch_result.records_per_second:.1f} records/sec")
                
        except Exception as e:
            console.print(f"  [red]Error processing {model_config.model}:[/] {e}")
            logger.log_error(str(e))
            if config.import_settings.stop_on_error:
                break
    
    # Finish and show summary
    summary = logger.end_migration()
    
    if config.logging.export_json:
        console.print(f"\nLogs exported to: {config.logging.output_dir}/")


@app.command()
def validate(
    config_path: str = typer.Argument(..., help="Path to configuration file"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output report to file"),
):
    """
    Validate data without connecting to Odoo.
    
    Performs local validation only (schema, required fields, data types).
    Does not check references against Odoo.
    """
    config = load_config(config_path)
    reader = DataReader()
    cleaner = DataCleaner()
    validator = ValidationEngine()  # No client for offline validation
    
    all_issues: list[tuple[str, str, int, str]] = []  # (model, file, row, message)
    
    for model_config in config.models:
        if not model_config.enabled:
            continue
        
        console.print(f"\n[bold]Validating: {model_config.model}[/]")
        
        source_path = Path(model_config.source)
        if not source_path.is_absolute():
            source_path = Path(config_path).parent / source_path
        
        try:
            read_result = reader.read_file(
                source_path,
                mapping=model_config.mapping,
                sheet=model_config.sheet,
            )
            
            df = cleaner.clean(read_result.data, transforms=model_config.transforms)
            records = df.to_dict("records")
            
            validation_result = validator.validate(
                records,
                required=model_config.validation.get("required", []),
            )
            
            # Collect issues
            for issue in validation_result.issues:
                all_issues.append((
                    model_config.model,
                    str(source_path.name),
                    issue.row,
                    issue.message,
                ))
            
            if validation_result.is_valid:
                console.print(f"  [green]✓ Valid[/] ({len(records)} records)")
            else:
                console.print(f"  [red]✗ {validation_result.error_count} errors[/]")
                
        except Exception as e:
            console.print(f"  [red]Error:[/] {e}")
            all_issues.append((model_config.model, str(source_path), 0, str(e)))
    
    # Summary
    console.print(f"\n[bold]Total issues: {len(all_issues)}[/]")
    
    if output and all_issues:
        import csv
        with open(output, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Model", "File", "Row", "Message"])
            writer.writerows(all_issues)
        console.print(f"Report saved to: {output}")


@app.command()
def dedupe(
    config_path: str = typer.Argument(..., help="Path to configuration file"),
    model: Optional[str] = typer.Option(None, "--model", "-m", help="Only check specific model"),
    check_odoo: bool = typer.Option(True, "--check-odoo/--no-odoo", help="Check against Odoo"),
):
    """
    Show duplicate detection report without importing.
    """
    config = load_config(config_path)
    reader = DataReader()
    cleaner = DataCleaner()
    
    client = None
    if check_odoo:
        client = create_client(config)
    
    deduplicator = Deduplicator(client)
    
    for model_config in config.models:
        if not model_config.enabled:
            continue
        if model and model_config.model != model:
            continue
        if not model_config.dedupe:
            continue
        
        console.print(f"\n[bold]Checking duplicates: {model_config.model}[/]")
        
        source_path = Path(model_config.source)
        if not source_path.is_absolute():
            source_path = Path(config_path).parent / source_path
        
        try:
            read_result = reader.read_file(
                source_path,
                mapping=model_config.mapping,
                sheet=model_config.sheet,
            )
            
            df = cleaner.clean(read_result.data, transforms=model_config.transforms)
            records = df.to_dict("records")
            
            result = deduplicator.find_duplicates(
                records,
                model=model_config.model,
                keys=model_config.dedupe.keys,
                strategy=DedupeAction.SKIP,
                check_odoo=check_odoo and model_config.dedupe.match_odoo,
                check_batch=model_config.dedupe.match_batch,
            )
            
            console.print(deduplicator.get_duplicate_report(result))
            
        except Exception as e:
            console.print(f"  [red]Error:[/] {e}")


@app.command("test-connection")
def test_connection(
    config_path: str = typer.Argument(..., help="Path to configuration file"),
):
    """
    Test connection to Odoo server.
    """
    config = load_config(config_path)
    
    console.print(f"\n[bold]Testing connection to Odoo[/]")
    console.print(f"  URL: {config.odoo.url}")
    console.print(f"  Database: {config.odoo.database}")
    console.print(f"  Username: {config.odoo.username}")
    
    try:
        client = OdooClient(
            url=config.odoo.url,
            db=config.odoo.database,
            username=config.odoo.username,
            password=config.odoo.password,
        )
        
        # Get version first (no auth needed)
        version_info = client.version()
        console.print(f"\n  [green]✓ Server reachable[/]")
        console.print(f"  Odoo Version: {version_info.get('server_version', 'Unknown')}")
        
        # Authenticate
        uid = client.authenticate()
        console.print(f"  [green]✓ Authentication successful[/]")
        console.print(f"  User ID: {uid}")
        
        # Test model access
        models_to_check = ["res.partner", "product.template", "account.account"]
        console.print(f"\n  [bold]Access rights:[/]")
        
        for model in models_to_check:
            try:
                can_read = client.check_access_rights(model, "read")
                can_write = client.check_access_rights(model, "write")
                can_create = client.check_access_rights(model, "create")
                
                access = []
                if can_read:
                    access.append("read")
                if can_write:
                    access.append("write")
                if can_create:
                    access.append("create")
                
                console.print(f"    {model}: [green]{', '.join(access)}[/]")
            except Exception:
                console.print(f"    {model}: [red]No access[/]")
        
        console.print(f"\n[green]Connection test successful![/]")
        
    except OdooConnectionError as e:
        console.print(f"\n[red]Connection failed:[/] {e}")
        raise typer.Exit(1)


@app.command("init-config")
def init_config(
    output: str = typer.Argument("config.yaml", help="Output configuration file path"),
):
    """
    Create an example configuration file.
    """
    ConfigLoader.create_example_config(output)
    console.print(f"[green]Example configuration created:[/] {output}")
    console.print("\nEdit this file and set the following environment variables:")
    console.print("  - ODOO_URL")
    console.print("  - ODOO_DB")
    console.print("  - ODOO_USER")
    console.print("  - ODOO_PASSWORD")


@app.command("inspect-schema")
def inspect_schema(
    config_path: str = typer.Argument(..., help="Path to configuration file"),
    model: str = typer.Option(..., "--model", "-m", help="Model to inspect (e.g., res.partner)"),
    show_all: bool = typer.Option(False, "--all", "-a", help="Show all fields, not just importable"),
    refresh: bool = typer.Option(False, "--refresh", help="Force refresh from Odoo (ignore cache)"),
):
    """
    Inspect Odoo model schema dynamically.
    
    Shows field information including importability, types, and relations.
    Results are cached for performance.
    
    Example:
        migrate inspect-schema config.yaml --model res.partner
    """
    from migration_tool.core.schema import SchemaInspector, SchemaCache
    
    config = load_config(config_path)
    client = create_client(config)
    
    console.print(f"\n[bold]Inspecting schema for: {model}[/]")
    
    # Create inspector
    cache = SchemaCache()
    inspector = SchemaInspector(client, cache=cache)
    
    # Get model metadata
    model_meta = inspector.get_model(model, refresh=refresh)
    
    if not model_meta:
        console.print(f"[red]Model '{model}' not found or not accessible[/]")
        raise typer.Exit(1)
    
    # Display model info
    console.print(f"  Label: {model_meta.label}")
    console.print(f"  Access: ", end="")
    access_parts = []
    if model_meta.can_read:
        access_parts.append("[green]read[/]")
    if model_meta.can_create:
        access_parts.append("[green]create[/]")
    if model_meta.can_write:
        access_parts.append("[green]write[/]")
    if model_meta.can_unlink:
        access_parts.append("[green]unlink[/]")
    console.print(", ".join(access_parts) if access_parts else "[red]none[/]")
    
    # Get fields
    if show_all:
        fields = list(model_meta.fields.values())
        console.print(f"\n[bold]All Fields ({len(fields)}):[/]")
    else:
        fields = model_meta.importable_fields
        console.print(f"\n[bold]Importable Fields ({len(fields)}):[/]")
    
    # Create table
    table = Table(show_header=True, header_style="bold")
    table.add_column("Field", style="cyan")
    table.add_column("Label")
    table.add_column("Type")
    table.add_column("Required")
    table.add_column("Status")
    table.add_column("Relation")
    
    for field in sorted(fields, key=lambda f: (not f.required, f.name)):
        # Status indicator
        if field.importable:
            status = "[green]✓ importable[/]"
        elif field.exportable:
            status = "[yellow]export only[/]"
        else:
            status = "[dim]ignored[/]"
        
        # Required indicator
        required = "[red]yes[/]" if field.required else ""
        
        # Relation
        relation = field.relation or ""
        if field.is_custom:
            relation = f"[magenta]{relation}[/]" if relation else "[magenta]custom[/]"
        
        table.add_row(
            field.name,
            field.label,
            field.field_type.value,
            required,
            status,
            relation,
        )
    
    console.print(table)
    
    # Summary
    required_fields = model_meta.required_fields
    relational_fields = model_meta.relational_fields
    
    console.print(f"\n[bold]Summary:[/]")
    console.print(f"  Total fields: {len(model_meta.fields)}")
    console.print(f"  Importable: {len(model_meta.importable_fields)}")
    console.print(f"  Required (for import): {len(required_fields)}")
    console.print(f"  Relational (many2one): {len(relational_fields)}")
    
    if required_fields:
        console.print(f"\n  [yellow]Required fields:[/] {', '.join(f.name for f in required_fields)}")


@app.command()
def version():
    """Show version information."""
    from migration_tool import __version__
    console.print(f"Odoo Migration Tool v{__version__}")


if __name__ == "__main__":
    app()
