"""
Entry point for running the migration tool as a module.

Usage: python -m migration_tool <command> [options]
"""

from migration_tool.cli import app

if __name__ == "__main__":
    app()
