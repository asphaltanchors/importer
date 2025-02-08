"""
Main entry point for running the CLI as a module.
This allows the CLI to be run with `python -m importer.cli`
"""

from .main import cli

if __name__ == '__main__':
    cli()
