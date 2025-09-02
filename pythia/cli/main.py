"""
Main CLI entry point for Pythia
"""

import click
from .create import create
from .run import run, validate
from .monitor import monitor


@click.group()
@click.version_option(version="0.1.0", prog_name="pythia")
@click.pass_context
def cli(ctx):
    """
    🐍 Pythia - Python Worker Framework CLI

    A modern framework for creating efficient and scalable workers in Python.
    """
    ctx.ensure_object(dict)


# Register commands
cli.add_command(create)
cli.add_command(run)
cli.add_command(monitor)
cli.add_command(validate)


@cli.command()
def info():
    """Show Pythia framework information"""
    click.echo("🐍 Pythia - Python Worker Framework")
    click.echo("Version: 0.1.0")
    click.echo()
    click.echo("Available commands:")
    click.echo("  create   - Generate new workers from templates")
    click.echo("  run      - Run workers with hot reload")
    click.echo("  monitor  - Monitor running workers")
    click.echo("  validate - Validate worker configuration")
    click.echo()
    click.echo("For help with a specific command, run:")
    click.echo("  pythia <command> --help")


def main():
    """Entry point for the CLI"""
    cli()


if __name__ == "__main__":
    main()
