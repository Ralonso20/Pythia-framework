"""
Run command for executing workers with hot reload
"""

import os
import sys
import time
import signal
import subprocess
from pathlib import Path
from typing import Optional

import click
from watchfiles import watch


@click.command()
@click.argument("worker_file", type=click.Path(exists=True))
@click.option("--reload", is_flag=True, help="Enable hot reload on file changes")
@click.option("--watch-dir", default=".", help="Directory to watch for changes")
@click.option("--ignore", multiple=True, help="Patterns to ignore")
@click.option("--config", type=click.Path(), help="Configuration file")
@click.option("--log-level", default="INFO", help="Log level")
@click.option("--env-file", type=click.Path(), help="Environment file to load")
def run(
    worker_file: str,
    reload: bool,
    watch_dir: str,
    ignore: tuple,
    config: Optional[str],
    log_level: str,
    env_file: Optional[str],
):
    """
    Run a Pythia worker

    Examples:
      pythia run worker.py
      pythia run worker.py --reload
      pythia run worker.py --config config.yaml --log-level DEBUG
    """

    # Load environment file if specified
    if env_file:
        load_env_file(env_file)

    # Set log level
    os.environ["PYTHIA_LOG_LEVEL"] = log_level.upper()

    # Add config to environment if specified
    if config:
        os.environ["PYTHIA_CONFIG_FILE"] = config

    worker_path = Path(worker_file).resolve()

    if not reload:
        # Simple run without reload
        click.echo(f"🚀 Running worker: {worker_path.name}")
        run_worker(worker_path)
    else:
        # Run with hot reload
        click.echo(f"🚀 Running worker with hot reload: {worker_path.name}")
        click.echo(f"👀 Watching directory: {Path(watch_dir).resolve()}")
        click.echo("Press Ctrl+C to stop")

        run_with_reload(worker_path, watch_dir, ignore)


def run_worker(worker_path: Path) -> None:
    """Run the worker directly"""
    try:
        # Change to worker directory
        original_cwd = os.getcwd()
        os.chdir(worker_path.parent)

        # Run worker
        result = subprocess.run([sys.executable, str(worker_path.name)], check=False)

        sys.exit(result.returncode)

    except KeyboardInterrupt:
        click.echo("\n👋 Stopped by user")
        sys.exit(0)
    except Exception as e:
        click.echo(f"❌ Error running worker: {e}")
        sys.exit(1)
    finally:
        os.chdir(original_cwd)


def run_with_reload(worker_path: Path, watch_dir: str, ignore_patterns: tuple) -> None:
    """Run worker with hot reload capability"""

    process: Optional[subprocess.Popen] = None

    def start_worker():
        nonlocal process

        if process:
            stop_worker()

        click.echo(f"🔄 Starting worker: {worker_path.name}")

        try:
            # Change to worker directory
            original_cwd = os.getcwd()
            os.chdir(worker_path.parent)

            process = subprocess.Popen([sys.executable, str(worker_path.name)])

            os.chdir(original_cwd)

        except Exception as e:
            click.echo(f"❌ Error starting worker: {e}")

    def stop_worker():
        nonlocal process

        if process and process.poll() is None:
            click.echo("🛑 Stopping worker...")

            try:
                # Try graceful shutdown first
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # Force kill if needed
                process.kill()
                process.wait()

            process = None

    def handle_signal(signum, frame):
        click.echo(f"\n📡 Received signal {signum}")
        stop_worker()
        sys.exit(0)

    # Setup signal handlers
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Start initial worker
    start_worker()

    try:
        # Watch for file changes
        watch_path = Path(watch_dir).resolve()

        # Default ignore patterns
        default_ignore = {
            "*.pyc",
            "__pycache__",
            ".git",
            ".pytest_cache",
            "*.log",
            ".DS_Store",
            "*.tmp",
        }
        all_ignore = set(ignore_patterns) | default_ignore

        for changes in watch(watch_path, ignore_paths=all_ignore):
            if not changes:
                continue

            # Check if any Python files changed
            python_changes = [
                change
                for change in changes
                if change[1].endswith(".py")
                or change[1].endswith(".yaml")
                or change[1].endswith(".json")
            ]

            if python_changes:
                click.echo(
                    f"📝 Files changed: {[Path(c[1]).name for c in python_changes]}"
                )

                # Restart worker
                start_worker()

                # Brief pause to avoid rapid restarts
                time.sleep(0.5)

    except KeyboardInterrupt:
        click.echo("\n👋 Stopped by user")
        stop_worker()
        sys.exit(0)
    except Exception as e:
        click.echo(f"❌ Error in reload loop: {e}")
        stop_worker()
        sys.exit(1)


def load_env_file(env_file: str) -> None:
    """Load environment variables from file"""
    try:
        env_path = Path(env_file)
        if not env_path.exists():
            click.echo(f"⚠️ Environment file not found: {env_file}")
            return

        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ[key.strip()] = value.strip()

        click.echo(f"✅ Loaded environment from: {env_file}")

    except Exception as e:
        click.echo(f"❌ Error loading environment file: {e}")


@click.command()
@click.argument("config_file", type=click.Path(exists=True))
def validate(config_file: str):
    """Validate a worker configuration file"""

    try:
        config_path = Path(config_file)

        if config_path.suffix.lower() == ".yaml":
            import yaml

            with open(config_path) as f:
                config = yaml.safe_load(f)
        elif config_path.suffix.lower() == ".json":
            import json

            with open(config_path) as f:
                config = json.load(f)
        else:
            click.echo("❌ Unsupported config format. Use .yaml or .json")
            return

        # Basic validation
        required_fields = ["broker_type"]
        missing_fields = [field for field in required_fields if field not in config]

        if missing_fields:
            click.echo(f"❌ Missing required fields: {missing_fields}")
            return

        click.echo("✅ Configuration is valid")
        click.echo(f"📋 Broker type: {config['broker_type']}")

        if "worker_name" in config:
            click.echo(f"🏷️ Worker name: {config['worker_name']}")

    except Exception as e:
        click.echo(f"❌ Error validating config: {e}")


# Note: validate is available as a separate command in the main CLI
