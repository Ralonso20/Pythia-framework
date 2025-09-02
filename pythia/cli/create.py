"""
Create command for generating workers from templates
"""

import os
import click
from pathlib import Path
from cookiecutter.main import cookiecutter
from typing import Dict, Any


@click.group()
def create():
    """Generate new workers from templates"""
    pass


@create.command()
@click.option("--name", required=True, help="Worker name")
@click.option(
    "--type",
    "worker_type",
    type=click.Choice(
        [
            "kafka-consumer",
            "kafka-producer",
            "rabbitmq-consumer",
            "rabbitmq-producer",
            "redis-streams",
            "redis-pubsub",
            "batch-processor",
            "pipeline-worker",
            "multi-source-worker",
            "http-poller",
            "webhook-processor",
        ]
    ),
    required=True,
    help="Type of worker to create",
)
@click.option("--output-dir", default=".", help="Output directory")
@click.option("--no-input", is_flag=True, help="Don't prompt for parameters")
@click.option("--overwrite", is_flag=True, help="Overwrite if exists")
def worker(
    name: str, worker_type: str, output_dir: str, no_input: bool, overwrite: bool
):
    """
    Create a new worker from template

    Examples:
      pythia create worker --name my-worker --type kafka-consumer
      pythia create worker --name batch-job --type batch-processor
    """

    try:
        # Get template directory
        template_dir = get_template_directory(worker_type)

        if not template_dir.exists():
            click.echo(f"❌ Template not found: {worker_type}")
            click.echo("Available templates:")
            for t in get_available_templates():
                click.echo(f"  • {t}")
            return

        # Prepare extra context
        extra_context = {
            "worker_name": name,
            "worker_type": worker_type,
        }

        # Add type-specific defaults
        extra_context.update(get_type_defaults(worker_type))

        # Generate worker
        click.echo(f"🚀 Creating {worker_type} worker: {name}")

        output_path = cookiecutter(
            str(template_dir),
            output_dir=output_dir,
            no_input=no_input,
            overwrite_if_exists=overwrite,
            extra_context=extra_context,
        )

        click.echo("✅ Worker created successfully!")
        click.echo(f"📁 Location: {output_path}")
        click.echo()
        click.echo("Next steps:")
        click.echo(f"  cd {os.path.basename(output_path)}")
        click.echo("  pip install -r requirements.txt")
        click.echo("  pythia run worker.py")

    except Exception as e:
        click.echo(f"❌ Error creating worker: {e}")
        raise click.ClickException(str(e))


@create.command()
@click.argument("template_name")
@click.option("--output-dir", default=".", help="Output directory")
def template(template_name: str, output_dir: str):
    """Create a custom template"""

    template_path = Path(output_dir) / f"pythia-template-{template_name}"

    if template_path.exists():
        click.confirm(
            f"Template {template_name} already exists. Overwrite?", abort=True
        )

    create_custom_template(template_name, template_path)

    click.echo(f"✅ Template '{template_name}' created!")
    click.echo(f"📁 Location: {template_path}")
    click.echo()
    click.echo("Edit the template files and use it with:")
    click.echo(f"  pythia create worker --name my-worker --type custom:{template_name}")


def get_template_directory(worker_type: str) -> Path:
    """Get the template directory for a worker type"""
    pythia_dir = Path(__file__).parent.parent
    return pythia_dir / "templates" / worker_type


def get_available_templates() -> list[str]:
    """Get list of available templates"""
    pythia_dir = Path(__file__).parent.parent
    templates_dir = pythia_dir / "templates"

    if not templates_dir.exists():
        return []

    return [
        d.name
        for d in templates_dir.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    ]


def get_type_defaults(worker_type: str) -> Dict[str, Any]:
    """Get default configuration for worker type"""

    defaults = {
        "kafka-consumer": {
            "topics": "['my-topic']",
            "group_id": "my-group",
            "bootstrap_servers": "localhost:9092",
        },
        "kafka-producer": {"topic": "my-topic", "bootstrap_servers": "localhost:9092"},
        "rabbitmq-consumer": {
            "queue": "my-queue",
            "exchange": "",
            "routing_key": "my-key",
        },
        "rabbitmq-producer": {"exchange": "my-exchange", "routing_key": "my-key"},
        "redis-streams": {"stream": "my-stream", "consumer_group": "my-group"},
        "redis-pubsub": {"channel": "my-channel"},
        "batch-processor": {"batch_size": "10", "max_wait_time": "5.0"},
        "pipeline-worker": {"stages": "3"},
        "multi-source-worker": {
            "source_brokers": "kafka,rabbitmq",
            "sink_brokers": "kafka,webhook",
            "routing_strategy": "round_robin",
        },
        "http-poller": {"url": "https://api.example.com/data", "interval": "60"},
        "webhook-processor": {"webhook_url": "https://webhook.example.com"},
    }

    return defaults.get(worker_type, {})


def create_custom_template(name: str, output_path: Path):
    """Create a custom template structure"""

    # Create directory structure
    output_path.mkdir(parents=True, exist_ok=True)
    (output_path / "{{cookiecutter.worker_name}}").mkdir(exist_ok=True)

    # Create cookiecutter.json
    cookiecutter_json = {
        "worker_name": "my-worker",
        "description": "My custom worker",
        "author": "Developer",
        "python_version": "3.11",
    }

    with open(output_path / "cookiecutter.json", "w") as f:
        import json

        json.dump(cookiecutter_json, f, indent=2)

    # Create basic worker template
    worker_template = '''"""
{{cookiecutter.description}}
"""

from pythia import Worker, Message
from pythia.logging.setup import get_pythia_logger


class {{cookiecutter.worker_name|title}}Worker(Worker):
    """{{cookiecutter.description}}"""

    def __init__(self):
        super().__init__()
        self.logger = get_pythia_logger("{{cookiecutter.worker_name}}")

    async def process(self, message: Message):
        """Process incoming message"""
        self.logger.info("Processing message", message_id=message.message_id)

        # Your processing logic here
        result = message.body

        return result


if __name__ == "__main__":
    worker = {{cookiecutter.worker_name|title}}Worker()
    worker.run_sync()
'''

    with open(output_path / "{{cookiecutter.worker_name}}" / "worker.py", "w") as f:
        f.write(worker_template)

    # Create requirements.txt template
    requirements = """pythia>=0.1.0
"""

    with open(
        output_path / "{{cookiecutter.worker_name}}" / "requirements.txt", "w"
    ) as f:
        f.write(requirements)

    # Create README template
    readme = """# {{cookiecutter.worker_name}}

{{cookiecutter.description}}

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python worker.py
```

## Configuration

Set environment variables or edit worker.py configuration.
"""

    with open(output_path / "{{cookiecutter.worker_name}}" / "README.md", "w") as f:
        f.write(readme)
