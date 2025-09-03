"""
Comprehensive tests for CLI create functionality
"""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock, call
from pathlib import Path
from click.testing import CliRunner

from pythia.cli.create import (
    create,
    worker,
    template,
    get_template_directory,
    get_available_templates,
    get_type_defaults,
    create_custom_template
)


class TestCliCreate:
    """Test CLI create command functionality"""

    @pytest.fixture
    def runner(self):
        """Create CLI test runner"""
        return CliRunner()

    @pytest.fixture
    def mock_template_dir(self, tmp_path):
        """Create mock template directory structure"""
        templates_dir = tmp_path / "templates"
        templates_dir.mkdir(parents=True)

        # Create sample template directories
        (templates_dir / "kafka-consumer").mkdir()
        (templates_dir / "kafka-producer").mkdir()
        (templates_dir / "rabbitmq-consumer").mkdir()

        return templates_dir

    def test_create_group_help(self, runner):
        """Test create command group help"""
        result = runner.invoke(create, ["--help"])

        assert result.exit_code == 0
        assert "Generate new workers from templates" in result.output

    @patch('pythia.cli.create.cookiecutter')
    @patch('pythia.cli.create.get_template_directory')
    def test_worker_creation_success(self, mock_get_template_dir, mock_cookiecutter, runner):
        """Test successful worker creation"""
        # Setup mocks
        mock_template_path = Mock(spec=Path)
        mock_template_path.exists.return_value = True
        mock_get_template_dir.return_value = mock_template_path
        mock_cookiecutter.return_value = "/output/my-worker"

        result = runner.invoke(worker, [
            "--name", "my-worker",
            "--type", "kafka-consumer",
            "--output-dir", "/tmp",
            "--no-input"
        ])

        assert result.exit_code == 0
        assert "Creating kafka-consumer worker: my-worker" in result.output
        assert "Worker created successfully!" in result.output
        assert "Location: /output/my-worker" in result.output
        assert "pip install -r requirements.txt" in result.output

        mock_cookiecutter.assert_called_once()
        call_args = mock_cookiecutter.call_args
        assert call_args[0][0] == str(mock_template_path)
        assert call_args[1]["output_dir"] == "/tmp"
        assert call_args[1]["no_input"] is True
        assert "worker_name" in call_args[1]["extra_context"]
        assert call_args[1]["extra_context"]["worker_name"] == "my-worker"

    @patch('pythia.cli.create.get_template_directory')
    @patch('pythia.cli.create.get_available_templates')
    def test_worker_creation_template_not_found(self, mock_get_templates, mock_get_template_dir, runner):
        """Test worker creation with non-existent template"""
        # Setup mocks
        mock_template_path = Mock(spec=Path)
        mock_template_path.exists.return_value = False
        mock_get_template_dir.return_value = mock_template_path
        mock_get_templates.return_value = ["kafka-consumer", "rabbitmq-consumer"]

        result = runner.invoke(worker, [
            "--name", "my-worker",
            "--type", "kafka-consumer"
        ])

        assert result.exit_code == 0
        assert "Template not found: kafka-consumer" in result.output
        assert "Available templates:" in result.output
        assert "kafka-consumer" in result.output
        assert "rabbitmq-consumer" in result.output

    @patch('pythia.cli.create.cookiecutter')
    @patch('pythia.cli.create.get_template_directory')
    def test_worker_creation_with_overwrite(self, mock_get_template_dir, mock_cookiecutter, runner):
        """Test worker creation with overwrite flag"""
        mock_template_path = Mock(spec=Path)
        mock_template_path.exists.return_value = True
        mock_get_template_dir.return_value = mock_template_path
        mock_cookiecutter.return_value = "/output/my-worker"

        result = runner.invoke(worker, [
            "--name", "my-worker",
            "--type", "kafka-consumer",
            "--overwrite"
        ])

        assert result.exit_code == 0
        mock_cookiecutter.assert_called_once()
        assert mock_cookiecutter.call_args[1]["overwrite_if_exists"] is True

    @patch('pythia.cli.create.cookiecutter')
    @patch('pythia.cli.create.get_template_directory')
    def test_worker_creation_with_type_defaults(self, mock_get_template_dir, mock_cookiecutter, runner):
        """Test worker creation includes type-specific defaults"""
        mock_template_path = Mock(spec=Path)
        mock_template_path.exists.return_value = True
        mock_get_template_dir.return_value = mock_template_path
        mock_cookiecutter.return_value = "/output/my-worker"

        result = runner.invoke(worker, [
            "--name", "my-worker",
            "--type", "kafka-consumer"
        ])

        assert result.exit_code == 0
        call_args = mock_cookiecutter.call_args
        extra_context = call_args[1]["extra_context"]

        # Should include type defaults
        assert "topics" in extra_context
        assert "group_id" in extra_context
        assert "bootstrap_servers" in extra_context
        assert extra_context["topics"] == "['my-topic']"

    @patch('pythia.cli.create.cookiecutter')
    @patch('pythia.cli.create.get_template_directory')
    def test_worker_creation_error_handling(self, mock_get_template_dir, mock_cookiecutter, runner):
        """Test worker creation error handling"""
        mock_template_path = Mock(spec=Path)
        mock_template_path.exists.return_value = True
        mock_get_template_dir.return_value = mock_template_path
        mock_cookiecutter.side_effect = Exception("Cookiecutter failed")

        result = runner.invoke(worker, [
            "--name", "my-worker",
            "--type", "kafka-consumer"
        ])

        assert result.exit_code != 0
        assert "Error creating worker: Cookiecutter failed" in result.output

    @patch('pythia.cli.create.create_custom_template')
    def test_template_creation_success(self, mock_create_template, runner, tmp_path):
        """Test successful custom template creation"""
        with runner.isolated_filesystem():
            result = runner.invoke(template, ["my-template"])

            assert result.exit_code == 0
            assert "Template 'my-template' created!" in result.output
            assert "pythia create worker --name my-worker --type custom:my-template" in result.output

            mock_create_template.assert_called_once()
            call_args = mock_create_template.call_args[0]
            assert call_args[0] == "my-template"
            assert "pythia-template-my-template" in str(call_args[1])

    @patch('pythia.cli.create.create_custom_template')
    def test_template_creation_with_custom_output_dir(self, mock_create_template, runner):
        """Test custom template creation with custom output directory"""
        with runner.isolated_filesystem():
            result = runner.invoke(template, ["my-template", "--output-dir", "/custom/dir"])

            assert result.exit_code == 0
            mock_create_template.assert_called_once()
            call_args = mock_create_template.call_args[0]
            assert call_args[1] == Path("/custom/dir/pythia-template-my-template")

    def test_template_creation_existing_template_abort(self, runner, tmp_path):
        """Test template creation with existing template (user aborts)"""
        # Create existing template directory
        template_dir = tmp_path / "pythia-template-existing"
        template_dir.mkdir(parents=True)

        with runner.isolated_filesystem():
            # Create the directory in the isolated filesystem too
            Path("pythia-template-existing").mkdir()

            result = runner.invoke(template, ["existing"], input="n\n")

            assert result.exit_code == 1  # Aborted
            assert "already exists" in result.output

    def test_get_template_directory(self):
        """Test get_template_directory function"""
        result = get_template_directory("kafka-consumer")

        assert isinstance(result, Path)
        assert result.name == "kafka-consumer"
        assert "templates" in result.parts

    @patch('pathlib.Path.exists')
    def test_get_available_templates_no_directory(self, mock_exists):
        """Test get_available_templates when templates directory doesn't exist"""
        mock_exists.return_value = False

        result = get_available_templates()

        assert result == []

    def test_get_available_templates_with_mock_directory(self, mock_template_dir):
        """Test get_available_templates with mock directory"""
        with patch('pythia.cli.create.Path') as mock_path_class:
            # Mock the __file__ path to return our mock structure
            mock_file_path = Mock()
            mock_pythia_dir = Mock()
            mock_templates_dir = Mock()
            mock_templates_dir.exists.return_value = True

            # Create mock template directories
            mock_dirs = []
            for name in ["kafka-consumer", "kafka-producer", ".hidden"]:
                mock_dir = Mock()
                mock_dir.name = name
                mock_dir.is_dir.return_value = True
                mock_dirs.append(mock_dir)

            mock_templates_dir.iterdir.return_value = mock_dirs

            # Set up the path hierarchy
            mock_file_path.parent.parent = mock_pythia_dir
            mock_pythia_dir.__truediv__ = Mock(return_value=mock_templates_dir)
            mock_path_class.return_value = mock_file_path

            result = get_available_templates()

            assert "kafka-consumer" in result
            assert "kafka-producer" in result
            assert ".hidden" not in result  # Hidden directories should be excluded

    def test_get_type_defaults_kafka_consumer(self):
        """Test get_type_defaults for kafka-consumer"""
        result = get_type_defaults("kafka-consumer")

        expected = {
            "topics": "['my-topic']",
            "group_id": "my-group",
            "bootstrap_servers": "localhost:9092",
        }
        assert result == expected

    def test_get_type_defaults_rabbitmq_producer(self):
        """Test get_type_defaults for rabbitmq-producer"""
        result = get_type_defaults("rabbitmq-producer")

        expected = {
            "exchange": "my-exchange",
            "routing_key": "my-key"
        }
        assert result == expected

    def test_get_type_defaults_batch_processor(self):
        """Test get_type_defaults for batch-processor"""
        result = get_type_defaults("batch-processor")

        expected = {
            "batch_size": "10",
            "max_wait_time": "5.0"
        }
        assert result == expected

    def test_get_type_defaults_unknown_type(self):
        """Test get_type_defaults for unknown worker type"""
        result = get_type_defaults("unknown-type")

        assert result == {}

    def test_get_type_defaults_all_types(self):
        """Test get_type_defaults returns correct defaults for all supported types"""
        supported_types = [
            "kafka-consumer", "kafka-producer",
            "rabbitmq-consumer", "rabbitmq-producer",
            "redis-streams", "redis-pubsub",
            "batch-processor", "pipeline-worker",
            "multi-source-worker", "http-poller",
            "webhook-processor"
        ]

        for worker_type in supported_types:
            result = get_type_defaults(worker_type)
            assert isinstance(result, dict)
            # Each type should have at least one default
            if worker_type != "pipeline-worker":  # pipeline-worker has minimal defaults
                assert len(result) > 0

    def test_create_custom_template_structure(self, tmp_path):
        """Test create_custom_template creates proper directory structure"""
        template_name = "test-template"
        output_path = tmp_path / f"pythia-template-{template_name}"

        create_custom_template(template_name, output_path)

        # Check directory structure
        assert output_path.exists()
        assert (output_path / "{{cookiecutter.worker_name}}").exists()
        assert (output_path / "cookiecutter.json").exists()
        assert (output_path / "{{cookiecutter.worker_name}}" / "worker.py").exists()
        assert (output_path / "{{cookiecutter.worker_name}}" / "requirements.txt").exists()
        assert (output_path / "{{cookiecutter.worker_name}}" / "README.md").exists()

    def test_create_custom_template_cookiecutter_json(self, tmp_path):
        """Test create_custom_template creates valid cookiecutter.json"""
        template_name = "test-template"
        output_path = tmp_path / f"pythia-template-{template_name}"

        create_custom_template(template_name, output_path)

        # Check cookiecutter.json content
        with open(output_path / "cookiecutter.json") as f:
            config = json.load(f)

        assert config["worker_name"] == "my-worker"
        assert config["description"] == "My custom worker"
        assert config["author"] == "Developer"
        assert config["python_version"] == "3.11"

    def test_create_custom_template_worker_file(self, tmp_path):
        """Test create_custom_template creates proper worker.py template"""
        template_name = "test-template"
        output_path = tmp_path / f"pythia-template-{template_name}"

        create_custom_template(template_name, output_path)

        # Check worker.py content
        with open(output_path / "{{cookiecutter.worker_name}}" / "worker.py") as f:
            content = f.read()

        assert "from pythia import Worker, Message" in content
        assert "class {{cookiecutter.worker_name|title}}Worker(Worker)" in content
        assert "async def process(self, message: Message)" in content
        assert "get_pythia_logger" in content

    def test_create_custom_template_requirements_file(self, tmp_path):
        """Test create_custom_template creates requirements.txt"""
        template_name = "test-template"
        output_path = tmp_path / f"pythia-template-{template_name}"

        create_custom_template(template_name, output_path)

        # Check requirements.txt content
        with open(output_path / "{{cookiecutter.worker_name}}" / "requirements.txt") as f:
            content = f.read()

        assert "pythia>=0.1.0" in content

    def test_create_custom_template_readme_file(self, tmp_path):
        """Test create_custom_template creates README.md template"""
        template_name = "test-template"
        output_path = tmp_path / f"pythia-template-{template_name}"

        create_custom_template(template_name, output_path)

        # Check README.md content
        with open(output_path / "{{cookiecutter.worker_name}}" / "README.md") as f:
            content = f.read()

        assert "# {{cookiecutter.worker_name}}" in content
        assert "{{cookiecutter.description}}" in content
        assert "## Installation" in content
        assert "## Usage" in content
        assert "## Configuration" in content

    def test_worker_command_required_options(self, runner):
        """Test worker command requires name and type options"""
        # Test missing name
        result = runner.invoke(worker, ["--type", "kafka-consumer"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "Usage:" in result.output

        # Test missing type
        result = runner.invoke(worker, ["--name", "my-worker"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "Usage:" in result.output

    def test_worker_command_invalid_type(self, runner):
        """Test worker command with invalid worker type"""
        result = runner.invoke(worker, [
            "--name", "my-worker",
            "--type", "invalid-type"
        ])
        assert result.exit_code != 0
        assert "Invalid value for" in result.output or "Usage:" in result.output

    @pytest.mark.parametrize("worker_type", [
        "kafka-consumer", "kafka-producer",
        "rabbitmq-consumer", "rabbitmq-producer",
        "redis-streams", "redis-pubsub",
        "batch-processor", "pipeline-worker",
        "multi-source-worker", "http-poller",
        "webhook-processor"
    ])
    @patch('pythia.cli.create.cookiecutter')
    @patch('pythia.cli.create.get_template_directory')
    def test_worker_creation_all_types(self, mock_get_template_dir, mock_cookiecutter, runner, worker_type):
        """Test worker creation works for all supported worker types"""
        mock_template_path = Mock(spec=Path)
        mock_template_path.exists.return_value = True
        mock_get_template_dir.return_value = mock_template_path
        mock_cookiecutter.return_value = f"/output/my-{worker_type}-worker"

        result = runner.invoke(worker, [
            "--name", f"my-{worker_type}-worker",
            "--type", worker_type
        ])

        assert result.exit_code == 0
        assert f"Creating {worker_type} worker" in result.output
        mock_cookiecutter.assert_called_once()
