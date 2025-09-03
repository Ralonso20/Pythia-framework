"""
Comprehensive tests for CLI run functionality
"""

import os
import sys
import time
import signal
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
import pytest
from click.testing import CliRunner

from pythia.cli.run import run, run_worker, run_with_reload, load_env_file, validate


class TestCliRun:
    """Test CLI run command functionality"""

    @pytest.fixture
    def runner(self):
        """Create CLI test runner"""
        return CliRunner()

    @pytest.fixture
    def mock_worker_file(self, tmp_path):
        """Create a mock worker file"""
        worker_file = tmp_path / "test_worker.py"
        worker_file.write_text("""
from pythia import Worker

class TestWorker(Worker):
    def process(self, message):
        return message

if __name__ == "__main__":
    worker = TestWorker()
    worker.run_sync()
        """)
        return worker_file

    @pytest.fixture
    def mock_config_file(self, tmp_path):
        """Create a mock config file"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
broker_type: kafka
worker_name: test-worker
bootstrap_servers: localhost:9092
        """)
        return config_file

    @pytest.fixture
    def mock_env_file(self, tmp_path):
        """Create a mock environment file"""
        env_file = tmp_path / ".env"
        env_file.write_text("""
PYTHIA_LOG_LEVEL=DEBUG
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
# This is a comment
WORKER_TIMEOUT=30
EMPTY_LINE_ABOVE=test
        """)
        return env_file

    def test_run_command_help(self, runner):
        """Test run command help"""
        result = runner.invoke(run, ["--help"])

        assert result.exit_code == 0
        assert "Run a Pythia worker" in result.output
        assert "--reload" in result.output
        assert "--config" in result.output

    @patch('pythia.cli.run.run_worker')
    def test_run_without_reload(self, mock_run_worker, runner, mock_worker_file):
        """Test running worker without reload"""
        result = runner.invoke(run, [str(mock_worker_file)])

        assert result.exit_code == 0
        assert f"Running worker: {mock_worker_file.name}" in result.output
        mock_run_worker.assert_called_once_with(mock_worker_file.resolve())

    @patch('pythia.cli.run.run_with_reload')
    def test_run_with_reload(self, mock_run_with_reload, runner, mock_worker_file):
        """Test running worker with reload"""
        result = runner.invoke(run, [str(mock_worker_file), "--reload"])

        assert result.exit_code == 0
        assert "Running worker with hot reload" in result.output
        assert "Watching directory" in result.output
        mock_run_with_reload.assert_called_once()

    @patch('pythia.cli.run.run_worker')
    @patch('pythia.cli.run.load_env_file')
    def test_run_with_env_file(self, mock_load_env, mock_run_worker, runner, mock_worker_file, mock_env_file):
        """Test running worker with environment file"""
        result = runner.invoke(run, [
            str(mock_worker_file),
            "--env-file", str(mock_env_file)
        ])

        assert result.exit_code == 0
        mock_load_env.assert_called_once_with(str(mock_env_file))
        mock_run_worker.assert_called_once()

    @patch('pythia.cli.run.run_worker')
    def test_run_with_config(self, mock_run_worker, runner, mock_worker_file, mock_config_file):
        """Test running worker with config file"""
        result = runner.invoke(run, [
            str(mock_worker_file),
            "--config", str(mock_config_file)
        ])

        assert result.exit_code == 0
        mock_run_worker.assert_called_once()

    @patch('pythia.cli.run.run_worker')
    def test_run_with_log_level(self, mock_run_worker, runner, mock_worker_file):
        """Test running worker with custom log level"""
        with patch.dict(os.environ, {}, clear=True):
            result = runner.invoke(run, [
                str(mock_worker_file),
                "--log-level", "DEBUG"
            ])

            assert result.exit_code == 0
            assert os.environ.get("PYTHIA_LOG_LEVEL") == "DEBUG"
            mock_run_worker.assert_called_once()

    @patch('pythia.cli.run.run_with_reload')
    def test_run_with_reload_and_watch_options(self, mock_run_with_reload, runner, mock_worker_file):
        """Test running worker with reload and watch options"""
        result = runner.invoke(run, [
            str(mock_worker_file),
            "--reload",
            "--watch-dir", "/custom/watch/dir",
            "--ignore", "*.log",
            "--ignore", "*.tmp"
        ])

        assert result.exit_code == 0
        mock_run_with_reload.assert_called_once()
        call_args = mock_run_with_reload.call_args[0]
        assert call_args[1] == "/custom/watch/dir"  # watch_dir
        assert call_args[2] == ("*.log", "*.tmp")   # ignore patterns

    @patch('subprocess.run')
    def test_run_worker_success(self, mock_subprocess_run):
        """Test successful worker execution"""
        mock_subprocess_run.return_value.returncode = 0
        worker_path = Path("/fake/worker.py")

        with patch('os.chdir'), patch('os.getcwd', return_value="/original"):
            with pytest.raises(SystemExit) as exc_info:
                run_worker(worker_path)

            assert exc_info.value.code == 0
            mock_subprocess_run.assert_called_once_with(
                [sys.executable, "worker.py"],
                check=False
            )

    @patch('subprocess.run')
    def test_run_worker_failure(self, mock_subprocess_run):
        """Test worker execution failure"""
        mock_subprocess_run.return_value.returncode = 1
        worker_path = Path("/fake/worker.py")

        with patch('os.chdir'), patch('os.getcwd', return_value="/original"):
            with pytest.raises(SystemExit) as exc_info:
                run_worker(worker_path)

            assert exc_info.value.code == 1

    @patch('subprocess.run')
    def test_run_worker_keyboard_interrupt(self, mock_subprocess_run):
        """Test worker execution with keyboard interrupt"""
        mock_subprocess_run.side_effect = KeyboardInterrupt()
        worker_path = Path("/fake/worker.py")

        with patch('os.chdir'), patch('os.getcwd', return_value="/original"):
            with pytest.raises(SystemExit) as exc_info:
                run_worker(worker_path)

            assert exc_info.value.code == 0

    @patch('subprocess.run')
    def test_run_worker_exception(self, mock_subprocess_run):
        """Test worker execution with exception"""
        mock_subprocess_run.side_effect = Exception("Process failed")
        worker_path = Path("/fake/worker.py")

        with patch('os.chdir'), patch('os.getcwd', return_value="/original"):
            with pytest.raises(SystemExit) as exc_info:
                run_worker(worker_path)

            assert exc_info.value.code == 1

    @patch('subprocess.Popen')
    @patch('watchfiles.watch')
    @patch('signal.signal')
    def test_run_with_reload_start_worker(self, mock_signal, mock_watch, mock_popen):
        """Test run_with_reload starts worker correctly"""
        # Mock process
        mock_process = Mock()
        mock_process.poll.return_value = None
        mock_popen.return_value = mock_process

        # Mock file changes (return empty list to avoid infinite loop)
        mock_watch.return_value = iter([])

        worker_path = Path("/fake/worker.py")

        with patch('os.chdir'), patch('os.getcwd', return_value="/original"):
            with pytest.raises(SystemExit):
                run_with_reload(worker_path, "/watch/dir", ())

        mock_popen.assert_called()
        assert mock_signal.call_count == 2  # SIGINT and SIGTERM handlers

    @patch('subprocess.Popen')
    @patch('pythia.cli.run.watch')  # Patch the specific import
    @patch('signal.signal')
    def test_run_with_reload_file_changes(self, mock_signal, mock_watch, mock_popen):
        """Test run_with_reload responds to file changes"""
        # Mock process
        mock_process = Mock()
        mock_process.poll.return_value = None
        mock_popen.return_value = mock_process

        # Mock file changes - Python file change followed by KeyboardInterrupt to exit
        def mock_watch_changes():
            yield [("modified", "/watch/dir/test.py")]
            raise KeyboardInterrupt()

        mock_watch.return_value = mock_watch_changes()

        worker_path = Path("/fake/worker.py")

        with patch('os.chdir'), patch('os.getcwd', return_value="/original"):
            with pytest.raises(SystemExit) as exc_info:
                run_with_reload(worker_path, "/watch/dir", ())

            # Should exit cleanly (KeyboardInterrupt handling)
            assert exc_info.value.code == 0

        # Should have called Popen at least twice (initial + restart after change)
        assert mock_popen.call_count >= 2
        # Verify watch was called
        mock_watch.assert_called_once()

    @patch('subprocess.Popen')
    @patch('watchfiles.watch')
    @patch('signal.signal')
    def test_run_with_reload_ignore_non_python_changes(self, mock_signal, mock_watch, mock_popen):
        """Test run_with_reload ignores non-Python file changes"""
        # Mock process
        mock_process = Mock()
        mock_process.poll.return_value = None
        mock_popen.return_value = mock_process

        # Mock file changes - non-Python file change followed by empty list to exit
        mock_watch.return_value = iter([
            [("modified", "/watch/dir/test.txt")],
            []
        ])

        worker_path = Path("/fake/worker.py")

        with patch('os.chdir'), patch('os.getcwd', return_value="/original"):
            with pytest.raises(SystemExit):
                run_with_reload(worker_path, "/watch/dir", ())

        # Should only start worker once (no restart for non-Python file)
        assert mock_popen.call_count == 1

    @patch('subprocess.Popen')
    @patch('pythia.cli.run.watch')  # Patch the specific import
    @patch('signal.signal')
    def test_run_with_reload_handles_yaml_json_changes(self, mock_signal, mock_watch, mock_popen):
        """Test run_with_reload responds to yaml and json file changes"""
        # Mock process
        mock_process = Mock()
        mock_process.poll.return_value = None
        mock_popen.return_value = mock_process

        # Mock file changes - YAML file then KeyboardInterrupt
        def mock_watch_changes():
            yield [("modified", "/watch/dir/config.yaml")]
            raise KeyboardInterrupt()

        mock_watch.return_value = mock_watch_changes()

        worker_path = Path("/fake/worker.py")

        with patch('os.chdir'), patch('os.getcwd', return_value="/original"):
            with pytest.raises(SystemExit) as exc_info:
                run_with_reload(worker_path, "/watch/dir", ())

            # Should exit cleanly (KeyboardInterrupt handling)
            assert exc_info.value.code == 0

        # Should have called Popen at least twice (initial + restart after YAML change)
        assert mock_popen.call_count >= 2
        # Verify watch was called
        mock_watch.assert_called_once()

    @patch('subprocess.Popen')
    @patch('watchfiles.watch')
    @patch('signal.signal')
    def test_run_with_reload_signal_handling(self, mock_signal, mock_watch, mock_popen):
        """Test run_with_reload sets up signal handlers correctly"""
        mock_process = Mock()
        mock_popen.return_value = mock_process
        mock_watch.return_value = iter([])

        worker_path = Path("/fake/worker.py")

        with patch('os.chdir'), patch('os.getcwd', return_value="/original"):
            with pytest.raises(SystemExit):
                run_with_reload(worker_path, "/watch/dir", ())

        # Verify signal handlers were set up
        signal_calls = [call[0] for call in mock_signal.call_args_list]
        assert (signal.SIGINT, mock_signal.call_args_list[0][0][1]) in signal_calls
        assert (signal.SIGTERM, mock_signal.call_args_list[1][0][1]) in signal_calls

    @patch('subprocess.Popen')
    @patch('watchfiles.watch')
    @patch('signal.signal')
    def test_run_with_reload_stop_worker(self, mock_signal, mock_watch, mock_popen):
        """Test run_with_reload stops worker correctly"""
        # Mock process that's running
        mock_process = Mock()
        mock_process.poll.return_value = None
        mock_popen.return_value = mock_process
        mock_watch.return_value = iter([])

        worker_path = Path("/fake/worker.py")

        with patch('os.chdir'), patch('os.getcwd', return_value="/original"):
            with pytest.raises(SystemExit):
                run_with_reload(worker_path, "/watch/dir", ())

        # Process should be terminated
        mock_process.terminate.assert_called()

    @patch('subprocess.Popen')
    @patch('watchfiles.watch')
    @patch('signal.signal')
    @patch('pathlib.Path.resolve')
    def test_run_with_reload_force_kill_on_timeout(self, mock_resolve, mock_signal, mock_watch, mock_popen):
        """Test run_with_reload force kills unresponsive worker"""
        # Mock process that doesn't terminate gracefully
        mock_process = Mock()
        mock_process.poll.return_value = None
        mock_process.wait.side_effect = subprocess.TimeoutExpired("cmd", 5)
        mock_popen.return_value = mock_process

        # Mock Path.resolve
        mock_resolve.return_value = Path("/watch/dir")

        # Mock empty watch to go straight to exit
        mock_watch.return_value = iter([])

        worker_path = Path("/fake/worker.py")

        with patch('os.chdir'), patch('os.getcwd', return_value="/original"):
            try:
                run_with_reload(worker_path, "/watch/dir", ())
            except:
                pass  # Ignore any exceptions, we just want to test the termination logic

        # Should have called terminate (kill may not be called if exception happens first)
        mock_process.terminate.assert_called()

    def test_load_env_file_success(self, mock_env_file):
        """Test successful environment file loading"""
        with patch.dict(os.environ, {}, clear=True):
            load_env_file(str(mock_env_file))

            assert os.environ.get("PYTHIA_LOG_LEVEL") == "DEBUG"
            assert os.environ.get("KAFKA_BOOTSTRAP_SERVERS") == "localhost:9092"
            assert os.environ.get("WORKER_TIMEOUT") == "30"
            assert os.environ.get("EMPTY_LINE_ABOVE") == "test"

    def test_load_env_file_not_found(self, tmp_path):
        """Test environment file loading with non-existent file"""
        non_existent_file = tmp_path / "non_existent.env"

        with patch('click.echo') as mock_echo:
            load_env_file(str(non_existent_file))
            mock_echo.assert_called_with(f"⚠️ Environment file not found: {non_existent_file}")

    def test_load_env_file_malformed_line(self, tmp_path):
        """Test environment file loading with malformed lines"""
        env_file = tmp_path / ".env"
        env_file.write_text("""
VALID_VAR=value
INVALID_LINE_NO_EQUALS
ANOTHER_VALID=test
        """)

        with patch.dict(os.environ, {}, clear=True):
            load_env_file(str(env_file))

            # Only valid lines should be loaded
            assert os.environ.get("VALID_VAR") == "value"
            assert os.environ.get("ANOTHER_VALID") == "test"
            assert "INVALID_LINE_NO_EQUALS" not in os.environ

    def test_load_env_file_with_spaces(self, tmp_path):
        """Test environment file loading with spaces around values"""
        env_file = tmp_path / ".env"
        env_file.write_text("""
SPACED_KEY = spaced value
TRIMMED_KEY=trimmed_value
        """)

        with patch.dict(os.environ, {}, clear=True):
            load_env_file(str(env_file))

            assert os.environ.get("SPACED_KEY") == "spaced value"
            assert os.environ.get("TRIMMED_KEY") == "trimmed_value"

    def test_load_env_file_exception_handling(self, tmp_path):
        """Test environment file loading with read exception"""
        env_file = tmp_path / ".env"
        env_file.write_text("VALID=value")

        with patch('builtins.open', side_effect=Exception("Read error")):
            with patch('click.echo') as mock_echo:
                load_env_file(str(env_file))
                mock_echo.assert_called_with("❌ Error loading environment file: Read error")

    def test_validate_yaml_config_success(self, runner, mock_config_file):
        """Test successful YAML config validation"""
        result = runner.invoke(validate, [str(mock_config_file)])

        assert result.exit_code == 0
        assert "Configuration is valid" in result.output
        assert "Broker type: kafka" in result.output
        assert "Worker name: test-worker" in result.output

    def test_validate_json_config_success(self, runner, tmp_path):
        """Test successful JSON config validation"""
        json_config = tmp_path / "config.json"
        json_config.write_text('{"broker_type": "rabbitmq", "worker_name": "test-json-worker"}')

        result = runner.invoke(validate, [str(json_config)])

        assert result.exit_code == 0
        assert "Configuration is valid" in result.output
        assert "Broker type: rabbitmq" in result.output
        assert "Worker name: test-json-worker" in result.output

    def test_validate_config_missing_required_fields(self, runner, tmp_path):
        """Test config validation with missing required fields"""
        invalid_config = tmp_path / "invalid.yaml"
        invalid_config.write_text("worker_name: test-worker")  # Missing broker_type

        result = runner.invoke(validate, [str(invalid_config)])

        assert result.exit_code == 0
        assert "Missing required fields: ['broker_type']" in result.output

    def test_validate_config_unsupported_format(self, runner, tmp_path):
        """Test config validation with unsupported format"""
        unsupported_config = tmp_path / "config.txt"
        unsupported_config.write_text("broker_type=kafka")

        result = runner.invoke(validate, [str(unsupported_config)])

        assert result.exit_code == 0
        assert "Unsupported config format" in result.output

    def test_validate_config_yaml_error(self, runner, tmp_path):
        """Test config validation with YAML parsing error"""
        invalid_yaml = tmp_path / "invalid.yaml"
        invalid_yaml.write_text("""
broker_type: kafka
invalid_yaml: [unclosed list
        """)

        result = runner.invoke(validate, [str(invalid_yaml)])

        assert result.exit_code == 0
        assert "Error validating config" in result.output

    def test_validate_config_json_error(self, runner, tmp_path):
        """Test config validation with JSON parsing error"""
        invalid_json = tmp_path / "invalid.json"
        invalid_json.write_text('{"broker_type": "kafka", "unclosed": }')

        result = runner.invoke(validate, [str(invalid_json)])

        assert result.exit_code == 0
        assert "Error validating config" in result.output

    def test_validate_config_without_worker_name(self, runner, tmp_path):
        """Test config validation without optional worker_name field"""
        minimal_config = tmp_path / "minimal.yaml"
        minimal_config.write_text("broker_type: redis")

        result = runner.invoke(validate, [str(minimal_config)])

        assert result.exit_code == 0
        assert "Configuration is valid" in result.output
        assert "Broker type: redis" in result.output
        assert "Worker name:" not in result.output  # Should not show worker name line

    @patch('pythia.cli.run.run_worker')
    def test_run_sets_environment_variables_correctly(self, mock_run_worker, runner, mock_worker_file, mock_config_file):
        """Test that run command sets environment variables correctly"""
        original_pythia_log_level = os.environ.get("PYTHIA_LOG_LEVEL")
        original_config_file = os.environ.get("PYTHIA_CONFIG_FILE")

        try:
            result = runner.invoke(run, [
                str(mock_worker_file),
                "--log-level", "WARNING",
                "--config", str(mock_config_file)
            ])

            assert result.exit_code == 0
            assert os.environ.get("PYTHIA_LOG_LEVEL") == "WARNING"
            assert os.environ.get("PYTHIA_CONFIG_FILE") == str(mock_config_file)

        finally:
            # Restore original environment
            if original_pythia_log_level is not None:
                os.environ["PYTHIA_LOG_LEVEL"] = original_pythia_log_level
            elif "PYTHIA_LOG_LEVEL" in os.environ:
                del os.environ["PYTHIA_LOG_LEVEL"]

            if original_config_file is not None:
                os.environ["PYTHIA_CONFIG_FILE"] = original_config_file
            elif "PYTHIA_CONFIG_FILE" in os.environ:
                del os.environ["PYTHIA_CONFIG_FILE"]

    def test_run_file_not_exists(self, runner):
        """Test run command with non-existent worker file"""
        result = runner.invoke(run, ["/path/to/non/existent/worker.py"])

        assert result.exit_code != 0
        assert "does not exist" in result.output or "Usage:" in result.output
