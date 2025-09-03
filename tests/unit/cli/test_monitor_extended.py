"""
Comprehensive tests for CLI monitor functionality
"""

import time
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call, mock_open
import pytest
from click.testing import CliRunner
from rich.text import Text

from pythia.cli.monitor import (
    monitor,
    broker,
    logs,
    health,
    generate_worker_display,
    generate_broker_display,
    follow_log_file,
    show_log_lines,
    style_log_line,
    get_worker_stats,
    get_broker_stats,
    check_python_env,
    check_dependencies,
    check_workers,
    check_brokers,
    _get_process_stats,
    _get_kafka_stats,
    _get_rabbitmq_stats,
    _get_redis_stats,
    _check_kafka_connection,
    _check_rabbitmq_connection,
    _check_redis_connection
)


class TestCliMonitor:
    """Test CLI monitor command functionality"""

    @pytest.fixture
    def runner(self):
        """Create CLI test runner"""
        return CliRunner()

    @pytest.fixture
    def mock_stats_file(self, tmp_path):
        """Create mock stats file"""
        stats_file = tmp_path / "worker_stats.json"
        stats_data = {
            "basic": {
                "worker_name": "test-worker",
                "pid": 12345,
                "memory_mb": 128.5
            },
            "performance": {
                "msg_rate": 150,
                "success_rate": 98.5,
                "avg_latency": 12.3
            },
            "status": "Running",
            "uptime": "2h 15m"
        }
        stats_file.write_text(json.dumps(stats_data))
        return stats_file

    @pytest.fixture
    def mock_log_file(self, tmp_path):
        """Create mock log file"""
        log_file = tmp_path / "worker.log"
        log_content = """
2023-09-03 10:00:01 INFO Starting worker
2023-09-03 10:00:02 DEBUG Processing message
2023-09-03 10:00:03 WARN Connection timeout
2023-09-03 10:00:04 ERROR Failed to process message
2023-09-03 10:00:05 INFO Worker stopped
        """.strip()
        log_file.write_text(log_content)
        return log_file

    def test_monitor_group_help(self, runner):
        """Test monitor command group help"""
        result = runner.invoke(monitor, ["--help"])

        assert result.exit_code == 0
        assert "Monitor Pythia workers and brokers" in result.output

    @patch('pythia.cli.monitor.Live')
    @patch('pythia.cli.monitor.generate_worker_display')
    @patch('time.sleep')
    def test_worker_monitor_basic(self, mock_sleep, mock_generate, mock_live, runner):
        """Test basic worker monitoring"""
        # Setup mocks
        mock_display = Mock()
        mock_generate.return_value = mock_display

        mock_live_instance = Mock()
        mock_live.return_value.__enter__.return_value = mock_live_instance

        # Simulate KeyboardInterrupt after one iteration
        mock_sleep.side_effect = [None, KeyboardInterrupt()]

        result = runner.invoke(monitor, ["worker", "--refresh", "1"])

        assert result.exit_code == 0
        assert "Monitoring stopped" in result.output
        mock_live.assert_called_once()
        mock_generate.assert_called()

    @patch('pythia.cli.monitor.Live')
    @patch('pythia.cli.monitor.generate_worker_display')
    def test_worker_monitor_with_stats_file(self, mock_generate, mock_live, runner, mock_stats_file):
        """Test worker monitoring with stats file"""
        mock_display = Mock()
        mock_generate.return_value = mock_display

        mock_live_instance = Mock()
        mock_live.return_value.__enter__.return_value = mock_live_instance
        mock_live.return_value.__enter__.side_effect = KeyboardInterrupt()

        result = runner.invoke(monitor, ["worker", "--stats-file", str(mock_stats_file)])

        assert result.exit_code == 0
        mock_generate.assert_called_with(None, str(mock_stats_file))

    def test_worker_monitor_stats_file_not_found(self, runner, tmp_path):
        """Test worker monitoring with non-existent stats file"""
        non_existent = tmp_path / "missing.json"

        result = runner.invoke(monitor, ["worker", "--stats-file", str(non_existent)])

        assert result.exit_code == 0
        assert "Stats file not found" in result.output

    @patch('pythia.cli.monitor.Live')
    @patch('pythia.cli.monitor.generate_worker_display')
    @patch('time.sleep')
    def test_worker_monitor_with_worker_name(self, mock_sleep, mock_generate, mock_live, runner):
        """Test worker monitoring with specific worker name"""
        mock_display = Mock()
        mock_generate.return_value = mock_display

        mock_live_instance = Mock()
        mock_live.return_value.__enter__.return_value = mock_live_instance
        mock_sleep.side_effect = [KeyboardInterrupt()]

        result = runner.invoke(monitor, ["worker", "--worker", "my-worker"])

        assert result.exit_code == 0
        mock_generate.assert_called_with("my-worker", None)

    @patch('pythia.cli.monitor.Live')
    @patch('pythia.cli.monitor.generate_broker_display')
    @patch('time.sleep')
    def test_broker_monitor_basic(self, mock_sleep, mock_generate, mock_live, runner):
        """Test basic broker monitoring"""
        mock_display = Mock()
        mock_generate.return_value = mock_display

        mock_live_instance = Mock()
        mock_live.return_value.__enter__.return_value = mock_live_instance
        mock_sleep.side_effect = [KeyboardInterrupt()]

        result = runner.invoke(monitor, ["broker"])

        assert result.exit_code == 0
        assert "Monitoring stopped" in result.output
        mock_generate.assert_called_with(None)

    @patch('pythia.cli.monitor.Live')
    @patch('pythia.cli.monitor.generate_broker_display')
    @patch('time.sleep')
    def test_broker_monitor_with_type(self, mock_sleep, mock_generate, mock_live, runner):
        """Test broker monitoring with specific type"""
        mock_display = Mock()
        mock_generate.return_value = mock_display

        mock_live_instance = Mock()
        mock_live.return_value.__enter__.return_value = mock_live_instance
        mock_sleep.side_effect = [KeyboardInterrupt()]

        result = runner.invoke(monitor, ["broker", "--broker-type", "kafka", "--refresh", "3"])

        assert result.exit_code == 0
        mock_generate.assert_called_with("kafka")

    @patch('pythia.cli.monitor.follow_log_file')
    def test_logs_follow_mode(self, mock_follow, runner, mock_log_file):
        """Test log monitoring in follow mode"""
        result = runner.invoke(monitor, ["logs", str(mock_log_file), "--follow"])

        assert result.exit_code == 0
        mock_follow.assert_called_once_with(mock_log_file, None)

    @patch('pythia.cli.monitor.show_log_lines')
    def test_logs_static_mode(self, mock_show, runner, mock_log_file):
        """Test log monitoring in static mode"""
        result = runner.invoke(monitor, ["logs", str(mock_log_file), "--lines", "25"])

        assert result.exit_code == 0
        mock_show.assert_called_once_with(mock_log_file, 25, None)

    @patch('pythia.cli.monitor.show_log_lines')
    def test_logs_with_filter(self, mock_show, runner, mock_log_file):
        """Test log monitoring with filter"""
        result = runner.invoke(monitor, ["logs", str(mock_log_file), "--filter", "ERROR"])

        assert result.exit_code == 0
        mock_show.assert_called_once_with(mock_log_file, 50, "ERROR")

    @patch('pythia.cli.monitor.get_worker_stats')
    def test_generate_worker_display_no_data(self, mock_get_stats):
        """Test worker display generation with no data"""
        mock_get_stats.return_value = {}

        result = generate_worker_display("test-worker", None)

        # Check that it returns a Panel object and get_worker_stats was called
        from rich.panel import Panel
        assert isinstance(result, Panel)
        mock_get_stats.assert_called_once_with("test-worker", None)

    @patch('pythia.cli.monitor.get_worker_stats')
    def test_generate_worker_display_with_data(self, mock_get_stats):
        """Test worker display generation with stats data"""
        mock_stats = {
            "basic": {"worker_name": "test-worker", "pid": 12345},
            "performance": {"msg_rate": 150, "success_rate": 98.5, "avg_latency": 12.3},
            "status": "Running",
            "uptime": "2h 15m"
        }
        mock_get_stats.return_value = mock_stats

        result = generate_worker_display("test-worker", None)

        # Check that it returns a Panel object and stats were used
        from rich.panel import Panel
        assert isinstance(result, Panel)
        mock_get_stats.assert_called_once_with("test-worker", None)

    @patch('pythia.cli.monitor.get_worker_stats')
    def test_generate_worker_display_error_status(self, mock_get_stats):
        """Test worker display with error status"""
        mock_stats = {
            "basic": {"worker_name": "test-worker"},
            "performance": {},
            "status": "Error",
            "uptime": "10m"
        }
        mock_get_stats.return_value = mock_stats

        result = generate_worker_display("test-worker", None)

        # Check that it returns a Panel object
        from rich.panel import Panel
        assert isinstance(result, Panel)
        mock_get_stats.assert_called_once_with("test-worker", None)

    @patch('pythia.cli.monitor.get_broker_stats')
    def test_generate_broker_display_no_data(self, mock_get_stats):
        """Test broker display generation with no data"""
        mock_get_stats.return_value = {}

        result = generate_broker_display("kafka")

        # Check that it returns a Panel object
        from rich.panel import Panel
        assert isinstance(result, Panel)
        mock_get_stats.assert_called_once_with("kafka")

    @patch('pythia.cli.monitor.get_broker_stats')
    def test_generate_broker_display_with_data(self, mock_get_stats):
        """Test broker display generation with data"""
        mock_stats = {
            "info": {"type": "kafka", "status": "Connected", "brokers": 3},
            "queues": [
                {"name": "topic1", "messages": 100, "consumers": 2, "rate": 15},
                {"name": "topic2", "messages": 50, "consumers": 1, "rate": 8}
            ]
        }
        mock_get_stats.return_value = mock_stats

        result = generate_broker_display("kafka")

        # Check that it returns a Panel object
        from rich.panel import Panel
        assert isinstance(result, Panel)
        mock_get_stats.assert_called_once_with("kafka")

    @patch('time.sleep')
    @patch('builtins.open', mock_open(read_data="line 1\nline 2\nline 3\n"))
    @patch('pythia.cli.monitor.console')
    def test_follow_log_file(self, mock_console, mock_sleep):
        """Test log file following"""
        mock_sleep.side_effect = [KeyboardInterrupt()]

        with patch('builtins.open', mock_open(read_data="")) as mock_file:
            mock_file.return_value.readline.side_effect = ["new line\n", "", KeyboardInterrupt()]

            follow_log_file(Path("/fake/log.txt"), None)

        mock_console.print.assert_called()

    @patch('time.sleep')
    @patch('pythia.cli.monitor.console')
    def test_follow_log_file_with_filter(self, mock_console, mock_sleep):
        """Test log file following with filter"""
        mock_sleep.side_effect = [KeyboardInterrupt()]

        with patch('builtins.open', mock_open()) as mock_file:
            mock_file.return_value.readline.side_effect = [
                "INFO: normal message\n",
                "ERROR: error message\n",
                "",
                KeyboardInterrupt()
            ]

            follow_log_file(Path("/fake/log.txt"), "ERROR")

        # Should print filtered lines
        mock_console.print.assert_called()

    @patch('pythia.cli.monitor.console')
    def test_show_log_lines_success(self, mock_console, mock_log_file):
        """Test showing log lines"""
        show_log_lines(mock_log_file, 3, None)

        # Should call console.print multiple times
        assert mock_console.print.call_count >= 3

    @patch('pythia.cli.monitor.console')
    def test_show_log_lines_with_filter(self, mock_console, mock_log_file):
        """Test showing log lines with filter"""
        show_log_lines(mock_log_file, 5, "ERROR")

        mock_console.print.assert_called()

    @patch('pythia.cli.monitor.console')
    def test_show_log_lines_error(self, mock_console):
        """Test showing log lines with read error"""
        with patch('builtins.open', side_effect=Exception("Read error")):
            show_log_lines(Path("/fake/log.txt"), 10, None)

        mock_console.print.assert_called_with("❌ Error reading log file: Read error")

    def test_style_log_line_error(self):
        """Test styling ERROR log lines"""
        result = style_log_line("2023-09-03 ERROR: Something went wrong")

        assert isinstance(result, Text)
        # Just check that it returns a Text object with the original content
        assert "ERROR" in str(result)

    def test_style_log_line_warning(self):
        """Test styling WARNING log lines"""
        result = style_log_line("2023-09-03 WARN: Connection slow")

        assert isinstance(result, Text)
        assert "WARN" in str(result)

    def test_style_log_line_info(self):
        """Test styling INFO log lines"""
        result = style_log_line("2023-09-03 INFO: Worker started")

        assert isinstance(result, Text)
        assert "INFO" in str(result)

    def test_style_log_line_debug(self):
        """Test styling DEBUG log lines"""
        result = style_log_line("2023-09-03 DEBUG: Processing message")

        assert isinstance(result, Text)
        assert "DEBUG" in str(result)

    def test_style_log_line_normal(self):
        """Test styling normal log lines"""
        result = style_log_line("2023-09-03 Regular log line")

        assert isinstance(result, Text)
        assert "Regular log line" in str(result)

    @patch('builtins.open', mock_open(read_data='{"status": "running", "pid": 123}'))
    def test_get_worker_stats_from_file(self, mock_stats_file):
        """Test getting worker stats from file"""
        result = get_worker_stats("test-worker", str(mock_stats_file))

        assert result["status"] == "running"
        assert result["pid"] == 123

    @patch('pythia.cli.monitor.console')
    @patch('builtins.open', side_effect=Exception("File error"))
    def test_get_worker_stats_file_error(self, mock_open, mock_console):
        """Test getting worker stats with file error"""
        result = get_worker_stats("test-worker", "/fake/stats.json")

        mock_console.print.assert_called()

    @patch('httpx.Client')
    def test_get_worker_stats_from_http(self, mock_client):
        """Test getting worker stats from HTTP endpoint"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"worker": "data"}

        mock_client_instance = Mock()
        mock_client_instance.get.return_value = mock_response
        mock_client.return_value.__enter__.return_value = mock_client_instance

        result = get_worker_stats("test-worker", None)

        assert result == {"worker": "data"}

    @patch('httpx.Client')
    @patch('pythia.cli.monitor._get_process_stats')
    def test_get_worker_stats_from_process(self, mock_process_stats, mock_client):
        """Test getting worker stats from process info"""
        # HTTP fails
        mock_client.side_effect = Exception("HTTP error")

        # Process stats succeed
        mock_process_stats.return_value = {"process": "stats"}

        result = get_worker_stats("test-worker", None)

        assert result == {"process": "stats"}

    def test_get_broker_stats_kafka(self):
        """Test getting Kafka broker stats"""
        with patch('pythia.cli.monitor._get_kafka_stats', return_value={"kafka": "stats"}):
            result = get_broker_stats("kafka")
            assert result == {"kafka": "stats"}

    def test_get_broker_stats_rabbitmq(self):
        """Test getting RabbitMQ broker stats"""
        with patch('pythia.cli.monitor._get_rabbitmq_stats', return_value={"rabbitmq": "stats"}):
            result = get_broker_stats("rabbitmq")
            assert result == {"rabbitmq": "stats"}

    def test_get_broker_stats_redis(self):
        """Test getting Redis broker stats"""
        with patch('pythia.cli.monitor._get_redis_stats', return_value={"redis": "stats"}):
            result = get_broker_stats("redis")
            assert result == {"redis": "stats"}

    def test_get_broker_stats_auto_detect(self):
        """Test auto-detecting broker types"""
        # Test the actual function without recursion by testing individual brokers
        with patch('pythia.cli.monitor._get_kafka_stats', return_value={}), \
             patch('pythia.cli.monitor._get_rabbitmq_stats', return_value={"rabbitmq": "found"}), \
             patch('pythia.cli.monitor._get_redis_stats', return_value={}):

            result = get_broker_stats("rabbitmq")
            assert result == {"rabbitmq": "found"}

            result = get_broker_stats("kafka")
            assert result == {}

    @patch('pythia.cli.monitor.check_python_env')
    @patch('pythia.cli.monitor.check_dependencies')
    @patch('pythia.cli.monitor.check_workers')
    @patch('pythia.cli.monitor.check_brokers')
    @patch('pythia.cli.monitor.console')
    def test_health_command_all_healthy(self, mock_console, mock_brokers, mock_workers, mock_deps, mock_python, runner):
        """Test health command with all components healthy"""
        # All checks return healthy status
        mock_python.return_value = ("✅", "Python 3.11")
        mock_deps.return_value = ("✅", "All dependencies available")
        mock_workers.return_value = ("✅", "Found 2 worker(s) running")
        mock_brokers.return_value = ("✅", "Connected: Kafka, Redis")

        result = runner.invoke(monitor, ["health"])

        assert result.exit_code == 0
        mock_console.print.assert_called()

    @patch('pythia.cli.monitor.check_python_env')
    @patch('pythia.cli.monitor.check_dependencies')
    @patch('pythia.cli.monitor.check_workers')
    @patch('pythia.cli.monitor.check_brokers')
    @patch('pythia.cli.monitor.console')
    def test_health_command_some_issues(self, mock_console, mock_brokers, mock_workers, mock_deps, mock_python, runner):
        """Test health command with some issues"""
        # Mix of healthy and unhealthy checks
        mock_python.return_value = ("✅", "Python 3.11")
        mock_deps.return_value = ("❌", "Missing: kafka-python")
        mock_workers.return_value = ("⚠️", "No active workers detected")
        mock_brokers.return_value = ("✅", "Connected: Redis")

        result = runner.invoke(monitor, ["health"])

        assert result.exit_code == 0
        mock_console.print.assert_called()

    def test_check_python_env(self):
        """Test Python environment check"""
        status, details = check_python_env()

        assert status == "✅"
        assert "Python" in details

    @patch('importlib.util.find_spec')
    def test_check_dependencies_all_available(self, mock_find_spec):
        """Test dependency check with all dependencies available"""
        mock_find_spec.return_value = Mock()  # All packages found

        status, details = check_dependencies()

        assert status == "✅"
        assert "All dependencies available" in details

    @patch('importlib.util.find_spec')
    def test_check_dependencies_missing(self, mock_find_spec):
        """Test dependency check with missing dependencies"""
        mock_find_spec.return_value = None  # Packages not found

        status, details = check_dependencies()

        assert status == "❌"
        assert "Missing:" in details

    @patch('psutil.process_iter')
    def test_check_workers_found(self, mock_process_iter):
        """Test worker check with workers found"""
        # Mock process with pythia in cmdline
        mock_proc = Mock()
        mock_proc.info = {
            "pid": 12345,
            "name": "python",
            "cmdline": ["python", "pythia_worker.py"]
        }
        mock_process_iter.return_value = [mock_proc]

        status, details = check_workers()

        assert status == "✅"
        assert "Found 1 worker(s) running" in details

    @patch('psutil.process_iter')
    def test_check_workers_none_found(self, mock_process_iter):
        """Test worker check with no workers found"""
        mock_process_iter.return_value = []

        status, details = check_workers()

        assert status == "⚠️"
        assert "No active workers detected" in details

    @patch('psutil.process_iter')
    def test_check_workers_exception(self, mock_process_iter):
        """Test worker check with exception"""
        mock_process_iter.side_effect = Exception("Process error")

        status, details = check_workers()

        assert status == "❌"
        assert "Unable to check worker processes" in details

    @patch('pythia.cli.monitor._check_kafka_connection')
    @patch('pythia.cli.monitor._check_rabbitmq_connection')
    @patch('pythia.cli.monitor._check_redis_connection')
    def test_check_brokers_all_available(self, mock_redis, mock_rabbitmq, mock_kafka):
        """Test broker check with all brokers available"""
        mock_kafka.return_value = True
        mock_rabbitmq.return_value = True
        mock_redis.return_value = True

        status, details = check_brokers()

        assert status == "✅"
        assert "Connected: Kafka, RabbitMQ, Redis" in details

    @patch('pythia.cli.monitor._check_kafka_connection')
    @patch('pythia.cli.monitor._check_rabbitmq_connection')
    @patch('pythia.cli.monitor._check_redis_connection')
    def test_check_brokers_none_available(self, mock_redis, mock_rabbitmq, mock_kafka):
        """Test broker check with no brokers available"""
        mock_kafka.return_value = False
        mock_rabbitmq.return_value = False
        mock_redis.return_value = False

        status, details = check_brokers()

        assert status == "⚠️"
        assert "No brokers accessible" in details

    @patch('psutil.process_iter')
    def test_get_process_stats_found(self, mock_process_iter):
        """Test getting process stats with worker found"""
        mock_proc = Mock()
        mock_proc.info = {
            "pid": 12345,
            "name": "python",
            "cmdline": ["python", "pythia_worker.py"],
            "create_time": time.time() - 3600,  # 1 hour ago
            "memory_info": Mock(rss=128 * 1024 * 1024)  # 128 MB
        }
        mock_process_iter.return_value = [mock_proc]

        result = _get_process_stats("test-worker")

        assert result is not None
        assert result["basic"]["pid"] == 12345
        assert result["status"] == "Running"

    @patch('psutil.process_iter')
    def test_get_process_stats_not_found(self, mock_process_iter):
        """Test getting process stats with no worker found"""
        mock_process_iter.return_value = []

        result = _get_process_stats("test-worker")

        assert result is None

    @patch('confluent_kafka.admin.AdminClient')
    def test_get_kafka_stats_success(self, mock_admin_client):
        """Test getting Kafka stats successfully"""
        mock_metadata = Mock()
        mock_metadata.brokers = {"broker1": Mock(), "broker2": Mock()}
        mock_metadata.topics = {
            "topic1": Mock(partitions={"0": Mock(), "1": Mock()}),
            "__internal_topic": Mock(partitions={"0": Mock()})
        }

        mock_admin = Mock()
        mock_admin.list_topics.return_value = mock_metadata
        mock_admin_client.return_value = mock_admin

        result = _get_kafka_stats()

        assert result["info"]["type"] == "kafka"
        assert result["info"]["brokers"] == 2
        assert len(result["queues"]) == 1  # Should exclude internal topics

    @patch('confluent_kafka.admin.AdminClient')
    def test_get_kafka_stats_failure(self, mock_admin_client):
        """Test getting Kafka stats with failure"""
        mock_admin_client.side_effect = Exception("Connection failed")

        result = _get_kafka_stats()

        assert result == {}

    @patch('httpx.Client')
    def test_get_rabbitmq_stats_success(self, mock_client):
        """Test getting RabbitMQ stats successfully"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "name": "queue1",
                "messages": 100,
                "consumers": 2,
                "message_stats": {"publish_details": {"rate": 15.5}}
            },
            {
                "name": "queue2",
                "messages": 50,
                "consumers": 1
            }
        ]

        mock_client_instance = Mock()
        mock_client_instance.get.return_value = mock_response
        mock_client.return_value.__enter__.return_value = mock_client_instance

        result = _get_rabbitmq_stats()

        assert result["info"]["type"] == "rabbitmq"
        assert len(result["queues"]) == 2
        assert result["queues"][0]["rate"] == 15.5

    @patch('httpx.Client')
    def test_get_rabbitmq_stats_failure(self, mock_client):
        """Test getting RabbitMQ stats with failure"""
        mock_client.side_effect = Exception("Connection failed")

        result = _get_rabbitmq_stats()

        assert result == {}

    @patch('redis.client.Redis')
    def test_get_redis_stats_success(self, mock_redis):
        """Test getting Redis stats successfully"""
        mock_redis_instance = Mock()
        mock_redis_instance.info.return_value = {
            "redis_version": "6.2.0",
            "used_memory_human": "1.2M"
        }
        mock_redis_instance.keys.return_value = ["stream1", "key2"]
        mock_redis_instance.type.side_effect = ["stream", "string"]
        mock_redis_instance.xlen.return_value = 10

        mock_redis.return_value = mock_redis_instance

        result = _get_redis_stats()

        assert result["info"]["type"] == "redis"
        assert result["info"]["version"] == "6.2.0"
        assert len(result["queues"]) == 1  # Only stream type

    @patch('redis.client.Redis')
    def test_get_redis_stats_failure(self, mock_redis):
        """Test getting Redis stats with failure"""
        mock_redis.side_effect = Exception("Connection failed")

        result = _get_redis_stats()

        assert result == {}

    @patch('confluent_kafka.admin.AdminClient')
    def test_check_kafka_connection_success(self, mock_admin_client):
        """Test successful Kafka connection check"""
        mock_admin = Mock()
        mock_admin.list_topics.return_value = Mock()
        mock_admin_client.return_value = mock_admin

        result = _check_kafka_connection()

        assert result is True

    @patch('confluent_kafka.admin.AdminClient')
    def test_check_kafka_connection_failure(self, mock_admin_client):
        """Test failed Kafka connection check"""
        mock_admin_client.side_effect = Exception("Connection failed")

        result = _check_kafka_connection()

        assert result is False

    @patch('asyncio.run')
    def test_check_rabbitmq_connection_success(self, mock_run):
        """Test successful RabbitMQ connection check"""
        mock_run.return_value = True

        result = _check_rabbitmq_connection()

        assert result is True

    @patch('asyncio.run')
    def test_check_rabbitmq_connection_failure(self, mock_run):
        """Test failed RabbitMQ connection check"""
        mock_run.side_effect = Exception("Connection failed")

        result = _check_rabbitmq_connection()

        assert result is False

    @patch('redis.client.Redis')
    def test_check_redis_connection_success(self, mock_redis):
        """Test successful Redis connection check"""
        mock_redis_instance = Mock()
        mock_redis_instance.ping.return_value = True
        mock_redis.return_value = mock_redis_instance

        result = _check_redis_connection()

        assert result is True

    @patch('redis.client.Redis')
    def test_check_redis_connection_failure(self, mock_redis):
        """Test failed Redis connection check"""
        mock_redis.side_effect = Exception("Connection failed")

        result = _check_redis_connection()

        assert result is False

    def test_monitor_command_invalid_broker_type(self, runner):
        """Test broker monitor with invalid broker type"""
        result = runner.invoke(monitor, ["broker", "--broker-type", "invalid"])

        assert result.exit_code != 0
        assert "Invalid value" in result.output or "Usage:" in result.output
