"""
Monitor command for tracking worker status and metrics
"""

import time
from pathlib import Path
from typing import Dict, Any, Optional

import click
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.columns import Columns
from rich.text import Text


console = Console()


@click.group()
def monitor():
    """Monitor Pythia workers and brokers"""
    pass


@monitor.command()
@click.option("--worker", help="Specific worker to monitor")
@click.option("--refresh", default=2, help="Refresh interval in seconds")
@click.option("--stats-file", help="Path to worker stats file")
def worker(worker_name: Optional[str], refresh: int, stats_file: Optional[str]):
    """
    Monitor worker metrics and status

    Examples:
      pythia monitor worker
      pythia monitor worker --worker my-worker --refresh 1
    """

    if stats_file and not Path(stats_file).exists():
        click.echo(f"❌ Stats file not found: {stats_file}")
        return

    try:
        with Live(
            generate_worker_display(worker_name, stats_file),
            refresh_per_second=1 / refresh,
        ) as live:
            while True:
                live.update(generate_worker_display(worker_name, stats_file))
                time.sleep(refresh)

    except KeyboardInterrupt:
        console.print("\n👋 Monitoring stopped")


@monitor.command()
@click.option(
    "--broker-type",
    type=click.Choice(["kafka", "rabbitmq", "redis"]),
    help="Broker type to monitor",
)
@click.option("--refresh", default=5, help="Refresh interval in seconds")
def broker(broker_type: Optional[str], refresh: int):
    """
    Monitor broker health and queues

    Examples:
      pythia monitor broker --broker-type kafka
      pythia monitor broker --broker-type rabbitmq --refresh 3
    """

    try:
        with Live(
            generate_broker_display(broker_type), refresh_per_second=1 / refresh
        ) as live:
            while True:
                live.update(generate_broker_display(broker_type))
                time.sleep(refresh)

    except KeyboardInterrupt:
        console.print("\n👋 Monitoring stopped")


@monitor.command()
@click.argument("log_file", type=click.Path(exists=True))
@click.option("--follow", "-f", is_flag=True, help="Follow log file")
@click.option("--lines", "-n", default=50, help="Number of lines to show")
@click.option("--filter", help="Filter log entries")
def logs(log_file: str, follow: bool, lines: int, filter: Optional[str]):
    """
    Monitor worker logs

    Examples:
      pythia monitor logs worker.log
      pythia monitor logs worker.log --follow --filter ERROR
    """

    log_path = Path(log_file)

    if follow:
        # Follow mode - tail the file
        follow_log_file(log_path, filter)
    else:
        # Static mode - show last N lines
        show_log_lines(log_path, lines, filter)


def generate_worker_display(
    worker_name: Optional[str], stats_file: Optional[str]
) -> Panel:
    """Generate rich display for worker monitoring"""

    # Get worker stats (mock data for now)
    stats = get_worker_stats(worker_name, stats_file)

    if not stats:
        return Panel(
            "[red]No worker data available[/red]\n\n"
            "Make sure your worker is running and stats are enabled.",
            title="🐍 Pythia Worker Monitor",
            border_style="red",
        )

    # Create main stats table
    stats_table = Table(title="Worker Statistics")
    stats_table.add_column("Metric", style="cyan")
    stats_table.add_column("Value", style="green")

    for key, value in stats.get("basic", {}).items():
        stats_table.add_row(key.replace("_", " ").title(), str(value))

    # Create performance metrics
    perf_table = Table(title="Performance")
    perf_table.add_column("Metric", style="cyan")
    perf_table.add_column("Value", style="yellow")
    perf_table.add_column("Rate", style="magenta")

    perf_data = stats.get("performance", {})
    perf_table.add_row("Messages/sec", str(perf_data.get("msg_rate", "0")), "📈")
    perf_table.add_row("Success Rate", f"{perf_data.get('success_rate', 0):.1f}%", "✅")
    perf_table.add_row("Avg Latency", f"{perf_data.get('avg_latency', 0):.2f}ms", "⏱️")

    # Create status info
    status = stats.get("status", "Unknown")
    status_color = (
        "green" if status == "Running" else "red" if status == "Error" else "yellow"
    )

    status_text = Text()
    status_text.append("Status: ", style="bold")
    status_text.append(status, style=f"bold {status_color}")
    status_text.append(f"\nUptime: {stats.get('uptime', 'Unknown')}")
    status_text.append(f"\nLast Updated: {time.strftime('%H:%M:%S')}")

    # Combine displays
    display = Columns(
        [
            Panel(stats_table, border_style="blue"),
            Panel(perf_table, border_style="green"),
            Panel(status_text, title="Status", border_style=status_color),
        ]
    )

    return Panel(
        display,
        title=f"🐍 Pythia Worker Monitor {f'- {worker_name}' if worker_name else ''}",
        border_style="bright_blue",
    )


def generate_broker_display(broker_type: Optional[str]) -> Panel:
    """Generate rich display for broker monitoring"""

    # Get broker stats (mock data for now)
    stats = get_broker_stats(broker_type)

    if not stats:
        return Panel(
            f"[red]No {broker_type or 'broker'} data available[/red]\n\n"
            "Make sure your broker is running and accessible.",
            title="📡 Pythia Broker Monitor",
            border_style="red",
        )

    # Create broker info table
    info_table = Table(title="Broker Information")
    info_table.add_column("Property", style="cyan")
    info_table.add_column("Value", style="green")

    for key, value in stats.get("info", {}).items():
        info_table.add_row(key.replace("_", " ").title(), str(value))

    # Create queues/topics table
    queues_table = Table(title="Queues/Topics")
    queues_table.add_column("Name", style="cyan")
    queues_table.add_column("Messages", style="yellow")
    queues_table.add_column("Consumers", style="green")
    queues_table.add_column("Rate", style="magenta")

    for queue_data in stats.get("queues", []):
        queues_table.add_row(
            queue_data.get("name", "Unknown"),
            str(queue_data.get("messages", 0)),
            str(queue_data.get("consumers", 0)),
            f"{queue_data.get('rate', 0)}/s",
        )

    display = Columns(
        [
            Panel(info_table, border_style="blue"),
            Panel(queues_table, border_style="green"),
        ]
    )

    return Panel(
        display,
        title=f"📡 Pythia Broker Monitor {f'- {broker_type}' if broker_type else ''}",
        border_style="bright_blue",
    )


def follow_log_file(log_path: Path, filter_text: Optional[str]):
    """Follow log file like tail -f"""

    console.print(f"👀 Following {log_path.name}...")
    console.print("Press Ctrl+C to stop\n")

    try:
        with open(log_path, "r") as f:
            # Go to end of file
            f.seek(0, 2)

            while True:
                line = f.readline()
                if line:
                    if not filter_text or filter_text.lower() in line.lower():
                        # Color code log levels
                        styled_line = style_log_line(line.rstrip())
                        console.print(styled_line)
                else:
                    time.sleep(0.1)

    except KeyboardInterrupt:
        console.print("\n👋 Log following stopped")


def show_log_lines(log_path: Path, lines: int, filter_text: Optional[str]):
    """Show last N lines of log file"""

    try:
        with open(log_path, "r") as f:
            all_lines = f.readlines()

        # Get last N lines
        start_idx = max(0, len(all_lines) - lines)
        recent_lines = all_lines[start_idx:]

        console.print(f"📋 Last {len(recent_lines)} lines from {log_path.name}:\n")

        for line in recent_lines:
            line = line.rstrip()
            if not filter_text or filter_text.lower() in line.lower():
                styled_line = style_log_line(line)
                console.print(styled_line)

    except Exception as e:
        console.print(f"❌ Error reading log file: {e}")


def style_log_line(line: str) -> Text:
    """Apply color coding to log lines based on level"""

    text = Text(line)

    if "ERROR" in line.upper():
        text.stylize("red")
    elif "WARN" in line.upper():
        text.stylize("yellow")
    elif "INFO" in line.upper():
        text.stylize("blue")
    elif "DEBUG" in line.upper():
        text.stylize("dim")

    return text


def get_worker_stats(
    worker_name: Optional[str], stats_file: Optional[str]
) -> Dict[str, Any]:
    """Get worker statistics from file or HTTP endpoint"""

    # Try to read from stats file first
    if stats_file:
        try:
            import json

            with open(stats_file) as f:
                return json.load(f)
        except Exception as e:
            console.print(f"❌ Error reading stats file: {e}")

    # Try to get stats from HTTP endpoint (common pattern)
    try:
        import httpx
        import json

        # Try common worker stats endpoints
        endpoints = [
            "http://localhost:8080/stats",
            "http://localhost:8081/metrics",
            f"http://localhost:8080/worker/{worker_name}/stats"
            if worker_name
            else None,
        ]

        for endpoint in endpoints:
            if endpoint:
                try:
                    with httpx.Client(timeout=2.0) as client:
                        response = client.get(endpoint)
                        if response.status_code == 200:
                            return response.json()
                except Exception:
                    continue
    except ImportError:
        pass

    # Try to find stats from running processes
    stats = _get_process_stats(worker_name)
    if stats:
        return stats

    # Return empty dict if no stats available
    return {}


def get_broker_stats(broker_type: Optional[str]) -> Dict[str, Any]:
    """Get broker statistics from actual brokers"""

    if broker_type == "kafka":
        return _get_kafka_stats()
    elif broker_type == "rabbitmq":
        return _get_rabbitmq_stats()
    elif broker_type == "redis":
        return _get_redis_stats()
    else:
        # Try to detect available brokers
        for btype in ["kafka", "rabbitmq", "redis"]:
            stats = get_broker_stats(btype)
            if stats:
                return stats
        return {}


@monitor.command()
def health():
    """Check overall system health"""

    console.print("🏥 Pythia System Health Check\n")

    # Check components
    checks = {
        "Python Environment": check_python_env(),
        "Dependencies": check_dependencies(),
        "Workers": check_workers(),
        "Brokers": check_brokers(),
    }

    # Create results table
    table = Table(title="Health Check Results")
    table.add_column("Component", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Details")

    overall_healthy = True

    for component, (status, details) in checks.items():
        status_style = (
            "green" if status == "✅" else "red" if status == "❌" else "yellow"
        )
        table.add_row(component, f"[{status_style}]{status}[/{status_style}]", details)

        if status == "❌":
            overall_healthy = False

    console.print(table)

    # Overall status
    if overall_healthy:
        console.print("\n[green]🎉 System is healthy![/green]")
    else:
        console.print("\n[red]⚠️ Some components need attention[/red]")


def check_python_env() -> tuple[str, str]:
    """Check Python environment"""
    import sys

    return "✅", f"Python {sys.version.split()[0]}"


def check_dependencies() -> tuple[str, str]:
    """Check if required dependencies are installed"""
    from importlib.util import find_spec

    missing = [pkg for pkg in ("pydantic", "click") if find_spec(pkg) is None]

    if not missing:
        return "✅", "All dependencies available"

    return "❌", f"Missing: {', '.join(missing)}"


def check_workers() -> tuple[str, str]:
    """Check worker processes"""
    import psutil

    try:
        pythia_processes = []
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                if proc.info["cmdline"] and any(
                    "pythia" in str(cmd).lower() for cmd in proc.info["cmdline"]
                ):
                    pythia_processes.append(proc.info["pid"])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        if pythia_processes:
            return "✅", f"Found {len(pythia_processes)} worker(s) running"
        else:
            return "⚠️", "No active workers detected"
    except Exception:
        return "❌", "Unable to check worker processes"


def check_brokers() -> tuple[str, str]:
    """Check broker connectivity"""
    available_brokers = []

    # Check Kafka
    if _check_kafka_connection():
        available_brokers.append("Kafka")

    # Check RabbitMQ
    if _check_rabbitmq_connection():
        available_brokers.append("RabbitMQ")

    # Check Redis
    if _check_redis_connection():
        available_brokers.append("Redis")

    if available_brokers:
        return "✅", f"Connected: {', '.join(available_brokers)}"
    else:
        return "⚠️", "No brokers accessible"


# Helper functions for broker stats
def _get_process_stats(worker_name: Optional[str]) -> Optional[Dict[str, Any]]:
    """Get basic stats from running processes"""
    import psutil
    import time

    try:
        for proc in psutil.process_iter(
            ["pid", "name", "cmdline", "create_time", "memory_info"]
        ):
            try:
                if proc.info["cmdline"] and any(
                    "pythia" in str(cmd).lower() for cmd in proc.info["cmdline"]
                ):
                    uptime_seconds = time.time() - proc.info["create_time"]
                    uptime = f"{int(uptime_seconds // 3600)}h {int((uptime_seconds % 3600) // 60)}m"

                    return {
                        "basic": {
                            "worker_name": worker_name or f"worker-{proc.info['pid']}",
                            "pid": proc.info["pid"],
                            "memory_mb": round(
                                proc.info["memory_info"].rss / 1024 / 1024, 1
                            ),
                        },
                        "performance": {
                            "msg_rate": "Unknown",
                            "success_rate": "Unknown",
                            "avg_latency": "Unknown",
                        },
                        "status": "Running",
                        "uptime": uptime,
                    }
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
    except Exception:
        pass
    return None


def _get_kafka_stats() -> Dict[str, Any]:
    """Get Kafka broker statistics"""
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": "localhost:9092"})
        metadata = admin.list_topics(timeout=2)

        topics = []
        for topic_name, topic_metadata in metadata.topics.items():
            if not topic_name.startswith("__"):  # Skip internal topics
                topics.append(
                    {
                        "name": topic_name,
                        "messages": "Unknown",
                        "consumers": len(topic_metadata.partitions),
                        "rate": 0,
                    }
                )

        return {
            "info": {
                "type": "kafka",
                "status": "Connected",
                "brokers": len(metadata.brokers),
            },
            "queues": topics,
        }
    except Exception:
        return {}


def _get_rabbitmq_stats() -> Dict[str, Any]:
    """Get RabbitMQ broker statistics"""
    try:
        import httpx

        # Try RabbitMQ Management API
        with httpx.Client(timeout=2.0) as client:
            response = client.get(
                "http://guest:guest@localhost:15672/api/queues", auth=("guest", "guest")
            )
            if response.status_code == 200:
                queues = response.json()

                queue_stats = []
                for q in queues[:10]:  # Limit to first 10 queues
                    queue_stats.append(
                        {
                            "name": q.get("name", "Unknown"),
                            "messages": q.get("messages", 0),
                            "consumers": q.get("consumers", 0),
                            "rate": round(
                                q.get("message_stats", {})
                                .get("publish_details", {})
                                .get("rate", 0),
                                1,
                            ),
                        }
                    )

                return {
                    "info": {
                        "type": "rabbitmq",
                        "status": "Connected",
                        "node": "localhost",
                    },
                    "queues": queue_stats,
                }
        # If we get here, the request didn't succeed
        return {}
    except Exception:
        return {}


def _get_redis_stats() -> Dict[str, Any]:
    """Get Redis broker statistics"""
    try:
        import redis.client

        r = redis.client.Redis(host="localhost", port=6379, decode_responses=True)
        info = r.info()  # type: ignore

        # Get some stream/key stats
        keys = list(r.keys("*"))[:10]  # type: ignore
        key_stats = []

        for key in keys:
            key_type = r.type(key)  # type: ignore
            if key_type == "stream":
                length = r.xlen(key)  # type: ignore
                key_stats.append(
                    {"name": key, "messages": length, "consumers": 0, "rate": 0}
                )

        return {
            "info": {
                "type": "redis",
                "version": info.get("redis_version", "Unknown"),  # type: ignore
                "status": "Connected",
                "memory": f"{info.get('used_memory_human', 'Unknown')}",  # type: ignore
            },
            "queues": key_stats,
        }
    except Exception:
        return {}


def _check_kafka_connection() -> bool:
    """Check if Kafka is accessible"""
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": "localhost:9092"})
        admin.list_topics(timeout=2)
        return True
    except Exception:
        return False


def _check_rabbitmq_connection() -> bool:
    """Check if RabbitMQ is accessible"""
    try:
        import aio_pika
        import asyncio

        async def check():
            connection = await aio_pika.connect(
                "amqp://guest:guest@localhost:5672/", timeout=2
            )
            await connection.close()
            return True

        return asyncio.run(check())
    except Exception:
        return False


def _check_redis_connection() -> bool:
    """Check if Redis is accessible"""
    try:
        import redis.client

        r = redis.client.Redis(host="localhost", port=6379, socket_timeout=2)
        r.ping()  # type: ignore
        return True
    except Exception:
        return False
