"""
{{cookiecutter.worker_name}} - Database CDC Worker
"""

import asyncio
from typing import Any

from pythia.brokers.database import CDCWorker, DatabaseChange, ChangeType


class {{cookiecutter.worker_name.title().replace('_', '')}}(CDCWorker):
    """
    Database CDC Worker using SQLAlchemy

    Monitors database changes and processes them
    """

    def __init__(self):
        # Parse tables from configuration
        tables = [t.strip() for t in "{{cookiecutter.tables}}".split(",") if t.strip()]

        super().__init__(
            connection_string="{{cookiecutter.connection_string}}",
            tables=tables,
            poll_interval={{cookiecutter.poll_interval}},
            timestamp_column="{{cookiecutter.timestamp_column}}"
        )

    async def process_change(self, change: DatabaseChange) -> Any:
        """
        Process a database change event

        Args:
            change: DatabaseChange object containing change details

        Returns:
            Any: Processing result
        """
        self.logger.info(f"Processing {change.change_type.value} on {change.table}")

        # TODO: Implement your change processing logic here
        # Note: This polling-based CDC only detects updates/new records

        return await self._process_update(change)

    async def _process_update(self, change: DatabaseChange) -> dict:
        """Process UPDATE operations (polling only detects updates/new records)"""
        self.logger.info(f"Record changed in {change.table}: {change.primary_key}")

        # TODO: Add your processing logic
        # Example: Send notification, update cache, trigger workflow, etc.

        return {
            "operation": "update",
            "table": change.table,
            "primary_key": change.primary_key,
            "data": change.new_data,
            "processed_at": change.timestamp
        }


async def main():
    """Main function to run the CDC worker"""
    worker = {{cookiecutter.worker_name.title().replace('_', '')}}()

    try:
        # Connect to database and start CDC
        async with worker:
            await worker.start_cdc()

            # Process changes
            async for change in worker.consume_changes():
                try:
                    result = await worker.process_change(change)
                    worker.logger.info(f"Change processed successfully: {result}")
                except Exception as e:
                    worker.logger.error(f"Error processing change: {e}")

    except KeyboardInterrupt:
        worker.logger.info("Worker stopped by user")
    except Exception as e:
        worker.logger.error(f"Worker error: {e}")
    finally:
        await worker.stop_cdc()


if __name__ == "__main__":
    asyncio.run(main())
