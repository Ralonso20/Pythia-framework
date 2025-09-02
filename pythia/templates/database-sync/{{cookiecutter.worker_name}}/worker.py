"""
{{cookiecutter.worker_name}} - Database Synchronization Worker
"""

import asyncio
from typing import Any, List

from pythia.brokers.database import SyncWorker


class {{cookiecutter.worker_name.title().replace('_', '')}}(SyncWorker):
    """
    Database Synchronization Worker

    Synchronizes data between source and target databases
    """

    def __init__(self):
        sync_config = {
            'batch_size': {{cookiecutter.batch_size}},
            'mode': '{{cookiecutter.sync_mode}}',
            'conflict_resolution': 'source_wins',
            'timestamp_column': 'updated_at',
            'truncate_target': False  # Set to True for full sync with truncate
        }

        super().__init__(
            source_connection="{{cookiecutter.source_connection}}",
            target_connection="{{cookiecutter.target_connection}}",
            sync_config=sync_config
        )

    async def sync_tables(self, table_names: List[str]) -> dict:
        """
        Sync multiple tables

        Args:
            table_names: List of table names to sync

        Returns:
            dict: Sync results for all tables
        """
        results = []

        for table_name in table_names:
            self.logger.info(f"Starting sync for table: {table_name}")
            try:
                result = await self.sync_table(table_name)
                results.append(result)
                self.logger.info(f"Table sync completed: {table_name}")
            except Exception as e:
                self.logger.error(f"Error syncing table {table_name}: {e}")
                results.append({
                    'table': table_name,
                    'error': str(e),
                    'success': False
                })

        return {
            'total_tables': len(table_names),
            'successful_syncs': len([r for r in results if 'error' not in r]),
            'failed_syncs': len([r for r in results if 'error' in r]),
            'results': results
        }

    async def sync_all_tables(self) -> dict:
        """
        Sync all tables in the source database

        Returns:
            dict: Sync results for all tables
        """
        # Get all tables from source
        tables = await self._get_all_source_tables()
        return await self.sync_tables(tables)

    async def _get_all_source_tables(self) -> List[str]:
        """Get list of all tables in source database"""
        async with self.get_session() as session:
            # Generic query that works with both PostgreSQL and MySQL
            try:
                # Try PostgreSQL first
                query = """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                """
                result = await session.execute(query)
                return [row[0] for row in result]
            except:
                # Fallback to MySQL
                query = "SHOW TABLES"
                result = await session.execute(query)
                return [row[0] for row in result]

    async def validate_sync(self, table_name: str) -> dict:
        """
        Validate sync by comparing row counts and checksums

        Args:
            table_name: Table to validate

        Returns:
            dict: Validation results
        """
        try:
            source_count = await self._get_table_count(table_name, 'source')
            target_count = await self._get_table_count(table_name, 'target')

            # Simple validation - could be enhanced with checksums
            is_valid = source_count == target_count

            return {
                'table': table_name,
                'source_count': source_count,
                'target_count': target_count,
                'is_valid': is_valid,
                'difference': abs(source_count - target_count)
            }

        except Exception as e:
            self.logger.error(f"Error validating table {table_name}: {e}")
            return {
                'table': table_name,
                'error': str(e),
                'is_valid': False
            }


async def sync_specific_tables():
    """Sync specific tables"""
    worker = {{cookiecutter.worker_name.title().replace('_', '')}}()

    # Define tables to sync
    tables_to_sync = [
        'users',
        'orders',
        'products',
        # Add your tables here
    ]

    try:
        async with worker:
            result = await worker.sync_tables(tables_to_sync)

            print(f"Sync completed:")
            print(f"- Total tables: {result['total_tables']}")
            print(f"- Successful: {result['successful_syncs']}")
            print(f"- Failed: {result['failed_syncs']}")

            # Validate synced tables
            for table_result in result['results']:
                if 'error' not in table_result:
                    validation = await worker.validate_sync(table_result['table'])
                    print(f"Validation for {table_result['table']}: {validation}")

    except Exception as e:
        print(f"Sync error: {e}")


async def sync_entire_database():
    """Sync entire database"""
    worker = {{cookiecutter.worker_name.title().replace('_', '')}}()

    try:
        async with worker:
            result = await worker.sync_all_tables()

            print(f"Database sync completed:")
            print(f"- Total tables: {result['total_tables']}")
            print(f"- Successful: {result['successful_syncs']}")
            print(f"- Failed: {result['failed_syncs']}")

    except Exception as e:
        print(f"Database sync error: {e}")


async def sync_single_table(table_name: str):
    """Sync a single table"""
    worker = {{cookiecutter.worker_name.title().replace('_', '')}}()

    try:
        async with worker:
            result = await worker.sync_table(table_name)
            print(f"Table sync result: {result}")

            # Validate the sync
            validation = await worker.validate_sync(table_name)
            print(f"Validation result: {validation}")

    except Exception as e:
        print(f"Table sync error: {e}")


async def main():
    """Main function - choose your sync strategy"""
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python worker.py tables    # Sync specific tables")
        print("  python worker.py all       # Sync all tables")
        print("  python worker.py <table>   # Sync single table")
        return

    command = sys.argv[1]

    if command == "tables":
        await sync_specific_tables()
    elif command == "all":
        await sync_entire_database()
    else:
        # Treat as table name
        await sync_single_table(command)


if __name__ == "__main__":
    asyncio.run(main())
