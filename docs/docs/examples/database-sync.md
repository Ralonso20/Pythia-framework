# Database Sync Worker Example

This example demonstrates how to create a database synchronization worker that replicates data between PostgreSQL and MySQL databases.

## Scenario

We'll build a data replication system that:
- Syncs customer data from PostgreSQL (production) to MySQL (analytics)
- Performs incremental syncs based on timestamps
- Validates data integrity after sync
- Handles cross-database type conversions

## Code

```python
import asyncio
from typing import List, Dict, Any
from datetime import datetime, timedelta
from pythia.brokers.database import DatabaseSyncWorker

class DataReplicationWorker(DatabaseSyncWorker):
    """
    Production to Analytics database sync worker
    """

    def __init__(self):
        sync_config = {
            'batch_size': 1000,
            'mode': 'incremental',
            'conflict_resolution': 'source_wins',
            'timestamp_column': 'updated_at',
            'truncate_target': False
        }

        super().__init__(
            source_connection="postgresql://user:password@prod-db:5432/ecommerce",
            target_connection="mysql://user:password@analytics-db:3306/analytics",
            sync_config=sync_config
        )

        # Define tables to sync with their configurations
        self.sync_tables = {
            'customers': {
                'primary_key': 'id',
                'timestamp_column': 'updated_at',
                'batch_size': 1000,
                'validations': ['email_format', 'required_fields']
            },
            'orders': {
                'primary_key': 'id',
                'timestamp_column': 'updated_at',
                'batch_size': 500,
                'validations': ['amount_positive', 'customer_exists']
            },
            'products': {
                'primary_key': 'id',
                'timestamp_column': 'updated_at',
                'batch_size': 2000,
                'validations': ['price_positive']
            },
            'order_items': {
                'primary_key': 'id',
                'timestamp_column': 'created_at',  # Different timestamp column
                'batch_size': 2000,
                'validations': ['quantity_positive']
            }
        }

    async def sync_all_configured_tables(self) -> Dict[str, Any]:
        """Sync all configured tables with their specific settings"""

        results = []
        start_time = datetime.now()

        for table_name, config in self.sync_tables.items():
            try:
                self.logger.info(f"Starting sync for {table_name}")

                # Override timestamp column for this table
                original_timestamp_column = self.timestamp_column
                self.timestamp_column = config['timestamp_column']

                # Override batch size for this table
                original_batch_size = self.batch_size
                self.batch_size = config['batch_size']

                # Perform sync
                sync_result = await self.sync_table(table_name)

                # Validate sync
                validation_result = await self.validate_sync(table_name)

                # Run custom validations
                custom_validations = await self._run_custom_validations(
                    table_name, config['validations']
                )

                # Restore original settings
                self.timestamp_column = original_timestamp_column
                self.batch_size = original_batch_size

                result = {
                    **sync_result,
                    'validation': validation_result,
                    'custom_validations': custom_validations,
                    'success': True
                }

                results.append(result)
                self.logger.info(f"✅ {table_name} sync completed: {sync_result['rows_synced']} rows")

            except Exception as e:
                self.logger.error(f"❌ Error syncing {table_name}: {e}")
                results.append({
                    'table': table_name,
                    'error': str(e),
                    'success': False
                })

        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        total_rows_synced = sum(r.get('rows_synced', 0) for r in results if 'rows_synced' in r)

        summary = {
            'total_tables': len(self.sync_tables),
            'successful_syncs': len([r for r in results if r.get('success', False)]),
            'failed_syncs': len([r for r in results if not r.get('success', False)]),
            'total_rows_synced': total_rows_synced,
            'total_duration_seconds': total_duration,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'results': results
        }

        return summary

    async def _run_custom_validations(self, table_name: str, validations: List[str]) -> Dict[str, Any]:
        """Run custom data validations"""

        validation_results = {}

        for validation in validations:
            try:
                if validation == 'email_format':
                    result = await self._validate_email_format(table_name)
                elif validation == 'required_fields':
                    result = await self._validate_required_fields(table_name)
                elif validation == 'amount_positive':
                    result = await self._validate_positive_amounts(table_name)
                elif validation == 'customer_exists':
                    result = await self._validate_customer_references(table_name)
                elif validation == 'price_positive':
                    result = await self._validate_positive_prices(table_name)
                elif validation == 'quantity_positive':
                    result = await self._validate_positive_quantities(table_name)
                else:
                    result = {'status': 'skipped', 'reason': 'Unknown validation'}

                validation_results[validation] = result

            except Exception as e:
                validation_results[validation] = {
                    'status': 'error',
                    'error': str(e)
                }

        return validation_results

    async def _validate_email_format(self, table_name: str) -> Dict[str, Any]:
        """Validate email format in target database"""

        if self.target_type == 'mysql':
            async with self.target_conn.cursor() as cursor:
                query = f"""
                    SELECT COUNT(*) as total,
                           SUM(CASE WHEN email REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{{2,}}$' THEN 1 ELSE 0 END) as valid
                    FROM {table_name}
                """
                await cursor.execute(query)
                result = await cursor.fetchone()

                return {
                    'status': 'completed',
                    'total_rows': result[0],
                    'valid_emails': result[1],
                    'invalid_emails': result[0] - result[1],
                    'valid_percentage': (result[1] / result[0] * 100) if result[0] > 0 else 0
                }

        return {'status': 'skipped', 'reason': 'Only supported for MySQL target'}

    async def _validate_required_fields(self, table_name: str) -> Dict[str, Any]:
        """Validate required fields are not null"""

        required_fields = {
            'customers': ['email', 'first_name', 'last_name'],
            'orders': ['customer_id', 'total_amount', 'status'],
            'products': ['name', 'price'],
            'order_items': ['order_id', 'product_id', 'quantity']
        }

        fields = required_fields.get(table_name, [])
        if not fields:
            return {'status': 'skipped', 'reason': 'No required fields defined'}

        validation_results = {}

        for field in fields:
            if self.target_type == 'mysql':
                async with self.target_conn.cursor() as cursor:
                    query = f"SELECT COUNT(*) FROM {table_name} WHERE {field} IS NULL"
                    await cursor.execute(query)
                    null_count = (await cursor.fetchone())[0]

                    validation_results[field] = {
                        'null_count': null_count,
                        'is_valid': null_count == 0
                    }

        return {
            'status': 'completed',
            'field_validations': validation_results,
            'all_valid': all(v['is_valid'] for v in validation_results.values())
        }

    async def _validate_positive_amounts(self, table_name: str) -> Dict[str, Any]:
        """Validate amounts are positive"""

        if self.target_type == 'mysql':
            async with self.target_conn.cursor() as cursor:
                query = f"""
                    SELECT COUNT(*) as total,
                           SUM(CASE WHEN total_amount > 0 THEN 1 ELSE 0 END) as positive
                    FROM {table_name}
                """
                await cursor.execute(query)
                result = await cursor.fetchone()

                return {
                    'status': 'completed',
                    'total_orders': result[0],
                    'positive_amounts': result[1],
                    'negative_or_zero': result[0] - result[1],
                    'valid_percentage': (result[1] / result[0] * 100) if result[0] > 0 else 0
                }

        return {'status': 'skipped', 'reason': 'Only supported for MySQL target'}

    async def _validate_customer_references(self, table_name: str) -> Dict[str, Any]:
        """Validate customer references exist"""

        if table_name == 'orders' and self.target_type == 'mysql':
            async with self.target_conn.cursor() as cursor:
                query = f"""
                    SELECT COUNT(*) as total_orders,
                           COUNT(c.id) as valid_customers
                    FROM {table_name} o
                    LEFT JOIN customers c ON o.customer_id = c.id
                """
                await cursor.execute(query)
                result = await cursor.fetchone()

                return {
                    'status': 'completed',
                    'total_orders': result[0],
                    'valid_references': result[1],
                    'orphaned_orders': result[0] - result[1],
                    'valid_percentage': (result[1] / result[0] * 100) if result[0] > 0 else 0
                }

        return {'status': 'skipped', 'reason': 'Not applicable for this table'}

    async def _validate_positive_prices(self, table_name: str) -> Dict[str, Any]:
        """Validate product prices are positive"""

        if self.target_type == 'mysql':
            async with self.target_conn.cursor() as cursor:
                query = f"""
                    SELECT COUNT(*) as total,
                           SUM(CASE WHEN price > 0 THEN 1 ELSE 0 END) as positive
                    FROM {table_name}
                """
                await cursor.execute(query)
                result = await cursor.fetchone()

                return {
                    'status': 'completed',
                    'total_products': result[0],
                    'positive_prices': result[1],
                    'invalid_prices': result[0] - result[1],
                    'valid_percentage': (result[1] / result[0] * 100) if result[0] > 0 else 0
                }

        return {'status': 'skipped'}

    async def _validate_positive_quantities(self, table_name: str) -> Dict[str, Any]:
        """Validate quantities are positive"""

        if self.target_type == 'mysql':
            async with self.target_conn.cursor() as cursor:
                query = f"""
                    SELECT COUNT(*) as total,
                           SUM(CASE WHEN quantity > 0 THEN 1 ELSE 0 END) as positive
                    FROM {table_name}
                """
                await cursor.execute(query)
                result = await cursor.fetchone()

                return {
                    'status': 'completed',
                    'total_items': result[0],
                    'positive_quantities': result[1],
                    'invalid_quantities': result[0] - result[1],
                    'valid_percentage': (result[1] / result[0] * 100) if result[0] > 0 else 0
                }

        return {'status': 'skipped'}

    async def generate_sync_report(self, sync_result: Dict[str, Any]) -> str:
        """Generate a detailed sync report"""

        report_lines = [
            "=" * 60,
            "DATABASE SYNCHRONIZATION REPORT",
            "=" * 60,
            f"Sync completed at: {sync_result['end_time']}",
            f"Total duration: {sync_result['total_duration_seconds']:.2f} seconds",
            f"Total tables processed: {sync_result['total_tables']}",
            f"Successful syncs: {sync_result['successful_syncs']}",
            f"Failed syncs: {sync_result['failed_syncs']}",
            f"Total rows synced: {sync_result['total_rows_synced']:,}",
            "",
            "TABLE DETAILS:",
            "-" * 40
        ]

        for result in sync_result['results']:
            if result.get('success'):
                table = result['table']
                rows_synced = result.get('rows_synced', 0)
                duration = result.get('duration_seconds', 0)
                validation = result.get('validation', {})

                report_lines.extend([
                    f"✅ {table}:",
                    f"   Rows synced: {rows_synced:,}",
                    f"   Duration: {duration:.2f}s",
                    f"   Rate: {rows_synced/duration:.0f} rows/sec" if duration > 0 else "   Rate: N/A",
                    f"   Validation: {'✅ PASSED' if validation.get('is_valid', False) else '❌ FAILED'}",
                    ""
                ])
            else:
                table = result['table']
                error = result.get('error', 'Unknown error')
                report_lines.extend([
                    f"❌ {table}:",
                    f"   Error: {error}",
                    ""
                ])

        return "\n".join(report_lines)


async def scheduled_sync():
    """Scheduled sync job (run every hour)"""

    worker = DataReplicationWorker()

    try:
        async with worker:
            print("🚀 Starting scheduled database sync...")

            # Run the sync
            result = await worker.sync_all_configured_tables()

            # Generate and print report
            report = await worker.generate_sync_report(result)
            print(report)

            # Check if any syncs failed
            if result['failed_syncs'] > 0:
                print(f"⚠️  {result['failed_syncs']} table sync(s) failed!")
                # In production, send alert email/notification

            return result

    except Exception as e:
        print(f"💥 Sync job failed: {e}")
        raise


async def full_resync():
    """Full database resync (run weekly or on-demand)"""

    # Create worker with full sync mode
    worker = DataReplicationWorker()
    worker.sync_mode = 'full'
    worker.sync_config['truncate_target'] = True  # Clear target tables

    try:
        async with worker:
            print("🔄 Starting FULL database resync...")
            print("⚠️  Target tables will be truncated!")

            # Confirm in production
            confirmation = input("Continue? (y/N): ")
            if confirmation.lower() != 'y':
                print("Sync cancelled.")
                return

            result = await worker.sync_all_configured_tables()

            # Generate report
            report = await worker.generate_sync_report(result)
            print(report)

            return result

    except Exception as e:
        print(f"💥 Full resync failed: {e}")
        raise


async def validate_existing_data():
    """Validate data integrity without syncing"""

    worker = DataReplicationWorker()

    try:
        async with worker:
            print("🔍 Validating existing data integrity...")

            for table_name in worker.sync_tables.keys():
                print(f"\nValidating {table_name}...")

                # Basic validation
                validation = await worker.validate_sync(table_name)
                print(f"  Row count validation: {'✅ PASS' if validation['is_valid'] else '❌ FAIL'}")
                print(f"  Source: {validation['source_count']:,} rows")
                print(f"  Target: {validation['target_count']:,} rows")

                # Custom validations
                validations = worker.sync_tables[table_name]['validations']
                custom_results = await worker._run_custom_validations(table_name, validations)

                for validation_name, result in custom_results.items():
                    if result['status'] == 'completed':
                        print(f"  {validation_name}: ✅ COMPLETED")
                    else:
                        print(f"  {validation_name}: ⚠️ {result.get('reason', 'SKIPPED')}")

    except Exception as e:
        print(f"💥 Validation failed: {e}")
        raise


async def main():
    """Main CLI interface"""
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python sync_worker.py sync      # Incremental sync")
        print("  python sync_worker.py full      # Full resync")
        print("  python sync_worker.py validate  # Validate data only")
        return

    command = sys.argv[1]

    if command == "sync":
        await scheduled_sync()
    elif command == "full":
        await full_resync()
    elif command == "validate":
        await validate_existing_data()
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    asyncio.run(main())
```

## Database Setup

### Source Database (PostgreSQL)

```sql
-- Create production database
CREATE DATABASE ecommerce;

-- Create tables
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    status VARCHAR(50) DEFAULT 'pending',
    total_amount DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id),
    product_id INTEGER REFERENCES products(id),
    quantity INTEGER NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes on timestamp columns
CREATE INDEX idx_customers_updated_at ON customers(updated_at);
CREATE INDEX idx_products_updated_at ON products(updated_at);
CREATE INDEX idx_orders_updated_at ON orders(updated_at);
CREATE INDEX idx_order_items_created_at ON order_items(created_at);
```

### Target Database (MySQL)

```sql
-- Create analytics database
CREATE DATABASE analytics;

-- Create corresponding tables
CREATE TABLE customers (
    id INT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    customer_id INT,
    status VARCHAR(50) DEFAULT 'pending',
    total_amount DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_customer_id (customer_id)
);

CREATE TABLE order_items (
    id INT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_order_id (order_id),
    INDEX idx_product_id (product_id)
);
```

## Running the Example

1. **Install dependencies**:
```bash
pip install asyncpg aiomysql
```

2. **Configure connections**:
Update the connection strings in the worker code.

3. **Run sync operations**:
```bash
# Incremental sync
python sync_worker.py sync

# Full resync
python sync_worker.py full

# Validate data only
python sync_worker.py validate
```

## Expected Output

### Incremental Sync
```
🚀 Starting scheduled database sync...
============================================================
DATABASE SYNCHRONIZATION REPORT
============================================================
Sync completed at: 2024-01-15T10:30:00
Total duration: 12.45 seconds
Total tables processed: 4
Successful syncs: 4
Failed syncs: 0
Total rows synced: 15,234

TABLE DETAILS:
----------------------------------------
✅ customers:
   Rows synced: 1,234
   Duration: 2.1s
   Rate: 587 rows/sec
   Validation: ✅ PASSED

✅ orders:
   Rows synced: 5,678
   Duration: 4.2s
   Rate: 1,351 rows/sec
   Validation: ✅ PASSED
```

### Validation Only
```
🔍 Validating existing data integrity...

Validating customers...
  Row count validation: ✅ PASS
  Source: 10,234 rows
  Target: 10,234 rows
  email_format: ✅ COMPLETED
  required_fields: ✅ COMPLETED

Validating orders...
  Row count validation: ✅ PASS
  Source: 45,678 rows
  Target: 45,678 rows
  amount_positive: ✅ COMPLETED
  customer_exists: ✅ COMPLETED
```

## Key Features Demonstrated

1. **Cross-Database Sync**: PostgreSQL to MySQL replication
2. **Incremental Sync**: Only sync changed data using timestamps
3. **Custom Validations**: Business logic validation after sync
4. **Batch Processing**: Configurable batch sizes per table
5. **Error Handling**: Robust error handling with detailed reporting
6. **Data Integrity**: Validation of synced data
7. **Performance Monitoring**: Sync rates and duration tracking
8. **Flexible Configuration**: Per-table configuration options

## Production Considerations

1. **Scheduling**: Use cron or task scheduler for regular syncs
2. **Monitoring**: Set up alerting for failed syncs
3. **Performance**: Tune batch sizes based on data volume
4. **Security**: Use read-only source connections
5. **Backup**: Always backup target before full resync
6. **Network**: Consider connection timeouts and retries
