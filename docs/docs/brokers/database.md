# Database Workers

Database workers in Pythia provide simple yet powerful capabilities for monitoring database changes and synchronizing data using SQLAlchemy.

## Overview

Pythia supports two main types of database workers:

1. **CDC (Change Data Capture) Workers** - Monitor database changes using polling
2. **Sync Workers** - Synchronize data between databases

All database workers use SQLAlchemy for database abstraction, supporting PostgreSQL, MySQL, SQLite, and other SQLAlchemy-compatible databases.

## Change Data Capture (CDC) Workers

CDC workers monitor database changes by polling tables for new or updated records based on timestamp columns.

### Basic CDC Worker

```python
from pythia.brokers.database import CDCWorker, DatabaseChange

class OrderProcessor(CDCWorker):
    def __init__(self):
        super().__init__(
            connection_string="postgresql://user:pass@localhost:5432/ecommerce",
            tables=["orders", "order_items"],
            poll_interval=5.0,  # Check every 5 seconds
            timestamp_column="updated_at"
        )

    async def process_change(self, change: DatabaseChange):
        if change.table == "orders":
            # Process order changes
            await self.send_order_confirmation(change.new_data)
            await self.update_inventory(change.new_data)

        return {"processed": True, "order_id": change.primary_key}

# Usage
worker = OrderProcessor()
async with worker:
    await worker.start_cdc()

    # Process changes as they come
    async for change in worker.consume_changes():
        result = await worker.process_change(change)
        print(f"Processed: {result}")
```

### Multi-Database CDC Worker

```python
# Works with any SQLAlchemy-supported database
class UserActivityProcessor(CDCWorker):
    def __init__(self):
        super().__init__(
            connection_string="mysql://user:pass@localhost:3306/analytics",
            tables=["user_events", "user_sessions"],
            poll_interval=2.0,
            timestamp_column="created_at"
        )

    async def process_change(self, change: DatabaseChange):
        # Real-time analytics processing
        await self.update_user_metrics(change.new_data)
        return {"event_processed": True}
```

## Database Sync Workers

Sync workers synchronize data between different database systems.

### Basic Sync Worker

```python
from pythia.brokers.database import SyncWorker

class DataReplicationWorker(SyncWorker):
    def __init__(self):
        sync_config = {
            'batch_size': 1000,
            'mode': 'incremental',  # or 'full'
            'conflict_resolution': 'source_wins',
            'timestamp_column': 'updated_at'
        }

        super().__init__(
            source_connection="postgresql://user:pass@source:5432/prod_db",
            target_connection="mysql://user:pass@target:3306/analytics_db",
            sync_config=sync_config
        )

# Sync specific tables
worker = DataReplicationWorker()
async with worker:
    result = await worker.sync_table("users")
    print(f"Synced {result['rows_synced']} rows")

    # Validate sync
    validation = await worker.validate_sync("users")
    if validation['is_valid']:
        print("✓ Sync validated successfully")
```

### Cross-Database Sync

```python
# PostgreSQL → MySQL sync
async def postgres_to_mysql_sync():
    worker = SyncWorker(
        source_connection="postgresql://user:pass@pg-host:5432/source",
        target_connection="mysql://user:pass@mysql-host:3306/target"
    )

    async with worker:
        # Sync specific tables
        result = await worker.sync_table("users")
        print(f"Synced {result['rows_synced']} rows")

# MySQL → PostgreSQL sync
async def mysql_to_postgres_sync():
    worker = SyncWorker(
        source_connection="mysql://user:pass@mysql-host:3306/source",
        target_connection="postgresql://user:pass@pg-host:5432/target"
    )

    async with worker:
        # Sync and validate
        result = await worker.sync_table("products")
        validation = await worker.validate_sync("products")
        print(f"Sync valid: {validation['is_valid']}")
```

## Requirements

### Python Dependencies

```bash
# Core requirement
pip install sqlalchemy[asyncio]

# Database-specific drivers (choose what you need)
pip install asyncpg          # PostgreSQL
pip install aiomysql         # MySQL
pip install aiosqlite        # SQLite
```

### Database Setup

All database workers require:

1. **Timestamp columns** on tables you want to monitor:
   ```sql
   -- PostgreSQL
   ALTER TABLE your_table ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

   -- MySQL
   ALTER TABLE your_table ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
   ```

2. **Read permissions** for CDC workers:
   ```sql
   GRANT SELECT ON database.* TO 'cdc_user'@'%';
   ```

3. **Write permissions** for sync workers:
   ```sql
   GRANT SELECT, INSERT, UPDATE, DELETE ON target_database.* TO 'sync_user'@'%';
   ```

## Configuration Options

### CDC Configuration

```python
# Basic CDC configuration
worker = CDCWorker(
    connection_string="postgresql://...",  # Any SQLAlchemy URL
    tables=["table1", "table2"],           # Tables to monitor
    poll_interval=5.0,                     # Check every 5 seconds
    timestamp_column="updated_at"          # Column to track changes
)
```

### Sync Configuration

```python
sync_config = {
    'batch_size': 1000,                    # Rows per batch
    'mode': 'incremental',                 # 'full' or 'incremental'
    'conflict_resolution': 'source_wins',  # How to handle conflicts
    'timestamp_column': 'updated_at',      # Column for incremental sync
    'truncate_target': False               # Truncate before full sync
}
```

## Error Handling and Monitoring

### Built-in Error Handling

```python
class RobustCDCWorker(PostgreSQLCDCWorker):
    async def process_change(self, change: DatabaseChange):
        try:
            # Your processing logic
            result = await self.process_business_logic(change)
            return result
        except Exception as e:
            # Log error but don't stop worker
            self.logger.error(f"Error processing change: {e}")
            # Return error info for monitoring
            return {"error": str(e), "change_id": change.primary_key}
```

### Monitoring and Metrics

```python
# CDC monitoring
async with worker:
    await worker.start_cdc()

    change_count = 0
    error_count = 0

    async for change in worker.consume_changes():
        try:
            result = await worker.process_change(change)
            change_count += 1

            if change_count % 1000 == 0:
                self.logger.info(f"Processed {change_count} changes")

        except Exception as e:
            error_count += 1
            self.logger.error(f"Processing error: {e}")
```

## Use Cases

### Real-time Analytics
```python
class AnalyticsCDC(PostgreSQLCDCWorker):
    async def process_change(self, change: DatabaseChange):
        if change.table == "user_events":
            await self.update_real_time_dashboard(change.new_data)
            await self.trigger_alerts_if_needed(change.new_data)
```

### Data Pipeline Triggers
```python
class PipelineTrigger(MySQLCDCWorker):
    async def process_change(self, change: DatabaseChange):
        if change.table == "raw_data" and change.change_type.value == "INSERT":
            await self.trigger_etl_pipeline(change.new_data)
```

### Cross-System Synchronization
```python
class ECommerceCDC(PostgreSQLCDCWorker):
    async def process_change(self, change: DatabaseChange):
        if change.table == "products" and change.change_type.value == "UPDATE":
            # Update search index
            await self.update_elasticsearch(change.new_data)
            # Update cache
            await self.invalidate_cache(change.primary_key)
            # Sync to warehouse
            await self.sync_to_warehouse(change.new_data)
```

## Best Practices

1. **Performance**:
   - Use appropriate batch sizes for sync operations
   - Filter CDC events by relevant tables/schemas only
   - Monitor replication lag in production

2. **Reliability**:
   - Implement proper error handling
   - Use idempotent processing logic
   - Monitor worker health and restart if needed

3. **Security**:
   - Use dedicated database users with minimal privileges
   - Encrypt connections in production
   - Store credentials securely

4. **Operations**:
   - Monitor replication slot usage (PostgreSQL)
   - Monitor binary log disk usage (MySQL)
   - Set up alerting for worker failures

## Creating Database Workers

Use Pythia's templates to quickly create database workers:

```bash
# Create CDC worker
pythia create database-cdc my_cdc_worker --database-type postgresql

# Create sync worker
pythia create database-sync my_sync_worker
```

This will generate a complete worker with configuration examples and best practices built-in.
