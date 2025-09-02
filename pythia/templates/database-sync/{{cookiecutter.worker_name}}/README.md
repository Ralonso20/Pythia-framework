# {{cookiecutter.worker_name.title().replace('_', ' ')}} - Database Sync Worker

Database synchronization worker for cross-database data replication.

## Overview

This worker synchronizes data between different database systems using Pythia's database sync capabilities.

## Configuration

- **Source**: `{{cookiecutter.source_connection}}`
- **Target**: `{{cookiecutter.target_connection}}`
- **Sync Mode**: {{cookiecutter.sync_mode.title()}}
- **Batch Size**: {{cookiecutter.batch_size}} rows

## Supported Database Combinations

- PostgreSQL → PostgreSQL
- MySQL → MySQL
- PostgreSQL → MySQL
- MySQL → PostgreSQL

## Requirements

### Dependencies

```bash
# For PostgreSQL connections
pip install asyncpg

# For MySQL connections
pip install aiomysql
```

### Database Permissions

**Source Database**:
- `SELECT` privileges on all tables to sync
- Access to metadata tables (`information_schema`, `pg_tables`, etc.)

**Target Database**:
- `INSERT`, `UPDATE`, `DELETE` privileges on target tables
- `CREATE TABLE` if tables don't exist (optional)

## Usage

### Sync Specific Tables

```python
from worker import {{cookiecutter.worker_name.title().replace('_', '')}}

worker = {{cookiecutter.worker_name.title().replace('_', '')}}()
result = await worker.sync_tables(['users', 'orders', 'products'])
```

### Sync Entire Database

```python
worker = {{cookiecutter.worker_name.title().replace('_', '')}}()
result = await worker.sync_all_tables()
```

### Sync Single Table

```python
worker = {{cookiecutter.worker_name.title().replace('_', '')}}()
result = await worker.sync_table('users')
```

## Running the Worker

### Command Line Usage

```bash
# Sync specific tables (edit worker.py to configure tables)
python worker.py tables

# Sync entire database
python worker.py all

# Sync single table
python worker.py users
```

### Programmatic Usage

```python
import asyncio
from worker import {{cookiecutter.worker_name.title().replace('_', '')}}

async def main():
    worker = {{cookiecutter.worker_name.title().replace('_', '')}}()

    async with worker:
        # Sync and validate
        result = await worker.sync_table('users')
        validation = await worker.validate_sync('users')

        if validation['is_valid']:
            print("Sync successful and validated")
        else:
            print(f"Sync validation failed: {validation}")

asyncio.run(main())
```

## Sync Modes

### Full Sync
- Synchronizes all data from source to target
- Option to truncate target table first
- Best for initial sync or complete refresh

### Incremental Sync
- Only syncs changed data since last sync
- Requires timestamp column (configurable)
- More efficient for ongoing synchronization

## Configuration Options

```python
sync_config = {
    'batch_size': 1000,           # Rows per batch
    'mode': 'incremental',        # 'full' or 'incremental'
    'conflict_resolution': 'source_wins',  # How to handle conflicts
    'timestamp_column': 'updated_at',      # Column for incremental sync
    'truncate_target': False      # Truncate before full sync
}
```

## Monitoring and Validation

### Sync Results

```python
result = await worker.sync_table('users')
print(f"Synced {result['rows_synced']} rows in {result['duration_seconds']}s")
```

### Validation

```python
validation = await worker.validate_sync('users')
if validation['is_valid']:
    print(f"✓ Table valid: {validation['source_count']} rows")
else:
    print(f"✗ Mismatch: source={validation['source_count']}, target={validation['target_count']}")
```

## Error Handling

The worker includes comprehensive error handling:

- **Connection failures**: Automatic retry with exponential backoff
- **Data type mismatches**: Logs errors and continues with next batch
- **Constraint violations**: Configurable conflict resolution
- **Partial failures**: Individual batch failures don't stop entire sync

## Best Practices

### Performance
- Use appropriate batch sizes (1000-10000 depending on row size)
- Run during low-traffic periods for large syncs
- Consider partitioning large tables

### Data Integrity
- Always validate syncs after completion
- Use incremental sync for ongoing replication
- Monitor sync logs for errors or warnings

### Security
- Use dedicated sync users with minimal required privileges
- Encrypt connections in production
- Store credentials securely (environment variables, secrets manager)

## Scheduling

### Cron Example
```bash
# Daily full sync at 2 AM
0 2 * * * cd /path/to/worker && python worker.py all

# Hourly incremental sync
0 * * * * cd /path/to/worker && python worker.py tables
```

### Systemd Service
```ini
[Unit]
Description={{cookiecutter.worker_name.title().replace('_', ' ')}} Database Sync
After=network.target

[Service]
Type=oneshot
User=sync-user
WorkingDirectory=/path/to/worker
ExecStart=/usr/bin/python worker.py all
```
