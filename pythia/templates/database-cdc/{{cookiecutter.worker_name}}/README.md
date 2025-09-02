# {{cookiecutter.worker_name.title().replace('_', ' ')}} - CDC Worker

Change Data Capture worker for {{cookiecutter.database_type.title()}} database.

## Overview

This worker monitors changes in your {{cookiecutter.database_type.title()}} database and processes them in real-time using Pythia's CDC capabilities.

## Configuration

- **Database Type**: {{cookiecutter.database_type.title()}}
- **Connection**: `{{cookiecutter.connection_string}}`
- **Tables**: {{cookiecutter.tables}}
- **Schemas**: {{cookiecutter.schemas}}

## Requirements

{% if cookiecutter.database_type == "postgresql" %}
### PostgreSQL Setup

1. **Enable logical replication** in `postgresql.conf`:
   ```
   wal_level = logical
   max_replication_slots = 10
   max_wal_senders = 10
   ```

2. **Install dependencies**:
   ```bash
   pip install asyncpg
   ```

3. **Database permissions**: Ensure your user has `REPLICATION` privileges:
   ```sql
   ALTER USER your_user REPLICATION;
   ```
{% elif cookiecutter.database_type == "mysql" %}
### MySQL Setup

1. **Enable binary logging** in `my.cnf`:
   ```
   [mysqld]
   server-id = 1
   log-bin = mysql-bin
   binlog-format = ROW
   ```

2. **Install dependencies**:
   ```bash
   pip install aiomysql mysql-replication
   ```

3. **Database permissions**: Ensure your user has `REPLICATION CLIENT` and `REPLICATION SLAVE` privileges:
   ```sql
   GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'your_user'@'%';
   ```
{% endif %}

## Usage

### Basic Usage

```python
from worker import {{cookiecutter.worker_name.title().replace('_', '')}}

# Create and run worker
worker = {{cookiecutter.worker_name.title().replace('_', '')}}()
await worker.run()
```

### Custom Processing

Override the processing methods to implement your business logic:

```python
class My{{cookiecutter.worker_name.title().replace('_', '')}}({{cookiecutter.worker_name.title().replace('_', '')}}):
    async def _process_insert(self, change):
        # Custom INSERT processing
        await self.send_notification(change.new_data)
        return await super()._process_insert(change)

    async def _process_update(self, change):
        # Custom UPDATE processing
        await self.update_cache(change.primary_key, change.new_data)
        return await super()._process_update(change)
```

## Running the Worker

### Development
```bash
python worker.py
```

### Production with Pythia CLI
```bash
pythia run worker.py
```

## Monitoring

The worker provides built-in logging and metrics:

- **Changes processed**: Number of database changes processed
- **Processing latency**: Time taken to process each change
- **Error rates**: Failed processing attempts

## Error Handling

The worker includes automatic error handling:

- **Connection failures**: Automatic reconnection with exponential backoff
- **Processing errors**: Individual change failures don't stop the worker
- **Data validation**: Schema validation for change events

## Configuration Options

Additional configuration can be passed to the worker:

```python
worker = {{cookiecutter.worker_name.title().replace('_', '')}}(
    # Custom slot/publication names for PostgreSQL
    slot_name="custom_slot",
    publication_name="custom_publication",

    # Custom server ID for MySQL
    server_id=2,

    # Additional MySQL settings
    mysql_settings={
        'charset': 'utf8mb4',
        'autocommit': True
    }
)
```
