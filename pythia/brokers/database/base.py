"""
Base classes for database workers using SQLAlchemy
"""

import asyncio
from abc import abstractmethod
from typing import Any, Dict, List, Optional, AsyncIterator
from dataclasses import dataclass
from enum import Enum

from ...core.worker import Worker
from ...core.message import Message

try:
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession  # type: ignore
    from sqlalchemy.orm import sessionmaker  # type: ignore
    from sqlalchemy import text  # type: ignore

    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False


class ChangeType(Enum):
    """Types of database changes"""

    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    TRUNCATE = "TRUNCATE"


@dataclass
class DatabaseChange:
    """Represents a database change event"""

    table: str
    change_type: ChangeType
    primary_key: Dict[str, Any]
    old_data: Optional[Dict[str, Any]] = None
    new_data: Optional[Dict[str, Any]] = None
    timestamp: Optional[str] = None
    transaction_id: Optional[str] = None
    schema: Optional[str] = None


class DatabaseWorker(Worker):
    """Base class for database workers using SQLAlchemy"""

    def __init__(self, connection_string: str, **kwargs):
        if not HAS_SQLALCHEMY:
            raise ImportError(
                "SQLAlchemy is required for database workers. "
                "Install with: pip install sqlalchemy[asyncio]"
            )

        super().__init__(**kwargs)
        self.connection_string = connection_string
        self.engine = None
        self.session_maker = None

    async def connect(self) -> None:
        """Connect to the database using SQLAlchemy"""
        self.engine = create_async_engine(self.connection_string)
        self.session_maker = sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
        self.logger.info("Connected to database via SQLAlchemy")

    async def disconnect(self) -> None:
        """Disconnect from the database"""
        if self.engine:
            await self.engine.dispose()
        self.logger.info("Disconnected from database")

    def get_session(self):  # type: ignore
        """Get a database session"""
        if not self.session_maker:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self.session_maker()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
        return None


class CDCWorker(DatabaseWorker):
    """Base class for Change Data Capture workers"""

    def __init__(
        self,
        connection_string: str,
        tables: Optional[List[str]] = None,
        poll_interval: float = 1.0,
        timestamp_column: str = "updated_at",
        **kwargs,
    ):
        super().__init__(connection_string, **kwargs)
        self.tables = tables or []
        self.poll_interval = poll_interval
        self.timestamp_column = timestamp_column
        self._running = False
        self._last_check = {}  # Track last timestamp per table

    async def start_cdc(self) -> None:
        """Start CDC monitoring using polling"""
        self._running = True
        await self._initialize_timestamps()
        self.logger.info("CDC monitoring started (polling mode)")

    async def stop_cdc(self) -> None:
        """Stop CDC monitoring"""
        self._running = False
        self.logger.info("CDC monitoring stopped")

    async def _initialize_timestamps(self) -> None:
        """Initialize last check timestamps for each table"""
        async with self.get_session() as session:
            for table in self.tables:
                try:
                    # Get the latest timestamp from each table
                    query = text(f"SELECT MAX({self.timestamp_column}) FROM {table}")
                    result = await session.execute(query)
                    max_timestamp = result.scalar()
                    self._last_check[table] = max_timestamp
                    self.logger.debug(f"Initialized {table} timestamp: {max_timestamp}")
                except Exception as e:
                    self.logger.error(f"Error initializing timestamp for {table}: {e}")
                    self._last_check[table] = None

    async def consume_changes(self) -> AsyncIterator[DatabaseChange]:
        """Consume database changes using polling"""
        while self._running:
            try:
                changes = await self._poll_for_changes()
                for change in changes:
                    yield change

                await asyncio.sleep(self.poll_interval)

            except Exception as e:
                self.logger.error(f"Error polling for changes: {e}")
                await asyncio.sleep(self.poll_interval * 2)  # Back off on error

    async def _poll_for_changes(self) -> List[DatabaseChange]:
        """Poll tables for changes since last check"""
        changes = []

        async with self.get_session() as session:
            for table in self.tables:
                try:
                    last_timestamp = self._last_check.get(table)

                    # Query for new/updated records
                    if last_timestamp:
                        query = text(f"""
                            SELECT * FROM {table}
                            WHERE {self.timestamp_column} > :last_timestamp
                            ORDER BY {self.timestamp_column}
                        """)
                        result = await session.execute(query, {"last_timestamp": last_timestamp})
                    else:
                        # First run - get recent records
                        query = text(f"""
                            SELECT * FROM {table}
                            ORDER BY {self.timestamp_column} DESC
                            LIMIT 100
                        """)
                        result = await session.execute(query)

                    rows = result.fetchall()
                    for row in rows:
                        row_dict = dict(row._mapping)
                        change = DatabaseChange(
                            table=table,
                            change_type=ChangeType.UPDATE,  # Simplified - we detect updates only
                            primary_key={"id": row_dict.get("id")},
                            new_data=row_dict,
                            timestamp=str(row_dict.get(self.timestamp_column)),
                        )
                        changes.append(change)

                        # Update last check timestamp
                        self._last_check[table] = row_dict.get(self.timestamp_column)

                except Exception as e:
                    self.logger.error(f"Error polling table {table}: {e}")

        return changes

    async def process_message(self, message: Message) -> Any:
        """Process database change message"""
        try:
            # Ensure message.body is a dict with required fields
            if not isinstance(message.body, dict):
                raise ValueError("Message body must be a dictionary")

            # Create DatabaseChange with explicit parameters
            change = DatabaseChange(
                table=message.body.get("table", ""),
                change_type=ChangeType(message.body.get("change_type", "UPDATE")),
                primary_key=message.body.get("primary_key", {}),
                old_data=message.body.get("old_data"),
                new_data=message.body.get("new_data"),
                timestamp=message.body.get("timestamp"),
                transaction_id=message.body.get("transaction_id"),
                schema=message.body.get("schema"),
            )
            return await self.process_change(change)
        except Exception as e:
            self.logger.error(f"Error processing change: {e}")
            raise

    @abstractmethod
    async def process_change(self, change: DatabaseChange) -> Any:
        """Process a database change - to be implemented by user"""
        pass


class SyncWorker(DatabaseWorker):
    """Base class for database synchronization workers using SQLAlchemy"""

    def __init__(
        self,
        source_connection: str,
        target_connection: str,
        sync_config: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        super().__init__(source_connection, **kwargs)
        self.target_connection = target_connection
        self.sync_config = sync_config or {}

        # Sync configuration defaults
        self.batch_size = self.sync_config.get("batch_size", 1000)
        self.sync_mode = self.sync_config.get("mode", "incremental")  # 'full' or 'incremental'
        self.timestamp_column = self.sync_config.get("timestamp_column", "updated_at")

        # Connection objects
        self.target_engine = None
        self.target_session_maker = None

    async def connect(self) -> None:
        """Connect to both source and target databases"""
        # Connect to source (via parent class)
        await super().connect()

        # Connect to target
        self.target_engine = create_async_engine(self.target_connection)
        self.target_session_maker = sessionmaker(
            self.target_engine, class_=AsyncSession, expire_on_commit=False
        )
        self.logger.info("Connected to both source and target databases")

    async def disconnect(self) -> None:
        """Disconnect from both databases"""
        await super().disconnect()
        if self.target_engine:
            await self.target_engine.dispose()
        self.logger.info("Disconnected from both databases")

    def get_target_session(self):  # type: ignore
        """Get a target database session"""
        if not self.target_session_maker:
            raise RuntimeError("Target database not connected. Call connect() first.")
        return self.target_session_maker()

    async def sync_table(self, table_name: str) -> Dict[str, Any]:
        """Sync a specific table"""
        from datetime import datetime

        start_time = datetime.now()

        try:
            if self.sync_mode == "full":
                result = await self._full_table_sync(table_name)
            else:
                result = await self._incremental_table_sync(table_name)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            return {
                "table": table_name,
                "sync_mode": self.sync_mode,
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                **result,
            }

        except Exception as e:
            self.logger.error(f"Error syncing table {table_name}: {e}")
            raise

    async def _full_table_sync(self, table_name: str) -> Dict[str, Any]:
        """Perform full table synchronization"""
        rows_synced = 0

        async with self.get_session() as source_session:
            async with self.get_target_session() as target_session:
                # Get total count
                count_query = text(f"SELECT COUNT(*) FROM {table_name}")
                source_count = (await source_session.execute(count_query)).scalar()

                # Optionally truncate target
                if self.sync_config.get("truncate_target", False):
                    await target_session.execute(text(f"TRUNCATE TABLE {table_name}"))
                    await target_session.commit()

                # Sync in batches
                offset = 0
                while offset < source_count:
                    # Get batch from source
                    batch_query = text(f"""
                        SELECT * FROM {table_name}
                        LIMIT :limit OFFSET :offset
                    """)
                    result = await source_session.execute(
                        batch_query, {"limit": self.batch_size, "offset": offset}
                    )
                    rows = result.fetchall()

                    if not rows:
                        break

                    # Insert batch into target
                    await self._upsert_batch(target_session, table_name, rows)
                    rows_synced += len(rows)
                    offset += self.batch_size

                    self.logger.debug(f"Synced batch: {offset}/{source_count} rows")

        return {
            "rows_synced": rows_synced,
            "source_count": source_count,
            "sync_type": "full",
        }

    async def _incremental_table_sync(self, table_name: str) -> Dict[str, Any]:
        """Perform incremental table synchronization"""
        # This is a simplified version - in production you'd store last sync timestamps
        rows_synced = 0

        async with self.get_session() as source_session:
            async with self.get_target_session() as target_session:
                # Get recent changes (last hour as example)
                query = text(f"""
                    SELECT * FROM {table_name}
                    WHERE {self.timestamp_column} >= NOW() - INTERVAL '1 hour'
                    ORDER BY {self.timestamp_column}
                """)
                result = await source_session.execute(query)
                rows = result.fetchall()

                if rows:
                    await self._upsert_batch(target_session, table_name, rows)
                    rows_synced = len(rows)

        return {"rows_synced": rows_synced, "sync_type": "incremental"}

    async def _upsert_batch(
        self,
        session,
        table_name: str,
        rows: List,  # type: ignore
    ) -> None:
        """Insert/update a batch of rows to target table"""
        if not rows:
            return

        # Simple approach: delete and insert (can be optimized with proper UPSERT)
        for row in rows:
            row_dict = dict(row._mapping)

            # Delete existing row if it exists
            if "id" in row_dict:
                delete_query = text(f"DELETE FROM {table_name} WHERE id = :id")
                await session.execute(delete_query, {"id": row_dict["id"]})

            # Insert new row
            columns = list(row_dict.keys())
            placeholders = ", ".join([f":{col}" for col in columns])
            insert_query = text(f"""
                INSERT INTO {table_name} ({", ".join(columns)})
                VALUES ({placeholders})
            """)
            await session.execute(insert_query, row_dict)

        await session.commit()

    async def validate_sync(self, table_name: str) -> Dict[str, Any]:
        """Validate sync by comparing row counts"""
        try:
            async with self.get_session() as source_session:
                async with self.get_target_session() as target_session:
                    # Get counts from both databases
                    count_query = text(f"SELECT COUNT(*) FROM {table_name}")

                    source_count = (await source_session.execute(count_query)).scalar()
                    target_count = (await target_session.execute(count_query)).scalar()

                    is_valid = source_count == target_count

                    return {
                        "table": table_name,
                        "source_count": source_count,
                        "target_count": target_count,
                        "is_valid": is_valid,
                        "difference": abs(source_count - target_count),
                    }

        except Exception as e:
            self.logger.error(f"Error validating table {table_name}: {e}")
            return {"table": table_name, "error": str(e), "is_valid": False}


# Note: Some type checking errors are expected when SQLAlchemy is not installed
# Install SQLAlchemy with: pip install sqlalchemy[asyncio]
