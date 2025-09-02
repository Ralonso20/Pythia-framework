"""
Tests for database workers using SQLite in-memory database
"""

import pytest
import pytest_asyncio

# Import the database workers
try:
    from pythia.brokers.database import (
        CDCWorker,
        SyncWorker,
        DatabaseChange,
        ChangeType,
    )

    HAS_DATABASE_SUPPORT = True
except ImportError:
    HAS_DATABASE_SUPPORT = False

# Skip all tests if database dependencies are not available
pytestmark = pytest.mark.skipif(
    not HAS_DATABASE_SUPPORT,
    reason="Database dependencies not installed. Install with: pip install pythia[database]",
)


# Test worker implementations
class TestCDCWorkerImpl(CDCWorker):
    """Concrete CDC worker for testing"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_changes = []

    async def process_change(self, change):
        """Process a database change"""
        self.processed_changes.append(change)
        return {"processed": True, "table": change.table}

    async def process(self):
        """Main processing loop - required by base Worker class"""
        await self.start_cdc()
        async for change in self.consume_changes():
            await self.process_change(change)


class TestSyncWorkerImpl(SyncWorker):
    """Concrete Sync worker for testing"""

    async def process(self):
        """Main processing loop - required by base Worker class"""
        # For testing, we don't need a continuous loop
        pass


@pytest.fixture
def sqlite_connection():
    """SQLite file-based connection string for test sharing"""
    return "sqlite+aiosqlite:///test.db"


@pytest_asyncio.fixture
async def setup_test_tables():
    """Create test tables in SQLite"""
    if not HAS_DATABASE_SUPPORT:
        return None

    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy import text

    # Use a file-based SQLite database to share between tests
    engine = create_async_engine("sqlite+aiosqlite:///test.db")

    async with engine.begin() as conn:
        # Drop tables if they exist
        await conn.execute(text("DROP TABLE IF EXISTS users"))
        await conn.execute(text("DROP TABLE IF EXISTS orders"))

        # Create test tables
        await conn.execute(
            text("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        await conn.execute(
            text("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                status TEXT DEFAULT 'pending',
                amount DECIMAL(10,2),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Insert test data
        await conn.execute(
            text("""
            INSERT INTO users (name, email, updated_at) VALUES
            ('John Doe', 'john@example.com', datetime('now', '-1 hour')),
            ('Jane Smith', 'jane@example.com', datetime('now', '-30 minutes'))
        """)
        )

        await conn.execute(
            text("""
            INSERT INTO orders (user_id, amount, updated_at) VALUES
            (1, 99.99, datetime('now', '-45 minutes')),
            (2, 149.99, datetime('now', '-15 minutes'))
        """)
        )

    await engine.dispose()
    return True  # Return True to indicate setup completed


class TestDatabaseWorker:
    """Test base DatabaseWorker functionality"""

    @pytest.mark.asyncio
    async def test_connection_lifecycle(self, sqlite_connection):
        """Test database connection and disconnection"""
        worker = TestCDCWorkerImpl(
            connection_string=sqlite_connection, tables=["users"], poll_interval=1.0
        )

        # Test connection
        await worker.connect()
        assert worker.engine is not None
        assert worker.session_maker is not None

        # Test session creation
        async with worker.get_session() as session:
            assert session is not None

        # Test disconnection
        await worker.disconnect()

    @pytest.mark.asyncio
    async def test_context_manager(self, sqlite_connection):
        """Test async context manager"""
        worker = TestCDCWorkerImpl(
            connection_string=sqlite_connection, tables=["users"]
        )

        async with worker:
            assert worker.engine is not None

            # Should be able to get a session
            async with worker.get_session() as session:
                assert session is not None


class TestCDCWorker:
    """Test CDC Worker functionality"""

    @pytest.mark.asyncio
    async def test_cdc_initialization(self, sqlite_connection, setup_test_tables):
        """Test CDC worker initialization"""
        # Tables are already created by the fixture

        worker = TestCDCWorkerImpl(
            connection_string=sqlite_connection,
            tables=["users", "orders"],
            poll_interval=0.5,
            timestamp_column="updated_at",
        )

        async with worker:
            await worker.start_cdc()

            # Check that timestamps were initialized
            assert "users" in worker._last_check
            assert "orders" in worker._last_check

            await worker.stop_cdc()

    @pytest.mark.asyncio
    async def test_change_detection(self, sqlite_connection, setup_test_tables):
        """Test that CDC detects changes"""
        # Tables are already created by the fixture

        worker = TestCDCWorkerImpl(
            connection_string=sqlite_connection, tables=["users"], poll_interval=0.1
        )

        async with worker:
            await worker.start_cdc()

            # Insert new data
            async with worker.get_session() as session:
                from sqlalchemy import text

                await session.execute(
                    text("""
                    INSERT INTO users (name, email, updated_at)
                    VALUES ('New User', 'new@example.com', datetime('now'))
                """)
                )
                await session.commit()

            # Wait a bit and consume changes
            consumed_changes = []
            async for change in worker.consume_changes():
                consumed_changes.append(change)
                break  # Just get the first change

            assert len(consumed_changes) > 0
            change = consumed_changes[0]
            assert change.table == "users"
            assert change.change_type == ChangeType.UPDATE
            assert change.new_data is not None

            await worker.stop_cdc()

    @pytest.mark.asyncio
    async def test_multiple_table_monitoring(
        self, sqlite_connection, setup_test_tables
    ):
        """Test monitoring multiple tables"""
        # Tables are already created by the fixture

        class MultiTableCDCWorker(TestCDCWorkerImpl):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.changes_by_table = {}

            async def process_change(self, change):
                if change.table not in self.changes_by_table:
                    self.changes_by_table[change.table] = []
                self.changes_by_table[change.table].append(change)
                return {"processed": True}

        worker = MultiTableCDCWorker(
            connection_string=sqlite_connection,
            tables=["users", "orders"],
            poll_interval=0.1,
        )

        async with worker:
            await worker.start_cdc()

            # Insert data in both tables
            async with worker.get_session() as session:
                from sqlalchemy import text

                await session.execute(
                    text("""
                    INSERT INTO users (name, email, updated_at)
                    VALUES ('Another User', 'another@example.com', datetime('now'))
                """)
                )
                await session.execute(
                    text("""
                    INSERT INTO orders (user_id, amount, updated_at)
                    VALUES (1, 299.99, datetime('now'))
                """)
                )
                await session.commit()

            # Collect changes from both tables
            changes_collected = 0
            async for change in worker.consume_changes():
                await worker.process_change(change)
                changes_collected += 1
                if changes_collected >= 2:  # Got changes from both tables
                    break

            await worker.stop_cdc()

            # Should have changes from both tables
            assert len(worker.changes_by_table) >= 1
            # At least one table should have changes
            assert any(len(changes) > 0 for changes in worker.changes_by_table.values())


class TestSyncWorker:
    """Test Sync Worker functionality"""

    @pytest.mark.asyncio
    async def test_sync_worker_initialization(self):
        """Test sync worker initialization"""
        source_conn = "sqlite+aiosqlite:///:memory:"
        target_conn = "sqlite+aiosqlite:///:memory:"

        worker = TestSyncWorkerImpl(
            source_connection=source_conn,
            target_connection=target_conn,
            sync_config={"batch_size": 100, "mode": "full"},
        )

        assert worker.batch_size == 100
        assert worker.sync_mode == "full"
        assert worker.timestamp_column == "updated_at"

    @pytest.mark.asyncio
    async def test_dual_connection(self):
        """Test connecting to both source and target databases"""
        source_conn = "sqlite+aiosqlite:///:memory:"
        target_conn = "sqlite+aiosqlite:///:memory:"

        worker = TestSyncWorkerImpl(
            source_connection=source_conn, target_connection=target_conn
        )

        async with worker:
            assert worker.engine is not None  # Source connection
            assert worker.target_engine is not None  # Target connection

            # Test both session types
            async with worker.get_session() as source_session:
                assert source_session is not None

            async with worker.get_target_session() as target_session:
                assert target_session is not None

    @pytest.mark.asyncio
    async def test_table_sync_basic(self, setup_test_tables):
        """Test basic table synchronization"""
        # Use the shared test database for both source and target
        source_conn = "sqlite+aiosqlite:///test.db"
        target_conn = "sqlite+aiosqlite:///test_target.db"

        # Setup target database with same structure
        from sqlalchemy.ext.asyncio import create_async_engine
        from sqlalchemy import text

        target_engine = create_async_engine(target_conn)
        async with target_engine.begin() as conn:
            await conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            )
        await target_engine.dispose()

        # Test sync using existing users table
        worker = TestSyncWorkerImpl(
            source_connection=source_conn,
            target_connection=target_conn,
            sync_config={"mode": "full", "batch_size": 2},
        )

        async with worker:
            result = await worker.sync_table("users")

            assert result["table"] == "users"
            assert result["sync_mode"] == "full"
            assert result["rows_synced"] == 2  # We have 2 users in test data
            assert result["source_count"] == 2

            # Validate sync
            validation = await worker.validate_sync("users")
            assert validation["is_valid"] is True
            assert validation["source_count"] == 2
            assert validation["target_count"] == 2


class TestDatabaseChange:
    """Test DatabaseChange data class"""

    def test_database_change_creation(self):
        """Test creating DatabaseChange objects"""
        change = DatabaseChange(
            table="users",
            change_type=ChangeType.INSERT,
            primary_key={"id": 1},
            new_data={"name": "John", "email": "john@example.com"},
            timestamp="2024-01-01 12:00:00",
        )

        assert change.table == "users"
        assert change.change_type == ChangeType.INSERT
        assert change.primary_key["id"] == 1
        assert change.new_data["name"] == "John"
        assert change.timestamp == "2024-01-01 12:00:00"

    def test_change_type_enum(self):
        """Test ChangeType enum values"""
        assert ChangeType.INSERT.value == "INSERT"
        assert ChangeType.UPDATE.value == "UPDATE"
        assert ChangeType.DELETE.value == "DELETE"
        assert ChangeType.TRUNCATE.value == "TRUNCATE"


@pytest.mark.asyncio
async def test_error_handling():
    """Test error handling for missing dependencies"""

    # Test with invalid connection string
    worker = TestCDCWorkerImpl(
        connection_string="invalid://connection", tables=["test"]
    )

    with pytest.raises(Exception):  # Should raise some SQLAlchemy error
        async with worker:
            await worker.start_cdc()


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
