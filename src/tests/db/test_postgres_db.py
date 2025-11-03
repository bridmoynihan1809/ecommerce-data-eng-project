import datetime
import threading
import time
from unittest.mock import MagicMock, patch
import pytest
from testcontainers.postgres import PostgresContainer
from db.postgres_db import PostgresDB
from db.db_context_manager import ManagedConnection

postgres = PostgresContainer("postgres:16-alpine")


# Fixtures


@pytest.fixture(scope="module", autouse=True)
def postgres_container(request):
    container = PostgresContainer("postgres:16-alpine")
    container.start()

    def cleanup():
        container.stop()

    request.addfinalizer(cleanup)
    return container


@pytest.fixture
def db_config(postgres_container):
    return {
        "dbname": postgres_container.dbname,
        "user": postgres_container.username,
        "password": postgres_container.password,
        "host": postgres_container.get_container_host_ip(),
        "port": int(postgres_container.get_exposed_port(5432)),
    }


@pytest.fixture
def logger():
    return MagicMock()


@pytest.fixture
def reset_singleton():
    PostgresDB._instance = None


@pytest.fixture
def db_instance(db_config, logger, reset_singleton):
    """Return a fresh PostgresDB instance."""
    return PostgresDB(
        db_config["dbname"],
        db_config["user"],
        db_config["password"],
        db_config["host"],
        db_config["port"],
        min_conn=1,
        max_conn=5,
        logger=logger,
    )

# Tests


def test_singleton(db_instance, db_config, logger):
    """Verify that PostgresDB implements singleton correctly."""
    db2 = PostgresDB(
        db_config["dbname"], db_config["user"], db_config["password"],
        db_config["host"], db_config["port"], 1, 5, logger
    )

    assert db_instance is db2  # Singleton identity
    expected_repr = (
        f"PostgresDB(dbname={db_config['dbname']}, user={db_config['user']}, "
        f"host={db_config['host']}, port={db_config['port']})"
    )
    assert repr(db_instance) == expected_repr


def test_get_and_release_connection_threaded(db_instance):
    """Test threaded pool logic with multiple threads."""
    connections = []
    delay = 0.5

    def worker():
        conn = db_instance.get_connection()
        connections.append(conn)
        time.sleep(delay)
        db_instance.release_connection(conn)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(connections) == 5
    assert len(set(map(id, connections))) == 5  # max_conn

    conn = db_instance.get_connection()
    assert conn is not None
    db_instance.release_connection(conn)


def test_semaphore_enforces_max_connections(db_config, logger, reset_singleton):
    """Test that semaphore prevents more than max_conn simultaneous connections."""
    max_conn = 3
    delay = 0.5
    db = PostgresDB(
        db_config["dbname"], db_config["user"], db_config["password"],
        db_config["host"], db_config["port"], 1, max_conn, logger
    )

    active_conns = []

    def worker():
        conn = db.get_connection()
        active_conns.append(conn)
        time.sleep(delay)
        db.release_connection(conn)

    # Set the number of threads to double the max_conn size
    threads = [threading.Thread(target=worker) for _ in range(max_conn * 2)]

    start = datetime.datetime.now()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    end = datetime.datetime.now()

    # Semaphore should enforce max_conn: total time >= 2 * 0.5 sec
    assert (end - start).total_seconds() >= 1


def test_get_instance_before_initialization(reset_singleton):
    """Test that get_instance() raises error when not initialized."""
    with pytest.raises(ValueError, match="PostgresDB not initialized"):
        PostgresDB.get_instance()


def test_get_instance_after_initialization(db_instance):
    """Test that get_instance() returns the singleton after initialization."""
    instance = PostgresDB.get_instance()
    assert instance is db_instance


def test_get_connection_when_pool_closed(db_instance):
    """Test that get_connection() raises error when pool is closed."""
    db_instance.close_pool()

    with pytest.raises(ConnectionError, match="Pool not initialised"):
        db_instance.get_connection()


def test_close_pool(db_instance):
    """Test that close_pool() properly closes the pool and resets state."""
    # Get a connection first to verify pool is working
    conn = db_instance.get_connection()
    db_instance.release_connection(conn)

    # Close the pool
    db_instance.close_pool()

    # Verify pool is closed
    assert db_instance._pool is None
    assert db_instance._initialized is False

    # Verify can't get connections after closing
    with pytest.raises(ConnectionError):
        db_instance.get_connection()


def test_get_connection_exception_releases_semaphore(db_instance):
    """Test that semaphore is released if get_connection() fails."""
    # Mock the pool to raise an exception
    with patch.object(db_instance._pool, 'getconn', side_effect=Exception("Connection failed")):
        with pytest.raises(Exception, match="Connection failed"):
            db_instance.get_connection()

    # Verify semaphore was released by checking we can still acquire connections
    # If semaphore wasn't released, this would eventually block
    conn = db_instance.get_connection()
    assert conn is not None
    db_instance.release_connection(conn)


def test_release_connection_always_releases_semaphore(db_instance):
    """Test that semaphore is released even if putconn() fails."""
    conn = db_instance.get_connection()

    # Mock putconn to raise an exception
    with patch.object(db_instance._pool, 'putconn', side_effect=Exception("Release failed")):
        with pytest.raises(Exception, match="Release failed"):
            db_instance.release_connection(conn)

    # Verify semaphore was still released - we should be able to get another connection
    conn2 = db_instance.get_connection()
    assert conn2 is not None
    db_instance.release_connection(conn2)


def test_managed_connection_context_manager(db_instance):
    """Test that ManagedConnection properly manages connections."""
    with ManagedConnection(db_instance) as conn:
        assert conn is not None
        # Connection should be usable
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1
        cursor.close()

    # After context exits, connection should be released
    # Verify by getting max_conn connections
    conns = []
    for _ in range(5):  # max_conn is 5
        with ManagedConnection(db_instance) as conn:
            conns.append(id(conn))

    # All connections should work
    assert len(conns) == 5


def test_managed_connection_releases_on_exception(db_instance):
    """Test that ManagedConnection releases connection even when exception occurs."""
    initial_semaphore_value = db_instance._semaphore._value

    try:
        with ManagedConnection(db_instance) as conn:
            assert conn is not None
            raise ValueError("Test exception")
    except ValueError:
        pass

    # Semaphore should be back to initial value
    assert db_instance._semaphore._value == initial_semaphore_value


def test_concurrent_close_pool(db_instance):
    """Test thread safety of close_pool()."""
    results = []

    def worker():
        try:
            db_instance.close_pool()
            results.append("success")
        except Exception as e:
            results.append(f"error: {e}")

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Should handle concurrent closes gracefully
    assert len(results) == 5
    assert db_instance._pool is None


def test_connection_pool_reuse(db_instance):
    """Test that connections are properly reused from the pool."""
    conn1 = db_instance.get_connection()
    conn1_id = id(conn1)
    db_instance.release_connection(conn1)

    conn2 = db_instance.get_connection()
    conn2_id = id(conn2)
    db_instance.release_connection(conn2)

    # Connection should be reused from the pool
    assert conn1_id == conn2_id


def test_multiple_threads_with_context_manager(db_instance):
    """Test ManagedConnection with multiple threads."""
    results = []

    def worker():
        try:
            with ManagedConnection(db_instance) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                results.append(result[0])
        except Exception as e:
            results.append(f"error: {e}")

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # All threads should succeed
    assert len(results) == 10
    assert all(r == 1 for r in results)
