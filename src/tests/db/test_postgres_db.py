import datetime
import threading
import time
from unittest.mock import MagicMock
import pytest
from testcontainers.postgres import PostgresContainer
from db.postgres_db import PostgresDB

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
    expected_repr = f"PostgresDB(dbname={db_config['dbname']}, user={db_config['user']}, host={db_config['host']}, port={db_config['port']})"
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
    for t in threads: t.start()
    for t in threads: t.join()

    assert len(connections) == 5
    assert len(set(map(id, connections))) == 5

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
    for t in threads: t.start()
    for t in threads: t.join()
    end = datetime.datetime.now()

    # Semaphore should enforce max_conn: total time >= 2 * 0.5 sec
    assert (end - start).total_seconds() >= 1
