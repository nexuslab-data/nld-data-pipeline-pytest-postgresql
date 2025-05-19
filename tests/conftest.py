from typing import Generator, Any

import pytest
from testcontainers.postgres import PostgresContainer

from nexuslabdata.logging import EventLevel, LoggerManager
from nexuslabdata.connection.postgresql import PostgreSQLCredential, PostgreSQLConnectionWrapper
from tests.test_utils.flyway_container import FlyWayContainer
from tests.test_utils.postgresql_adapters import register_postgresql_adapters

register_postgresql_adapters()


@pytest.fixture(scope="session", autouse=True)
def init_logger_for_tests(request: Any) -> None:
    LoggerManager(EventLevel.TEST)


@pytest.fixture(scope="session", autouse=True)
def postgresql(request: Any) -> Generator[PostgreSQLConnectionWrapper, Any, Any]:
    postgres_port = 5432
    postgres = PostgresContainer(
        "postgres:15",
        port=postgres_port,
        user="default_user",
        password="default_password",
        dbname="default_db",
    )

    postgres.start()

    try:
        # We need to find the IP address of the postgres container within the docker network as
        # flyway is also a container.
        postgres_host = postgres.get_docker_client().bridge_ip(
            container_id=postgres.get_wrapped_container().id
        )
        flyway_container = FlyWayContainer(
            container_host_ip=postgres_host, exposed_port=postgres_port
        )
        flyway_container.run_migrate_command()
        flyway_container.clean_flyway_containers()

        credentials = PostgreSQLCredential(
            name="test",
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(5432),
            database="default_db",
            user="default_user",
            password="default_password",
        )

        connection_wrapper = PostgreSQLConnectionWrapper(name="default_db_conn", credentials=credentials)
        connection_wrapper.open()

        yield connection_wrapper

    except Exception as e:
        if postgres.get_wrapped_container() is not None:
            postgres.stop()
        raise e

    postgres.stop()
