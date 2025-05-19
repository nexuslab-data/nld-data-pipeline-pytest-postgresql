import os

import docker  # type: ignore[import-untyped]

from tests.configurations import PROJECT_ROOT_DIR


class FlyWayContainer:
    def __init__(self, container_host_ip: str, exposed_port: int) -> None:
        self.container_host_ip = container_host_ip
        self.exposed_port = exposed_port
        self.docker_client = docker.from_env()

    def run_migrate_command(self) -> None:
        self.docker_client.containers.run(
            "redgate/flyway",
            f'-configFiles="conf/flyway_test.toml"'
            f' -url="jdbc:postgresql://{self.container_host_ip}:{self.exposed_port}/default_db"'
            f" -baselineOnMigrate=true migrate -environment=default"
            ,
            volumes={
                f"{os.path.join(PROJECT_ROOT_DIR, 'database/migrations')}": {
                    "bind": "/flyway/migrations",
                    "mode": "ro",
                },
                f"{os.path.join(PROJECT_ROOT_DIR, 'database/conf')}": {
                    "bind": "/flyway/conf",
                    "mode": "ro",
                },
            },
        )

    def clean_flyway_containers(self) -> None:
        flyway_containers = self.docker_client.containers.list(
            all=True, filters={"ancestor": "redgate/flyway"}
        )
        for container in flyway_containers:
            container.remove()
