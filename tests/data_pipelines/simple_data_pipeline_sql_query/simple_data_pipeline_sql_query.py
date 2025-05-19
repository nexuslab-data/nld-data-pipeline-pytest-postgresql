from nexuslabdata.connection.base import QueryWrapper
from nexuslabdata.connection.postgresql import PostgreSQLService


class SimpleSqlQueryPipeline:
    def __init__(self, service: PostgreSQLService, schema_name: str, source_table: str, target_table: str):
        self.service = service
        self.schema_name = schema_name
        self.source_table = source_table
        self.target_table = target_table

    def run(
        self,
        filter_column: str = "id",
        filter_out: list = None
    ) -> None:
        self.service.execute_query(
            QueryWrapper(
                query="INSERT INTO {{ schema_name }}.{{ target_table }} "
                      "SELECT id, description "
                      "FROM {{ schema_name }}.{{ source_table }} "
                      "WHERE {{ filter_column }} not in ('{{ filter_out | first }}')",
                params={
                    "schema_name": self.schema_name,
                    "target_table": self.target_table,
                    "source_table": self.source_table,
                    "filter_column": filter_column,
                    "filter_out": filter_out
                }
            )
        )
        return
