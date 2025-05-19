import logging
import pandas as pd

from nexuslabdata.connection.postgresql import PostgreSQLService


class SimplePipeline:
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
        df = self.service.fetch_table(schema_name=self.schema_name, table_name=self.source_table)
        df = df[["id", "description"]]

        # Filters based on the provided parameters
        if filter_out:
            df = df[~df[filter_column].isin(filter_out)]
            logging.info(f"Filtered out rows with {filter_column} in {filter_out}, remaining rows: {len(df)}")

        self.service.insert_dataframe_to_postgres(df, schema_name=self.schema_name, table_name=self.target_table)

        return
