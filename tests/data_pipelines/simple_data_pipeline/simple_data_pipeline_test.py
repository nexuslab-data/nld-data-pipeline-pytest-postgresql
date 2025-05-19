import logging
import os
import pytest
import pandas as pd
from typing import Generator, Any

from tests.data_pipelines.simple_data_pipeline.simple_data_pipeline import SimplePipeline
from nexuslabdata.connection.postgresql import PostgreSQLConnectionWrapper, PostgreSQLService

TEST_FILE_FOLDER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "simple_data_pipeline_test_files",
)


@pytest.fixture(scope="function", autouse=True)
def setup_and_teardown_data(postgresql: PostgreSQLConnectionWrapper) -> Generator[Any, Any, None]:
    pgsql_service = PostgreSQLService(postgresql)

    yield

    pgsql_service.truncate_table(schema_name="test_schema", table_name="table_1")
    pgsql_service.truncate_table(schema_name="test_schema", table_name="table_2")



def test_load_using_local_file(postgresql: PostgreSQLConnectionWrapper) -> None:
    """ Test the data pipeline with a local CSV file. """

    SCHEMA_NAME = "test_schema"
    SOURCE_TABLE = "table_1"
    TARGET_TABLE = "table_2"

    # Setup the PostgreSQL service
    pgsql_service = PostgreSQLService(postgresql)

    # Setup the simple pipeline
    pipeline = SimplePipeline(
        pgsql_service, schema_name=SCHEMA_NAME, source_table=SOURCE_TABLE, target_table=TARGET_TABLE
    )
    
    # Load the CSV file into the PostgreSQL database
    source_df = pd.read_csv(os.path.join(TEST_FILE_FOLDER_PATH, "table_1_source.csv"), sep=";")
    pgsql_service.insert_dataframe_to_postgres(source_df, schema_name=SCHEMA_NAME, table_name=SOURCE_TABLE)

    # Execute the pipeline run
    pipeline.run(
        filter_column="id",
        filter_out=["id_2"]
    )

    # Read the loaded data
    result_df = pgsql_service.fetch_table(schema_name=SCHEMA_NAME, table_name=TARGET_TABLE)
    result_df = result_df.drop(columns=["updated_at"])
    logging.info(f"Fetched data from table {TARGET_TABLE}, rows: {len(result_df)}")

    # Read the expected DataFrame
    expected_df = pd.read_csv(
        os.path.join(TEST_FILE_FOLDER_PATH, "table_2_expected.csv"),
        sep=";"
    )
    
    # Compare the DataFrames
    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True),
        expected_df.reset_index(drop=True)
    )