import numpy as np
import pandas as pd
from psycopg2.extensions import register_adapter, AsIs
from psycopg2.extras import Json


def datetime_adapt(value: np.datetime64) -> AsIs:
    return AsIs(f"'{str(value)}'::timestamp") if not pd.isna(value) else AsIs("NULL")


def boolean_adapt(value: np.bool_) -> AsIs:
    return AsIs("TRUE") if value else AsIs("FALSE")


def register_postgresql_adapters() -> None:
    register_adapter(dict, Json)
    register_adapter(np.datetime64, datetime_adapt)
    register_adapter(np.int64, AsIs)
    register_adapter(np.float64, AsIs)
    register_adapter(np.bool_, boolean_adapt)
