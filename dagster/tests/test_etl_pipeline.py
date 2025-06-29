# dagster/tests/test_etl_pipeline.py
import pandas as pd

from dagster import build_op_context

from pipelines.etl_pipeline import (
    load_from_minio,
    write_to_duckdb,
    load_from_duckdb,
    write_to_postgres,
    read_count_from_postgres
)



def test_load_from_minio():
    context = build_op_context()
    df = load_from_minio(context)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_write_and_read_duckdb():
    context = build_op_context()
    df = load_from_minio(context)
    write_to_duckdb(context, df)
    df_duck = load_from_duckdb(context)
    assert isinstance(df_duck, pd.DataFrame)
    assert len(df_duck) == len(df)


def test_postgres_write_and_count():
    context = build_op_context()
    df = load_from_minio(context)
    write_to_postgres(context, df)
    count = read_count_from_postgres(context)
    assert isinstance(count, int)
    assert count == len(df)

def test_dataframe_columns():
    context = build_op_context()
    df = load_from_minio(context)
    expected_columns = {"INDICE", "01", "02", "03"}
    assert expected_columns.issubset(set(df.columns))
