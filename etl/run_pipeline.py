from pathlib import Path

import pandas as pd
from sqlalchemy import text

from ingestion.generator import generate_data
from processing.transform import (
    clean_raw_data,
    build_dim_channel,
    build_dim_video,
    build_fact_video_daily,
)
from utils.db import get_engine, execute_sql_file
from utils.helpers import load_yaml, ensure_dir, resolve_path
from utils.logger import get_logger

logger = get_logger(__name__)


def initialize_database(engine) -> None:
    logger.info("Initializing database objects")
    execute_sql_file(engine, "sql/01_create_schemas.sql")
    execute_sql_file(engine, "sql/02_create_tables.sql")
    execute_sql_file(engine, "sql/03_create_views.sql")


def truncate_tables(engine, settings: dict) -> None:
    raw_schema = settings["database"]["raw_schema"]
    mart_schema = settings["database"]["mart_schema"]

    raw_table = settings["database"]["raw_table"]
    dim_channel_table = settings["database"]["dim_channel_table"]
    dim_video_table = settings["database"]["dim_video_table"]
    fact_table = settings["database"]["fact_table"]

    truncate_raw = settings["pipeline"]["truncate_raw_before_load"]
    truncate_mart = settings["pipeline"]["truncate_mart_before_load"]

    with engine.begin() as conn:
        if truncate_raw:
            conn.execute(text(f"TRUNCATE TABLE {raw_schema}.{raw_table}"))

        if truncate_mart:
            conn.execute(
                text(
                    f"TRUNCATE TABLE "
                    f"{mart_schema}.{fact_table}, "
                    f"{mart_schema}.{dim_video_table}, "
                    f"{mart_schema}.{dim_channel_table} "
                    f"CASCADE"
                )
            )

    logger.info("Truncate step completed")


def load_raw(engine, df: pd.DataFrame, settings: dict) -> None:
    raw_schema = settings["database"]["raw_schema"]
    raw_table = settings["database"]["raw_table"]
    chunk_size = settings["pipeline"]["chunk_size"]

    df.to_sql(
        raw_table,
        engine,
        schema=raw_schema,
        if_exists="append",
        index=False,
        chunksize=chunk_size,
        method="multi",
    )
    logger.info("Raw data loaded into %s.%s", raw_schema, raw_table)


def load_mart(engine, dim_channel: pd.DataFrame, dim_video: pd.DataFrame, fact: pd.DataFrame, settings: dict) -> None:
    mart_schema = settings["database"]["mart_schema"]
    chunk_size = settings["pipeline"]["chunk_size"]

    dim_channel.to_sql(
        settings["database"]["dim_channel_table"],
        engine,
        schema=mart_schema,
        if_exists="append",
        index=False,
        chunksize=chunk_size,
        method="multi",
    )

    dim_video.to_sql(
        settings["database"]["dim_video_table"],
        engine,
        schema=mart_schema,
        if_exists="append",
        index=False,
        chunksize=chunk_size,
        method="multi",
    )

    fact.to_sql(
        settings["database"]["fact_table"],
        engine,
        schema=mart_schema,
        if_exists="append",
        index=False,
        chunksize=chunk_size,
        method="multi",
    )

    logger.info("Mart tables loaded successfully")


def save_processed_outputs(dim_channel: pd.DataFrame, dim_video: pd.DataFrame, fact: pd.DataFrame, output_dir: str) -> None:
    ensure_dir(output_dir)
    output_path = resolve_path(output_dir)
    dim_channel.to_csv(output_path / "dim_channel.csv", index=False)
    dim_video.to_csv(output_path / "dim_video.csv", index=False)
    fact.to_csv(output_path / "fact_video_daily.csv", index=False)
    logger.info("Processed output CSV files saved to %s", output_dir)


def run() -> None:
    settings = load_yaml("configs/settings.yaml")
    processed_dir = settings["paths"]["processed_output_dir"]

    logger.info("Pipeline started")
    engine = get_engine()
    try:
        initialize_database(engine)

        raw_file = generate_data()
        raw_df = pd.read_csv(resolve_path(raw_file))

        cleaned_df = clean_raw_data(raw_df)
        dim_channel = build_dim_channel(cleaned_df)
        dim_video = build_dim_video(cleaned_df)
        fact = build_fact_video_daily(cleaned_df)

        truncate_tables(engine, settings)
        load_raw(engine, cleaned_df, settings)
        load_mart(dim_channel=dim_channel, dim_video=dim_video, fact=fact, engine=engine, settings=settings)
        save_processed_outputs(dim_channel, dim_video, fact, processed_dir)

        logger.info("Pipeline completed successfully")
    except Exception:
        logger.exception("Pipeline failed")
        raise


if __name__ == "__main__":
    run()
