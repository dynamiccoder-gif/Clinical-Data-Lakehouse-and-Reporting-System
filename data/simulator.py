import os
import time
import pandas as pd
from datetime import datetime
import argparse

def _spark_session():
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    return SparkSession.builder.getOrCreate()


def _read_saved_offset(offset_table, source_name):
    if not offset_table:
        return None

    spark = _spark_session()
    if spark is None:
        print("[OFFSET] PySpark is unavailable; using start_chunk instead.")
        return None

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {offset_table} (
            source_name STRING,
            last_processed_row BIGINT,
            updated_at TIMESTAMP
        )
        USING DELTA
        """
    )

    rows = spark.sql(
        f"""
        SELECT COALESCE(MAX(last_processed_row), 0) AS last_processed_row
        FROM {offset_table}
        WHERE source_name = '{source_name}'
        """
    ).collect()
    return int(rows[0]["last_processed_row"]) if rows else 0


def _write_saved_offset(offset_table, source_name, last_processed_row):
    if not offset_table:
        return

    spark = _spark_session()
    if spark is None:
        return

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW simulator_offset_update AS
        SELECT
            '{source_name}' AS source_name,
            CAST({int(last_processed_row)} AS BIGINT) AS last_processed_row,
            current_timestamp() AS updated_at
        """
    )
    spark.sql(
        f"""
        MERGE INTO {offset_table} AS target
        USING simulator_offset_update AS source
        ON target.source_name = source.source_name
        WHEN MATCHED THEN UPDATE SET
            last_processed_row = source.last_processed_row,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def _csv_chunk_reader(source, size, start_row):
    if start_row <= 0:
        return pd.read_csv(source, chunksize=size)
    return pd.read_csv(
        source,
        chunksize=size,
        skiprows=range(1, start_row + 1),
    )


def start_simulation(
    source,
    target,
    size=5000,
    interval_seconds=5,
    max_chunks=None,
    loop=False,
    start_chunk=0,
    offset_table=None,
    source_name="encounters",
):
    print(f"📦 Unlocking source data block vault from {source}...")
    os.makedirs(target, exist_ok=True)

    print(f"🚀 Feeding data stream landing zone incrementally...")
    total_chunks = 0
    replay_round = 0
    saved_offset = _read_saved_offset(offset_table, source_name)
    start_row = saved_offset if saved_offset is not None else start_chunk * size
    next_offset = start_row
    print(f"[OFFSET] Starting {source_name} at source row offset {start_row:,}.")

    while True:
        reader = _csv_chunk_reader(source, size, start_row if replay_round == 0 else 0)
        for i, chunk in enumerate(reader):
            if max_chunks is not None and total_chunks >= max_chunks:
                print(f"✅ [SIMULATOR] Reached configured max_chunks={max_chunks}.")
                return

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            chunk_start = next_offset
            chunk_end = chunk_start + len(chunk)
            chunk_name = (
                f"live_{source_name}_rows_{chunk_start}_{chunk_end}_"
                f"round_{replay_round}_{timestamp}.csv"
            )

            chunk.to_csv(os.path.join(target, chunk_name), index=False)
            next_offset = chunk_end
            _write_saved_offset(offset_table, source_name, next_offset)
            total_chunks += 1
            print(
                f"📥 [SIMULATOR] Landed {len(chunk):,} rows into ingestion folder: {chunk_name}. "
                f"Next offset: {next_offset:,}"
            )
            if interval_seconds > 0:
                time.sleep(interval_seconds)

        if not loop:
            print(f"✅ [SIMULATOR] Source exhausted after {total_chunks:,} chunks.")
            return

        replay_round += 1
        print(f"🔁 [SIMULATOR] Source replay round {replay_round} starting...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Land Synthea encounter chunks into the raw ingestion zone.")
    parser.add_argument("--source", default=os.getenv("SIMULATOR_SOURCE", "source_dump/encounters_combined.csv"))
    parser.add_argument("--target", default=os.getenv("SIMULATOR_TARGET", "raw_ingestion"))
    parser.add_argument("--chunk-size", type=int, default=int(os.getenv("SIMULATOR_CHUNK_SIZE", "5000")))
    parser.add_argument("--interval-seconds", type=float, default=float(os.getenv("SIMULATOR_INTERVAL_SECONDS", "5")))
    parser.add_argument("--max-chunks", type=int, default=int(os.getenv("SIMULATOR_MAX_CHUNKS", "0")) or None)
    parser.add_argument("--start-chunk", type=int, default=int(os.getenv("SIMULATOR_START_CHUNK", "0")))
    parser.add_argument("--loop", action="store_true", default=os.getenv("SIMULATOR_LOOP", "false").lower() == "true")
    parser.add_argument("--source-name", default=os.getenv("SIMULATOR_SOURCE_NAME", "encounters"))
    parser.add_argument("--offset-table", default=os.getenv("SIMULATOR_OFFSET_TABLE", "workspace.default.ingestion_offsets"))
    args = parser.parse_args()

    start_simulation(
        args.source,
        args.target,
        size=args.chunk_size,
        interval_seconds=args.interval_seconds,
        max_chunks=args.max_chunks,
        loop=args.loop,
        start_chunk=args.start_chunk,
        offset_table=args.offset_table,
        source_name=args.source_name,
    )
