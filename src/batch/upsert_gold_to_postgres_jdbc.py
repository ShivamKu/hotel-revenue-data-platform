from pyspark.sql.functions import col

from src.common.config_reader import read_config
from src.common.logger import get_logger
from src.common.spark_session import create_spark_session
from src.common.postgres_jdbc import write_df_to_postgres_jdbc
from src.common.postgres_sql_runner import execute_postgres_sql


logger = get_logger(__name__)


STAGING_TABLE = "hotel_revenue.stg_gold_stay_daily_revenue"
FINAL_TABLE = "hotel_revenue.gold_stay_daily_revenue"


def build_upsert_sql() -> str:
    return f"""
        INSERT INTO {FINAL_TABLE} (
            property_code,
            stay_date,
            reservation_count,
            booked_rooms,
            total_revenue,
            adr,
            gold_ingestion_ts,
            created_at,
            updated_at
        )
        SELECT
            property_code,
            stay_date,
            reservation_count,
            booked_rooms,
            total_revenue,
            adr,
            gold_ingestion_ts,
            CURRENT_TIMESTAMP AS created_at,
            NULL AS updated_at
        FROM {STAGING_TABLE}
        ON CONFLICT (property_code, stay_date)
        DO UPDATE SET
            reservation_count = EXCLUDED.reservation_count,
            booked_rooms = EXCLUDED.booked_rooms,
            total_revenue = EXCLUDED.total_revenue,
            adr = EXCLUDED.adr,
            gold_ingestion_ts = EXCLUDED.gold_ingestion_ts,
            updated_at = CURRENT_TIMESTAMP;
    """


def main():
    config = read_config()

    spark = create_spark_session("upsert_gold_to_postgres_jdbc")

    gold_path = config["paths"]["gold_daily_revenue"]
    postgres_config = config["postgres"]

    try:
        logger.info(f"Reading gold stay daily revenue from: {gold_path}")

        gold_df = spark.read.parquet(gold_path)

        final_df = (
            gold_df
            .select(
                col("property_code"),
                col("stay_date"),
                col("reservation_count").cast("int").alias("reservation_count"),
                col("booked_rooms").cast("int").alias("booked_rooms"),
                col("total_revenue").cast("decimal(18,2)").alias("total_revenue"),
                col("adr").cast("decimal(18,2)").alias("adr"),
                col("gold_ingestion_ts")
            )
        )

        row_count = final_df.count()
        logger.info(f"Gold rows found for PostgreSQL JDBC upsert: {row_count}")

        logger.info("Truncating staging table before load")
        execute_postgres_sql(
            postgres_config=postgres_config,
            sql=f"TRUNCATE TABLE {STAGING_TABLE};"
        )

        logger.info("Writing gold dataframe to staging table using JDBC")
        write_df_to_postgres_jdbc(
            df=final_df,
            postgres_config=postgres_config,
            table_name=STAGING_TABLE,
            mode="append"
        )

        logger.info("Running PostgreSQL upsert from staging to final table")
        execute_postgres_sql(
            postgres_config=postgres_config,
            sql=build_upsert_sql()
        )

        logger.info("Cleaning staging table after successful upsert")
        execute_postgres_sql(
            postgres_config=postgres_config,
            sql=f"TRUNCATE TABLE {STAGING_TABLE};"
        )

        logger.info("Gold stay daily revenue JDBC upsert job completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()