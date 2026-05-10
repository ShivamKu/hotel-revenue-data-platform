from pyspark.sql.functions import col, lit, to_date

from src.common.config_reader import read_config
from src.common.logger import get_logger
from src.common.spark_session import create_spark_session
from src.common.run_context import generate_run_id
from src.common.postgres_jdbc import write_df_to_postgres_jdbc
from src.common.postgres_sql_runner import execute_postgres_sql, fetch_single_value
from src.common.audit_logger import (
    insert_audit_start,
    update_audit_success,
    update_audit_failure
)


logger = get_logger(__name__)


JOB_NAME = "load_gold_stay_revenue_to_postgres"
STAGING_TABLE = "hotel_revenue_prod.stg_gold_stay_daily_revenue"
FINAL_TABLE = "hotel_revenue_prod.gold_stay_daily_revenue"


def cleanup_staging_for_run(postgres_config: dict, run_id: str) -> None:
    sql = f"""
        DELETE FROM {STAGING_TABLE}
        WHERE run_id = '{run_id}';
    """

    execute_postgres_sql(postgres_config, sql)


def call_merge_procedure(postgres_config: dict, run_id: str) -> None:
    sql = f"""
        CALL hotel_revenue_prod.sp_merge_gold_stay_daily_revenue('{run_id}');
    """

    execute_postgres_sql(postgres_config, sql)


def get_staged_count(postgres_config: dict, run_id: str) -> int:
    sql = f"""
        SELECT COUNT(*)
        FROM {STAGING_TABLE}
        WHERE run_id = '{run_id}';
    """

    return fetch_single_value(postgres_config, sql)


def get_merged_count(postgres_config: dict, run_id: str) -> int:
    sql = f"""
        SELECT COUNT(*)
        FROM {FINAL_TABLE}
        WHERE last_run_id = '{run_id}';
    """

    return fetch_single_value(postgres_config, sql)


def main():
    config = read_config()

    business_date = config["data_generation"]["business_date"]
    postgres_config = config["postgres"]
    gold_path = config["paths"]["gold_daily_revenue"]

    run_id = generate_run_id(JOB_NAME, business_date)

    logger.info(f"Starting job: {JOB_NAME}")
    logger.info(f"Generated run_id: {run_id}")

    spark = create_spark_session(JOB_NAME)

    try:
        insert_audit_start(
            postgres_config=postgres_config,
            run_id=run_id,
            job_name=JOB_NAME,
            business_date=business_date
        )

        logger.info(f"Reading gold parquet from: {gold_path}")
        gold_df = spark.read.parquet(gold_path)

        gold_count = gold_df.count()
        logger.info(f"Gold parquet row count: {gold_count}")

        staging_df = (
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
            .withColumn("run_id", lit(run_id))
            .withColumn("business_date", to_date(lit(business_date)))
            .withColumn("source_system", lit("gold_parquet_layer"))
            .select(
                "run_id",
                "property_code",
                "stay_date",
                "reservation_count",
                "booked_rooms",
                "total_revenue",
                "adr",
                "business_date",
                "source_system",
                "gold_ingestion_ts"
            )
        )

        logger.info("Cleaning staging records for current run_id before JDBC load")
        cleanup_staging_for_run(postgres_config, run_id)

        logger.info("Writing gold records to staging table using Spark JDBC")
        write_df_to_postgres_jdbc(
            df=staging_df,
            postgres_config=postgres_config,
            table_name=STAGING_TABLE,
            mode="append"
        )

        staged_count = get_staged_count(postgres_config, run_id)
        logger.info(f"Staged row count for run_id={run_id}: {staged_count}")

        if staged_count != gold_count:
            raise ValueError(
                f"Staged count mismatch. gold_count={gold_count}, staged_count={staged_count}"
            )

        logger.info("Calling PostgreSQL merge procedure")
        call_merge_procedure(postgres_config, run_id)

        merged_count = get_merged_count(postgres_config, run_id)
        logger.info(f"Merged row count for run_id={run_id}: {merged_count}")

        logger.info("Cleaning staging records after successful merge")
        cleanup_staging_for_run(postgres_config, run_id)

        update_audit_success(
            postgres_config=postgres_config,
            run_id=run_id,
            gold_count=gold_count,
            staged_count=staged_count,
            merged_count=merged_count
        )

        logger.info(f"Job completed successfully. run_id={run_id}")

    except Exception as error:
        logger.error(f"Job failed. run_id={run_id}. Error: {error}")

        update_audit_failure(
            postgres_config=postgres_config,
            run_id=run_id,
            error_message=str(error)
        )

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()