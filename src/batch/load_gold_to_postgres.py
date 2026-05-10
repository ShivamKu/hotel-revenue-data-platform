from pyspark.sql.functions import col

from src.common.config_reader import read_config
from src.common.logger import get_logger
from src.common.spark_session import create_spark_session
from src.common.postgres_jdbc import write_df_to_postgres


logger = get_logger(__name__)


def main():
    config = read_config()

    spark = create_spark_session("load_gold_to_postgres")

    gold_path = config["paths"]["gold_daily_revenue"]
    postgres_config = config["postgres"]

    try:
        logger.info(f"Reading gold daily revenue from: {gold_path}")

        gold_df = spark.read.parquet(gold_path)

        final_df = (
            gold_df
            .select(
                col("property_code"),
                col("stay_date"),
                col("reservation_count").cast("int"),
                col("booked_rooms").cast("int"),
                col("total_revenue").cast("decimal(18,2)"),
                col("adr").cast("decimal(18,2)"),
                col("gold_ingestion_ts")
            )
        )

        logger.info(f"Gold rows to write into PostgreSQL: {final_df.count()}")

        write_df_to_postgres(
            df=final_df,
            postgres_config=postgres_config,
            table_name="hotel_revenue.gold_stay_daily_revenue",
            mode="append"
        )

        logger.info("Gold to PostgreSQL load completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()