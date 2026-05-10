from src.common.config_reader import read_config
from src.common.spark_session import create_spark_session


def main():
    config = read_config()
    spark = create_spark_session("show_outputs")

    print("\n========= BRONZE RESERVATIONS =========")
    spark.read.parquet(config["paths"]["bronze_reservations"]).show(10, truncate=False)

    print("\n========= SILVER RESERVATIONS =========")
    spark.read.parquet(config["paths"]["silver_reservations"]).show(10, truncate=False)

    print("\n========= GOLD DAILY REVENUE =========")
    spark.read.parquet(config["paths"]["gold_daily_revenue"]).show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()