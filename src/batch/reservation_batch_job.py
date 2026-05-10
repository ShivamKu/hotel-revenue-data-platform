from pyspark.sql import DataFrame
from pyspark.sql.functions import (col,lit,current_timestamp,to_date,to_timestamp,when,sum as spark_sum,
                                   count,countDistinct,round as spark_round,datediff,sequence,explode,
                                   expr)
from src.common.config_reader import  read_config
from src.common.logger import get_logger
from src.common.spark_session import create_spark_session

logger = get_logger(__name__)

VALID_BOOKING_STATUSES =["BOOKED","CANCELLED","MODIFIED"]

def read_raw_reservations(spark, input_path:str) -> DataFrame:
    logger.info(f"Reading raw reservation CSV from: {input_path}")

    return(
        spark.read.option("header","true").option("inferSchema","true").csv(input_path)
           )

def create_bronze(df: DataFrame, business_date: str) -> DataFrame:
    logger.info("Creating bronze reservation dataframe")

    return(
        df.withColumn("business_date",lit(business_date))
        .withColumn("bronze_ingestion_ts",current_timestamp())
        .withColumn("source_system",lit("dummy_reservation_generator"))
    )

def add_validation_reason(df: DataFrame) -> DataFrame:
    logger.info("Applying reservation validation rules")

    return (
        df.withColumn("validation_reason",
                      when(col("property_code").isNull(),lit("PROPERTY_CODE_NULL"))
                      .when(col("confirmation_number").isNull(),lit("CONFIRMATION_NUMBER_NULL"))
                      .when(col("arrival_date").isNull(), lit("ARRIVAL_DATE_NULL"))
                      .when(col("departure_date").isNull(), lit("DEPARTURE_DATE_NULL"))
                      .when(
                          to_date(col("arrival_date"), "dd-MM-yyyy") >= to_date(col("departure_date"), "dd-MM-yyyy"),
                          lit("INVALID_STAY_DATES")
                      )
                      .when(col("room_count") <= 0, lit("INVALID_ROOM_COUNT"))
                      .when(col("total_revenue") < 0, lit("NEGATIVE_REVENUE"))
                      .when(~col("booking_status").isin(VALID_BOOKING_STATUSES), lit("INVALID_BOOKING_STATUS"))
                      .otherwise(lit(None))
                      )

    )

def create_silver(valid_df: DataFrame) -> DataFrame:
    logger.info("Creating silver reservation dataframe")

    return (
        valid_df
        .withColumn("arrival_date", to_date(col("arrival_date"), "dd-MM-yyyy"))
        .withColumn("departure_date", to_date(col("departure_date"), "dd-MM-yyyy"))
        .withColumn("created_ts", to_timestamp(col("created_ts"), "dd-MM-yyyy HH:mm"))
        .withColumn("updated_ts", to_timestamp(col("updated_ts"), "dd-MM-yyyy HH:mm"))
        .withColumn("room_count", col("room_count").cast("int"))
        .withColumn("total_revenue", col("total_revenue").cast("double"))
        .withColumn("silver_ingestion_ts", current_timestamp())
        .drop("validation_reason")
    )

def create_gold_daily_revenue(silver_df: DataFrame) -> DataFrame:
    logger.info("Creating gold daily revenue metrics")

    active_reservation_df = (
                     silver_df
                     .filter(col("booking_status") != "CANCELLED")
                     .withColumn("stay_nights",datediff(col("departure_date"),col("arrival_date")))
                     .filter(col("stay_nights") >0)
        .withColumn("revenue_per_night",col("total_revenue")/col("stay_nights")) )

    stay_level_df = (
        active_reservation_df
        .withColumn(
            "stay_date",
            explode(
                sequence(
                    col("arrival_date"),
                    expr("date_sub(departure_date, 1)")
                )
            )
        )
    )

    gold_df = (
        stay_level_df
        .groupBy("property_code", "stay_date")
        .agg(
            countDistinct("reservation_id").alias("reservation_count"),
            spark_sum("room_count").alias("booked_rooms"),
            spark_round(spark_sum("revenue_per_night"), 2).alias("total_revenue")
        )
        .withColumn(
            "adr",
            spark_round(col("total_revenue") / col("booked_rooms"), 2)
        )
        .withColumn("gold_ingestion_ts", current_timestamp())
    )

    return gold_df

def write_parquet(df: DataFrame, output_path: str, mode: str = "overwrite") -> None:
    logger.info(f"Writing parquet output to: {output_path}")

    (
        df.write
        .mode(mode)
        .parquet(output_path)
    )


def main():
    config = read_config()

    app_name = f"{config['app']['name']}_reservation_batch_job"
    spark= create_spark_session(app_name)

    business_date = config["data_generation"]["business_date"]

    raw_input_path = config["paths"]["raw_reservations_csv"]
    bronze_output_path=config["paths"]["bronze_reservations"]
    silver_output_path=config["paths"]["silver_reservations"]
    gold_output_path=config["paths"]["gold_daily_revenue"]
    rejected_output_path=config["paths"]["rejected_reservations"]

    try:
        raw_df= read_raw_reservations(spark,raw_input_path)

        logger.info(f"Raw reservation count: {raw_df.count()}")

        bronze_df = create_bronze(raw_df, business_date)
        write_parquet(bronze_df, bronze_output_path)

        validate_df = add_validation_reason(bronze_df)

        valid_df = validate_df.filter(col("validation_reason").isNull())
        rejected_df = validate_df.filter(col("validation_reason").isNotNull())

        logger.info(f"Valid reservationcount: {valid_df.count()}")
        logger.info(f"Rejected reservation count: {rejected_df.count()}")

        silver_df = create_silver(valid_df)
        write_parquet(silver_df,silver_output_path)

        if rejected_df.count() >0:
            write_parquet(rejected_df, rejected_output_path)

        gold_df = create_gold_daily_revenue(silver_df)
        write_parquet(gold_df,gold_output_path)

        logger.info("Reservation batch job completed successfully")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
