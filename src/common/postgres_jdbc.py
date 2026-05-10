from pyspark.sql import DataFrame

from src.common.logger import get_logger

logger = get_logger(__name__)

def build_jdbc_url(postgres_config: dict) -> str:
    host = postgres_config["host"]
    port = postgres_config["port"]
    database=postgres_config["database"]

    return f"jdbc:postgresql://{host}:{port}/{database}"

def write_df_to_postgres_jdbc(df: DataFrame, postgres_config: dict, table_name: str, mode: str = "append") -> None:
    """Write Spark DataFrame to PostgresSQL using JDBC"""
    jdbc_url=build_jdbc_url(postgres_config)

    logger.info(f"Writting dataframe to PostgresSQl table: {table_name}")
    logger.info(f"JDBC url: {jdbc_url}")

    (
        df.write
        .format("jdbc")
        .option("url",jdbc_url)
        .option("dbtable",table_name)
        .option("user",postgres_config["user"])
        .option("password",postgres_config["password"])
        .option("driver",postgres_config.get("driver","org.postgresql.Driver"))
        .mode(mode)
        .save()
    )
    logger.info(f"Data successfully written to PostgreSQL table: {table_name}")