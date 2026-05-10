from pyspark.sql import SparkSession
import os
from pathlib import Path

def create_spark_session(app_name: str) -> SparkSession:

    project_root = Path.cwd()

    warehouse_dir = str(project_root / "spark-warehouse").replace("\\", "/")
    spark_local_dir = str(project_root / "tmp" / "spark-local").replace("\\", "/")
    checkpoint_dir = str(project_root / "spark-checkpoints").replace("\\", "/")
    postgres_jar = str(project_root / "jars" / "postgresql-42.7.4.jar").replace("\\", "/")

    Path(warehouse_dir).mkdir(parents=True, exist_ok=True)
    Path(spark_local_dir).mkdir(parents=True, exist_ok=True)
    Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)

    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.sql.warehouse.dir", f"file:///{warehouse_dir}")
        .config("spark.local.dir", spark_local_dir)
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={spark_local_dir}")
        .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={spark_local_dir}")
        .config("spark.jars",f"file:///{postgres_jar}")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark