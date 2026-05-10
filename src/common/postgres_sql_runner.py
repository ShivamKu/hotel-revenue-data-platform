import psycopg2

from src.common.logger import get_logger


logger = get_logger(__name__)


def get_postgres_connection(postgres_config: dict):
    return psycopg2.connect(
        host=postgres_config["host"],
        port=postgres_config["port"],
        dbname=postgres_config["database"],
        user=postgres_config["user"],
        password=postgres_config["password"]
    )


def execute_postgres_sql(postgres_config: dict, sql: str) -> None:
    conn = None

    try:
        conn = get_postgres_connection(postgres_config)
        conn.autocommit = False

        with conn.cursor() as cursor:
            cursor.execute(sql)

        conn.commit()
        logger.info("PostgreSQL SQL executed successfully")

    except Exception as error:
        if conn:
            conn.rollback()
        logger.error(f"PostgreSQL SQL execution failed: {error}")
        raise

    finally:
        if conn:
            conn.close()

def fetch_single_value(postgres_config: dict, sql: str):
    """
    Executes a SQL query and returns first column of first row.
    """

    conn = None

    try:
        conn = get_postgres_connection(postgres_config)

        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()

        if result:
            return result[0]

        return None

    finally:
        if conn:
            conn.close()