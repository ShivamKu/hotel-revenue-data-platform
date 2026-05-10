from datetime import datetime

from src.common.postgres_sql_runner import execute_postgres_sql


def escape_sql_text(value: str) -> str:
    if value is None:
        return None

    return value.replace("'", "''")


def insert_audit_start(
        postgres_config: dict,
        run_id: str,
        job_name: str,
        business_date: str
) -> None:
    sql = f"""
        INSERT INTO hotel_revenue_prod.audit_batch_runs (
            run_id,
            job_name,
            business_date,
            status,
            started_at
        )
        VALUES (
            '{run_id}',
            '{job_name}',
            DATE '{business_date}',
            'RUNNING',
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (run_id)
        DO UPDATE SET
            status = 'RUNNING',
            started_at = CURRENT_TIMESTAMP,
            ended_at = NULL,
            error_message = NULL;
    """

    execute_postgres_sql(postgres_config, sql)


def update_audit_success(
        postgres_config: dict,
        run_id: str,
        source_count: int = None,
        bronze_count: int = None,
        silver_count: int = None,
        gold_count: int = None,
        rejected_count: int = None,
        staged_count: int = None,
        merged_count: int = None
) -> None:

    sql = f"""
        UPDATE hotel_revenue_prod.audit_batch_runs
        SET
            status = 'SUCCESS',
            source_count = {source_count if source_count is not None else 'NULL'},
            bronze_count = {bronze_count if bronze_count is not None else 'NULL'},
            silver_count = {silver_count if silver_count is not None else 'NULL'},
            gold_count = {gold_count if gold_count is not None else 'NULL'},
            rejected_count = {rejected_count if rejected_count is not None else 'NULL'},
            staged_count = {staged_count if staged_count is not None else 'NULL'},
            merged_count = {merged_count if merged_count is not None else 'NULL'},
            ended_at = CURRENT_TIMESTAMP,
            error_message = NULL
        WHERE run_id = '{run_id}';
    """

    execute_postgres_sql(postgres_config, sql)


def update_audit_failure(
        postgres_config: dict,
        run_id: str,
        error_message: str
) -> None:

    escaped_error = escape_sql_text(error_message)

    sql = f"""
        UPDATE hotel_revenue_prod.audit_batch_runs
        SET
            status = 'FAILED',
            ended_at = CURRENT_TIMESTAMP,
            error_message = '{escaped_error}'
        WHERE run_id = '{run_id}';
    """

    execute_postgres_sql(postgres_config, sql)