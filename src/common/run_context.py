from datetime import datetime


def generate_run_id(job_name: str, business_date: str) -> str:
    """
    Generates a unique run_id for a pipeline run.

    Example:
    reservation_gold_load_20260509_20260510_211530
    """

    current_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    clean_job_name = (
        job_name
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
    )

    clean_business_date = business_date.replace("-", "")

    return f"{clean_job_name}_{clean_business_date}_{current_ts}"