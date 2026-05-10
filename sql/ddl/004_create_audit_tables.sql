CREATE TABLE IF NOT EXISTS hotel_revenue.audit_batch_runs (
    run_id VARCHAR(100) PRIMARY KEY,
    job_name VARCHAR(200),
    business_date DATE,
    status VARCHAR(50),
    source_count INTEGER,
    bronze_count INTEGER,
    silver_count INTEGER,
    gold_count INTEGER,
    rejected_count INTEGER,
    staged_count INTEGER,
    merged_count INTEGER,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    error_message TEXT
);