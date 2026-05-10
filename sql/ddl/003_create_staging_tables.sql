CREATE TABLE IF NOT EXISTS hotel_revenue_prod.stg_gold_stay_daily_revenue (
    run_id VARCHAR(100) NOT NULL,
    property_code VARCHAR(20),
    stay_date DATE,
    reservation_count INTEGER,
    booked_rooms INTEGER,
    total_revenue NUMERIC(18, 2),
    adr NUMERIC(18, 2),
    business_date DATE,
    source_system VARCHAR(100),
    gold_ingestion_ts TIMESTAMP,
    staged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);