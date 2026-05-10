CREATE TABLE IF NOT EXISTS hotel_revenue_prod.gold_stay_daily_revenue (
    property_code VARCHAR(20) NOT NULL,
    stay_date DATE NOT NULL,
    reservation_count INTEGER,
    booked_rooms INTEGER,
    total_revenue NUMERIC(18, 2),
    adr NUMERIC(18, 2),
    business_date DATE,
    source_system VARCHAR(100),
    last_run_id VARCHAR(100),
    gold_ingestion_ts TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT pk_gold_stay_daily_revenue
        PRIMARY KEY (property_code, stay_date)
);