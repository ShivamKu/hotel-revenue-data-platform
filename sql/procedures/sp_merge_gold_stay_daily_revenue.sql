CREATE OR REPLACE PROCEDURE hotel_revenue_prod.sp_merge_gold_stay_daily_revenue(
    p_run_id VARCHAR
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO hotel_revenue_prod.gold_stay_daily_revenue (
        property_code,
        stay_date,
        reservation_count,
        booked_rooms,
        total_revenue,
        adr,
        business_date,
        source_system,
        last_run_id,
        gold_ingestion_ts,
        created_at,
        updated_at
    )
    SELECT
        property_code,
        stay_date,
        reservation_count,
        booked_rooms,
        total_revenue,
        adr,
        business_date,
        source_system,
        run_id AS last_run_id,
        gold_ingestion_ts,
        CURRENT_TIMESTAMP AS created_at,
        NULL AS updated_at
    FROM hotel_revenue_prod.stg_gold_stay_daily_revenue
    WHERE run_id = p_run_id
    ON CONFLICT (property_code, stay_date)
    DO UPDATE SET
        reservation_count = EXCLUDED.reservation_count,
        booked_rooms = EXCLUDED.booked_rooms,
        total_revenue = EXCLUDED.total_revenue,
        adr = EXCLUDED.adr,
        business_date = EXCLUDED.business_date,
        source_system = EXCLUDED.source_system,
        last_run_id = EXCLUDED.last_run_id,
        gold_ingestion_ts = EXCLUDED.gold_ingestion_ts,
        updated_at = CURRENT_TIMESTAMP;
END;
$$;