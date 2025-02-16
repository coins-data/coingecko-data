CREATE OR REPLACE FUNCTION coins_to_update()
RETURNS TABLE (
    id VARCHAR(255),
    update_hourly BOOLEAN,
    updated_at TIMESTAMP,
    market_cap_rank INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT id, update_hourly, updated_at, market_cap_rank
    FROM coins
    WHERE (update_hourly = TRUE AND updated_at < CURRENT_TIMESTAMP - INTERVAL '1 hour')
       OR id IN (
           SELECT id
           FROM coins
           WHERE update_hourly = FALSE
           ORDER BY market_cap_rank - EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - updated_at)) / 60
           LIMIT 100
       );
END;
$$ LANGUAGE plpgsql;
