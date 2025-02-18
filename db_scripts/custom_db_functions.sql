CREATE OR REPLACE FUNCTION coins_to_update()
RETURNS TABLE (
    id VARCHAR(255),
    update_hourly BOOLEAN,
    updated_at TIMESTAMP,
    market_cap_rank INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT coins.id, coins.update_hourly, coins.updated_at, coins.market_cap_rank
    FROM coins
    WHERE (coins.update_hourly = TRUE AND coins.updated_at < CURRENT_TIMESTAMP - INTERVAL '1 hour')
       OR coins.id IN (
           SELECT coins.id
           FROM coins
           WHERE coins.update_hourly = FALSE
           ORDER BY coins.market_cap_rank - EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - coins.updated_at)) / 60
           LIMIT 100
       );
END;
$$ LANGUAGE plpgsql;
