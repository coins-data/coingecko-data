CREATE OR REPLACE FUNCTION coins_to_update(p_limit INTEGER)
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
    WHERE (coins.update_hourly = TRUE AND coins.updated_at < CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - INTERVAL '1 hour')
       OR coins.id IN (
           SELECT 
                coins.id
           FROM 
                coins
           ORDER BY 
                coins.market_cap_rank - (EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - coins.updated_at)) / 60),
                random()
           LIMIT p_limit
       );
END;
$$ LANGUAGE plpgsql;
