-- Ranks coins by priority for update in coins table
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

-- Ranks coins by priority for new price insert in continuous_usd_prices table
CREATE OR REPLACE FUNCTION usd_price_priority(p_limit INTEGER)
RETURNS TABLE (
     coin_id VARCHAR(255),
     vol_24h BIGINT,
     created_at TIMESTAMP
) AS $$
BEGIN
RETURN QUERY
     SELECT 
          recent_prices.coin_id, 
          recent_prices.vol_24h, 
          recent_prices.created_at
     FROM (
          SELECT 
               DISTINCT ON (cup.coin_id) 
               cup.coin_id, 
               cup.vol_24h, 
               cup.created_at
          FROM 
               continuous_usd_prices AS cup
          JOIN
               coins c ON cup.coin_id = c.id
               AND c.track_prices = TRUE
          ORDER BY 
               cup.coin_id, 
               cup.created_at DESC
     ) AS recent_prices
     ORDER BY 
        ROW_NUMBER() OVER (ORDER BY recent_prices.vol_24h ASC) + (EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - recent_prices.created_at)) / 60) DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- Ranks coins by priority for new price insert in continuous_btc_prices table
CREATE OR REPLACE FUNCTION btc_price_priority(p_limit INTEGER)
RETURNS TABLE (
     coin_id VARCHAR(255),
     vol_24h BIGINT,
     created_at TIMESTAMP
) AS $$
BEGIN
RETURN QUERY
     SELECT 
          recent_prices.coin_id, 
          recent_prices.vol_24h, 
          recent_prices.created_at
     FROM (
          SELECT 
               DISTINCT ON (cup.coin_id) 
               cup.coin_id, 
               cup.vol_24h, 
               cup.created_at
          FROM 
               continuous_btc_prices AS cup
          JOIN
               coins c ON cup.coin_id = c.id
               AND c.track_prices = TRUE
          ORDER BY 
               cup.coin_id, 
               cup.created_at DESC
     ) AS recent_prices
     ORDER BY 
        ROW_NUMBER() OVER (ORDER BY recent_prices.vol_24h ASC) + (EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP AT TIME ZONE 'UTC' - recent_prices.created_at)) / 60) DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;
