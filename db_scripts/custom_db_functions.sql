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

-- Formats number into short easily readable format
CREATE OR REPLACE FUNCTION format_number(num numeric)
RETURNS text AS $$
DECLARE
     formatted_number text;
     decimal_part text;
     zero_count int;
BEGIN
     IF num = 0 THEN
          formatted_number := '0';
     ELSIF num < 1 THEN
          -- Format very small numbers with scientific like notation
          IF num < 0.0001 THEN
               decimal_part := split_part(to_char(num, '999999990.999999999999999'), '.', 2);
               zero_count := length(regexp_replace(decimal_part, '^(0*)[1-9].*$', '\1'));

               IF zero_count > 0 THEN
                    formatted_number := '0.0' || chr(8320 + zero_count) || substr(decimal_part, zero_count + 1);
               ELSE
                    formatted_number := to_char(num, 'FM999999990.999999999999999');
               END IF;
          ELSIF num < 0.01 THEN
               formatted_number := trim(trailing '0' FROM to_char(num, 'FM0.000000'));
          ELSE
               formatted_number := trim(trailing '0' FROM to_char(num, 'FM0.0000'));
          END IF;
     ELSE
          -- Add suffixes for large numbers
          IF num >= 1000000000000000 THEN
               formatted_number := to_char(num, 'FM9.99EEEE');
          ELSIF num >= 1000000000000 THEN
               formatted_number := trim(trailing '0' FROM  to_char(num / 1000000000000, 'FM999G999D00')) || 'T';
          ELSIF num >= 1000000000 THEN
               formatted_number := trim(trailing '0' FROM  to_char(num / 1000000000, 'FM999G999D00')) || 'B';
          ELSIF num >= 1000000 THEN
               formatted_number := trim(trailing '0' FROM  to_char(num / 1000000, 'FM999G999D00')) || 'M';

          -- Format number with commas
          ELSIF num >= 1000 THEN
               formatted_number := to_char(num, 'FM999G999');

          -- Format number with two decimal places, if it has a decimal part
          ELSIF num % 1 = 0 THEN
               formatted_number := to_char(num, 'FM999G999');     
          ELSE
               formatted_number := to_char(round(num, 2), 'FM999G999D00');
          END IF;
  END IF;
  RETURN formatted_number;
END;
$$ LANGUAGE plpgsql;