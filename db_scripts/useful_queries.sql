-- Recent price update intervals for each coin
WITH gaps AS (
    SELECT
        coin_id,
        api_last_updated,
        LAG(api_last_updated) OVER (PARTITION BY coin_id ORDER BY api_last_updated) AS previous_api_last_updated
    FROM
        continuous_usd_prices
    WHERE
    	api_last_updated >= NOW() - INTERVAL '24 hours'
)
SELECT 
    coin_id,
    MIN(EXTRACT(EPOCH FROM (api_last_updated - previous_api_last_updated))) AS min_gap_seconds,
    AVG(EXTRACT(EPOCH FROM (api_last_updated - previous_api_last_updated))) AS avg_gap_seconds,
    MAX(EXTRACT(EPOCH FROM (api_last_updated - previous_api_last_updated))) AS max_gap_seconds
FROM 
    gaps
WHERE
    previous_api_last_updated IS NOT NULL
GROUP BY 
    coin_id
ORDER BY 
    avg_gap_seconds DESC;