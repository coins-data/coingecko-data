CREATE VIEW coingecko.recent_coin_prices AS
WITH latest_btc AS (
    SELECT
        coin_id,
        api_last_updated,
        created_at,
        price AS btc_price,
        vol_24h AS btc_vol_24h,
        high_24h AS btc_high_24h,
        low_24h AS btc_low_24h,
        price_change_percentage_24h AS btc_price_change_percentage_24h,
        ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY created_at DESC) AS btc_row_num
    FROM coingecko.continuous_btc_prices
),
latest_usd AS (
    SELECT
        coin_id,
        api_last_updated,
        created_at,
        price AS usd_price,
        vol_24h AS usd_vol_24h,
        high_24h AS usd_high_24h,
        low_24h AS usd_low_24h,
        price_change_percentage_24h AS usd_price_change_percentage_24h,
        ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY created_at DESC) AS usd_row_num
    FROM coingecko.continuous_usd_prices
)
SELECT
    c.id AS coin_id,
    UPPER(c.symbol) AS symbol,
    c.name,
    c.image_url,
    c.market_cap_usd,
    '$' || to_char(ROUND(c.market_cap_usd, 0), 'FM9,999,999,999,999,999,999') AS market_cap_usd_formatted,
    c.usd_stable_coin,
    c.wrapped_coin,
    lbtc.btc_price,
    '₿' || format_number(lbtc.btc_price::numeric) AS btc_price_formatted,
    lbtc.btc_vol_24h,
    lbtc.btc_high_24h,
    '₿' || format_number(lbtc.btc_high_24h::numeric) AS btc_high_24h_formatted,
    lbtc.btc_low_24h,
    '₿' || format_number(lbtc.btc_low_24h::numeric) AS btc_low_24h_formatted,
    lbtc.btc_price_change_percentage_24h,
    ROUND(lbtc.btc_price_change_percentage_24h::numeric, 2) AS btc_price_change_percentage_24h_rounded,
    lbtc.api_last_updated AS btc_api_last_updated,
    lbtc.created_at AS btc_last_checked_at,
    EXTRACT(EPOCH FROM (NOW() AT TIME ZONE 'UTC' - lbtc.created_at AT TIME ZONE 'UTC')) AS btc_seconds_since_last_check,
    lusd.usd_price,
    '$' || format_number(lusd.usd_price::numeric) AS usd_price_formatted,
    lusd.usd_vol_24h,
    '$' || to_char(ROUND(lusd.usd_vol_24h::numeric, 0), 'FM9,999,999,999,999,999,999') AS usd_vol_24h_formatted,
    lusd.usd_high_24h,
    '$' || format_number(lusd.usd_high_24h::numeric) AS usd_high_24h_formatted,
    lusd.usd_low_24h,
    '$' || format_number(lusd.usd_low_24h::numeric) AS usd_low_24h_formatted,
    lusd.usd_price_change_percentage_24h,
    ROUND(lusd.usd_price_change_percentage_24h::numeric, 2) AS usd_price_change_percentage_24h_rounded,
    lusd.api_last_updated AS usd_api_last_updated,
    lusd.created_at AT TIME ZONE 'UTC' AS usd_last_checked_at,
    EXTRACT(EPOCH FROM (NOW() AT TIME ZONE 'UTC' - lusd.created_at AT TIME ZONE 'UTC')) AS usd_seconds_since_last_check
FROM
    coingecko.coins c
    INNER JOIN latest_btc lbtc ON c.id = lbtc.coin_id AND lbtc.btc_row_num = 1
    INNER JOIN latest_usd lusd ON c.id = lusd.coin_id AND lusd.usd_row_num = 1
WHERE c.archived = FALSE
    AND c.track_prices = TRUE
;
