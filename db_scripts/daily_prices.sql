INSERT INTO daily_usd_prices (
    coin_id,
    day,
    open, 
    high, 
    low,  
    close, 
    vwap,
    twap,
    volume,
    price_snapshots
)
WITH DailyOHLCAndCount AS (
    -- Calculate Open, High, Low, Close, Simple Average Price (for TWAP), and Price Point Count per day (UTC based)
    SELECT
        coin_id,
        date_trunc('day', created_at AT TIME ZONE 'UTC') AS day_ts, -- Timestamp for the start of the UTC day
        AVG(price) AS simple_avg_price, -- This will be used for TWAP
        MIN(price) AS low_val,          -- Renamed for clarity
        MAX(price) AS high_val,         -- Renamed for clarity
        (array_agg(price ORDER BY created_at ASC))[1] AS open_val, -- Renamed for clarity
        (array_agg(price ORDER BY created_at DESC))[1] AS close_val, -- Renamed for clarity
        COUNT(*) AS price_snapshots_val
    FROM
        coingecko.continuous_usd_prices
    GROUP BY
        coin_id,
        date_trunc('day', created_at AT TIME ZONE 'UTC')
),
LaggedData AS (
    -- Get previous data point for each coin to define intervals
    SELECT
        coin_id,
        created_at AS ts_end,
        vol_24h AS vol_24h_end,
        price AS price_end,
        LAG(created_at, 1) OVER (PARTITION BY coin_id ORDER BY created_at) AS ts_start,
        LAG(vol_24h, 1) OVER (PARTITION BY coin_id ORDER BY created_at) AS vol_24h_start,
        LAG(price, 1) OVER (PARTITION BY coin_id ORDER BY created_at) AS price_start
    FROM
        coingecko.continuous_usd_prices
),
IntervalData AS (
    -- Calculate interval properties needed for volume and VWAP estimation
    SELECT
        coin_id,
        ts_start,
        ts_end,
        COALESCE(vol_24h_start, 0) AS vol_24h_start_safe,
        (vol_24h_end - COALESCE(vol_24h_start, 0)) AS raw_delta_vol_24h,
        (price_start + price_end) / 2.0 AS avg_interval_price,
        EXTRACT(EPOCH FROM (ts_end - ts_start)) AS interval_duration_seconds
    FROM
        LaggedData
    WHERE
        ts_start IS NOT NULL
        AND ts_end > ts_start
        AND price_start IS NOT NULL
        AND price_end IS NOT NULL
),
EstimatedIntervalVolume AS (
    -- Estimate volume within each interval using the difference method
    SELECT
        id.coin_id,
        id.ts_start,
        id.ts_end,
        id.interval_duration_seconds,
        id.avg_interval_price,
        GREATEST(0.0,
            (id.raw_delta_vol_24h + (id.vol_24h_start_safe / 24.0) * (id.interval_duration_seconds / 3600.0))
        ) AS estimated_interval_volume
    FROM
        IntervalData id
    WHERE
        id.interval_duration_seconds > 0
),
DailyBuckets AS (
    -- Generate all daily buckets (UTC based) that each interval potentially overlaps with
    SELECT
        eiv.coin_id,
        eiv.ts_start,
        eiv.ts_end,
        eiv.interval_duration_seconds,
        eiv.avg_interval_price,
        eiv.estimated_interval_volume,
        generate_series(
            date_trunc('day', eiv.ts_start AT TIME ZONE 'UTC'),
            date_trunc('day', eiv.ts_end AT TIME ZONE 'UTC'),
            interval '1 day'
        ) AS day_start_ts -- Timestamp for the start of the UTC day bucket
    FROM
        EstimatedIntervalVolume eiv
),
OverlapCalculation AS (
    -- Calculate the actual overlap duration and portion for each interval with each day it touches
    SELECT
        d.coin_id,
        d.day_start_ts,
        d.interval_duration_seconds,
        d.avg_interval_price,
        d.estimated_interval_volume,
        GREATEST(d.ts_start, d.day_start_ts) AS overlap_start,
        LEAST(d.ts_end, (d.day_start_ts + interval '1 day')) AS overlap_end
    FROM
        DailyBuckets d
),
FinalDailyDistribution AS (
    -- Calculate the overlap duration and distribute interval volume and volume*price product
    SELECT
        oc.coin_id,
        oc.day_start_ts,
        oc.estimated_interval_volume,
        oc.avg_interval_price,
        GREATEST(0.0, EXTRACT(EPOCH FROM (oc.overlap_end - oc.overlap_start))) / NULLIF(oc.interval_duration_seconds, 0) AS overlap_fraction
    FROM OverlapCalculation oc
    WHERE oc.overlap_start < oc.overlap_end
      AND oc.interval_duration_seconds > 0
),
DailyVolumeAndVWAPComponents AS (
    -- Aggregate distributed volume and volume*price product per day
    SELECT
        fd.coin_id,
        fd.day_start_ts AS day_ts, -- Timestamp for the start of the UTC day
        SUM(
            fd.estimated_interval_volume * fd.overlap_fraction
        ) AS estimated_daily_volume, -- This will be used for the 'volume' column
        SUM(
            fd.estimated_interval_volume * fd.avg_interval_price * fd.overlap_fraction
        ) AS estimated_daily_volume_price_product
    FROM
        FinalDailyDistribution fd
    GROUP BY
        fd.coin_id,
        fd.day_start_ts
)
-- Final Select for Insertion: Join OHLC/Count data with Volume/VWAP data
SELECT
    ohlc.coin_id,
    ohlc.day_ts::DATE AS day,
    ohlc.open_val AS open,
    ohlc.high_val AS high,
    ohlc.low_val AS low,
    ohlc.close_val AS close,
    CASE
        WHEN COALESCE(vol.estimated_daily_volume, 0) = 0 THEN ohlc.simple_avg_price -- Fallback if volume is 0 for VWAP
        ELSE vol.estimated_daily_volume_price_product / NULLIF(vol.estimated_daily_volume, 0)
    END AS vwap,
    ohlc.simple_avg_price AS twap,
    COALESCE(CAST(vol.estimated_daily_volume AS BIGINT), 0) AS volume, -- Added volume column
    ohlc.price_snapshots_val AS price_snapshots
FROM
    DailyOHLCAndCount ohlc
LEFT JOIN
    DailyVolumeAndVWAPComponents vol ON ohlc.coin_id = vol.coin_id AND ohlc.day_ts = vol.day_ts
ON CONFLICT (coin_id, day) DO NOTHING;