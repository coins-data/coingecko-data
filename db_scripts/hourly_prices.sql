-- Hourly USD Prices Only
SELECT
    coin_id,
    date_trunc('hour', created_at) AS hour,
    AVG(price) AS avg_price,
    MIN(price) AS low,
    MAX(price) AS high,
    COUNT(*) AS count,
    (array_agg(price ORDER BY created_at))[1] AS open,
    (array_agg(price ORDER BY created_at DESC))[1] AS close
FROM
    coingecko.continuous_usd_prices
GROUP BY 
    coin_id, 
    date_trunc('hour', created_at)
ORDER BY 
    coin_id, 
    hour;    


-- Hourly USD Prices and Volume
WITH HourlyOHLCAndCount AS (
    -- Calculate Open, High, Low, Close, Simple Average Price (for TWAP), and Price Point Count per hour
    SELECT
        coin_id,
        date_trunc('hour', created_at) AS hour,
        AVG(price) AS simple_avg_price, 
        MIN(price) AS low_price,
        MAX(price) AS high_price,
        (array_agg(price ORDER BY created_at ASC))[1] AS open_price,
        (array_agg(price ORDER BY created_at DESC))[1] AS close_price,
        COUNT(*) AS price_snapshots 
    FROM
        coingecko.continuous_usd_prices
    GROUP BY
        coin_id,
        date_trunc('hour', created_at)
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
        -- Use midpoint price as approximation for interval VWAP calculation
        (price_start + price_end) / 2.0 AS avg_interval_price,
        EXTRACT(EPOCH FROM (ts_end - ts_start)) AS interval_duration_seconds
    FROM
        LaggedData
    WHERE
        ts_start IS NOT NULL
        AND ts_end > ts_start
        AND price_start IS NOT NULL -- Ensure we have prices for VWAP calculation
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
        GREATEST(0.0, -- Ensure non-negative volume
            (id.raw_delta_vol_24h + (id.vol_24h_start_safe / 24.0) * (id.interval_duration_seconds / 3600.0))
        ) AS estimated_interval_volume
    FROM
        IntervalData id
    WHERE
        id.interval_duration_seconds > 0
),
HourlyBuckets AS (
    -- Generate all hourly buckets that each interval potentially overlaps with
    SELECT
        eiv.coin_id,
        eiv.ts_start,
        eiv.ts_end,
        eiv.interval_duration_seconds,
        eiv.avg_interval_price,
        eiv.estimated_interval_volume,
        generate_series(
            date_trunc('hour', eiv.ts_start),
            date_trunc('hour', eiv.ts_end),
            interval '1 hour'
        ) AS hour_start
    FROM
        EstimatedIntervalVolume eiv
),
OverlapCalculation AS (
    -- Calculate the actual overlap duration and portion for each interval with each hour it touches
    SELECT
        h.coin_id,
        h.hour_start,
        h.interval_duration_seconds,
        h.avg_interval_price,
        h.estimated_interval_volume,
        GREATEST(h.ts_start, h.hour_start) AS overlap_start,
        LEAST(h.ts_end, (h.hour_start + interval '1 hour')) AS overlap_end
    FROM
        HourlyBuckets h
),
FinalHourlyDistribution AS (
    -- Calculate the overlap duration and distribute interval volume and volume*price product
    SELECT
        oc.coin_id,
        oc.hour_start,
        oc.estimated_interval_volume,
        oc.avg_interval_price,
        -- Calculate portion of interval duration that overlaps with this hour
        GREATEST(0.0, EXTRACT(EPOCH FROM (oc.overlap_end - oc.overlap_start))) / NULLIF(oc.interval_duration_seconds, 0) AS overlap_fraction
    FROM OverlapCalculation oc
    WHERE oc.overlap_start < oc.overlap_end -- Ensure valid overlap
      AND oc.interval_duration_seconds > 0 -- Ensure valid interval for division
),
HourlyVolumeAndVWAPComponents AS (
    -- Aggregate distributed volume and volume*price product per hour
    SELECT
        fh.coin_id,
        fh.hour_start AS hour,
        -- Sum the estimated volume attributed to this hour from all overlapping intervals
        SUM(
            fh.estimated_interval_volume * fh.overlap_fraction
        ) AS estimated_hourly_volume,
        -- Sum the estimated (volume * avg_interval_price) attributed to this hour
        SUM(
            fh.estimated_interval_volume * fh.avg_interval_price * fh.overlap_fraction
        ) AS estimated_hourly_volume_price_product
    FROM
        FinalHourlyDistribution fh
    GROUP BY
        fh.coin_id,
        fh.hour_start
)
-- Final Select: Join OHLC/Count data with Volume/VWAP data
SELECT
    ohlc.coin_id,
    ohlc.hour,
    -- Calculate final VWAP: SUM(volume*price) / SUM(volume) for the hour
    CASE
        WHEN COALESCE(vol.estimated_hourly_volume, 0) = 0 THEN ohlc.simple_avg_price -- Fallback if volume is 0
        ELSE vol.estimated_hourly_volume_price_product / NULLIF(vol.estimated_hourly_volume, 0) -- Use NULLIF for safety
    END AS vwap,
    -- Use simple average price from OHLC query as TWAP proxy
    ohlc.simple_avg_price AS twap,
    ohlc.open_price AS open,
    ohlc.high_price AS high,
    ohlc.low_price AS low,
    ohlc.close_price AS close,
    -- Use the estimated hourly volume, cast to BIGINT, default to 0
    COALESCE(CAST(vol.estimated_hourly_volume AS BIGINT), 0) AS volume,
    -- Include the count of price points used for OHLC calculation
    ohlc.price_snapshots
FROM
    HourlyOHLCAndCount ohlc
LEFT JOIN -- Use LEFT JOIN to keep all hours with price data
    HourlyVolumeAndVWAPComponents vol ON ohlc.coin_id = vol.coin_id AND ohlc.hour = vol.hour
ORDER BY
    ohlc.coin_id,
    ohlc.hour;


-- Hourly USD Volume with extra columns for checks and debugging
WITH LaggedData AS (
    -- Get previous data point for each coin to define intervals
    SELECT
        coin_id,
        api_last_updated AS ts_end, -- Current timestamp (end of interval)
        created_at, -- Keep created_at if needed for other checks
        vol_24h AS vol_24h_end,
        price AS price_end, -- Include price for context if desired
        LAG(api_last_updated, 1) OVER (PARTITION BY coin_id ORDER BY api_last_updated) AS ts_start, -- Previous timestamp (start of interval)
        LAG(vol_24h, 1) OVER (PARTITION BY coin_id ORDER BY api_last_updated) AS vol_24h_start, -- Volume at the start of the interval
        LAG(price, 1) OVER (PARTITION BY coin_id ORDER BY api_last_updated) AS price_start -- Price at start
    FROM
        coingecko.continuous_usd_prices
),
IntervalData AS (
    -- Calculate interval properties: duration, delta volume, start rate, and raw delta
    SELECT
        coin_id,
        ts_start,
        ts_end,
        vol_24h_end,
        COALESCE(vol_24h_start, 0) AS vol_24h_start_safe, -- Handle null for the first entry
        (vol_24h_end - COALESCE(vol_24h_start, 0)) AS raw_delta_vol_24h, -- Raw change in 24h volume
        price_start,
        price_end,
        -- Calculate interval duration precisely in seconds
        EXTRACT(EPOCH FROM (ts_end - ts_start)) AS interval_duration_seconds,
        -- Calculate the implied hourly rate at the START of the interval for TWAR method
        (COALESCE(vol_24h_start, 0) / 24.0) AS hourly_rate_at_interval_start
    FROM
        LaggedData
    WHERE
        ts_start IS NOT NULL -- Need a previous point to form an interval
        AND ts_end > ts_start -- Ensure positive duration
),
EstimatedIntervalVolumeDiffMethod AS (
    -- Apply the primary (difference-based) estimation formula and calculate components
    SELECT
        id.*, -- Select all columns from IntervalData
        -- Calculate the estimated volume dropped off the back of the 24h window
        (id.vol_24h_start_safe / 24.0) * (id.interval_duration_seconds / 3600.0) AS estimated_volume_dropped_off,
        -- Calculate the total estimated volume for the interval using the difference method
        (id.raw_delta_vol_24h + (id.vol_24h_start_safe / 24.0) * (id.interval_duration_seconds / 3600.0)) AS preliminary_estimated_volume,
        -- Apply GREATEST(0, ...) and flag if it was triggered
        CASE
            WHEN (id.raw_delta_vol_24h + (id.vol_24h_start_safe / 24.0) * (id.interval_duration_seconds / 3600.0)) < 0 THEN 1
            ELSE 0
        END AS was_volume_zeroed_out,
        GREATEST(0.0,
            (id.raw_delta_vol_24h + (id.vol_24h_start_safe / 24.0) * (id.interval_duration_seconds / 3600.0))
        ) AS estimated_total_volume_diff_method
    FROM
        IntervalData id
    WHERE
        id.interval_duration_seconds > 0 -- Ensure positive duration after calculation
),
-- Generate all hourly buckets that each interval potentially overlaps with
HourlyBuckets AS (
    SELECT
        eiv.*, -- Select all columns from EstimatedIntervalVolumeDiffMethod
        -- Generate a series of timestamps, one for the start of each hour the interval touches
        generate_series(
            date_trunc('hour', eiv.ts_start),
            date_trunc('hour', eiv.ts_end),
            interval '1 hour'
        ) AS hour_start
    FROM
        EstimatedIntervalVolumeDiffMethod eiv
),
-- Calculate the actual overlap duration for each interval with each hour it touches
OverlapCalculation AS (
    SELECT
        h.*, -- Select all columns from HourlyBuckets
        -- Calculate the end of the current hour bucket
        (h.hour_start + interval '1 hour') AS hour_end,
        -- Determine the intersection points
        GREATEST(h.ts_start, h.hour_start) AS overlap_start,
        LEAST(h.ts_end, (h.hour_start + interval '1 hour')) AS overlap_end
    FROM
        HourlyBuckets h
),
-- Calculate the volume contribution for EACH method based on the overlap duration
FinalHourlyDistribution AS (
    SELECT
        oc.*, -- Select all columns from OverlapCalculation
        -- Calculate overlap duration in seconds, ensuring it's non-negative
        GREATEST(0.0, EXTRACT(EPOCH FROM (overlap_end - overlap_start))) AS overlap_duration_seconds
    FROM OverlapCalculation oc
    -- Filter out cases where the calculated overlap start is not before the end
    WHERE overlap_start < overlap_end
)
-- Final Aggregation: Sum the distributed volume portions and add debug columns
SELECT
    fh.coin_id,
    fh.hour_start AS hour_bucket, -- Alias defined here

    -- --- Core Estimated Volume Columns ---
    SUM(
        -- Method 1: Distribute volume calculated using the difference method
        fh.estimated_total_volume_diff_method * (fh.overlap_duration_seconds / NULLIF(fh.interval_duration_seconds, 0))
    ) AS estimated_hourly_volume_diff_method,
    SUM(
        -- Method 2: Distribute volume calculated using the Time-Weighted Average Rate (TWAR) method
        fh.hourly_rate_at_interval_start * (fh.overlap_duration_seconds / 3600.0)
    ) AS estimated_hourly_volume_twar_method,

    -- --- Debugging / Sanity Check Columns ---
    COUNT(*) AS contributing_interval_overlaps, -- How many interval parts contributed to this hour?
    SUM(fh.overlap_duration_seconds) AS total_overlap_seconds_in_hour, -- Should ideally be close to 3600 if data is continuous
    AVG(fh.interval_duration_seconds) AS avg_contributing_interval_duration_sec,
    MIN(fh.interval_duration_seconds) AS min_contributing_interval_duration_sec,
    MAX(fh.interval_duration_seconds) AS max_contributing_interval_duration_sec,
    MIN(fh.ts_start) AS first_contributing_interval_start, -- Start time of the earliest interval touching this hour
    MAX(fh.ts_end) AS last_contributing_interval_end,     -- End time of the latest interval touching this hour
    SUM(fh.was_volume_zeroed_out) AS count_intervals_zeroed_out, -- How many contributing intervals had their diff method volume forced to 0?

    -- Sum of components for the difference method (distributed proportionally)
    SUM(
        fh.raw_delta_vol_24h * (fh.overlap_duration_seconds / NULLIF(fh.interval_duration_seconds, 0))
    ) AS sum_distributed_raw_delta_vol_24h,
    SUM(
        fh.estimated_volume_dropped_off * (fh.overlap_duration_seconds / NULLIF(fh.interval_duration_seconds, 0))
    ) AS sum_distributed_estimated_dropped_off_volume,

    -- Range of TWAR rates used within the hour
    MIN(fh.hourly_rate_at_interval_start) AS min_twar_hourly_rate_in_hour,
    MAX(fh.hourly_rate_at_interval_start) AS max_twar_hourly_rate_in_hour,
    AVG(fh.hourly_rate_at_interval_start) AS avg_twar_hourly_rate_in_hour,

    -- Optional: Price context (average weighted by overlap duration)
    SUM(fh.price_start * fh.overlap_duration_seconds) / NULLIF(SUM(fh.overlap_duration_seconds), 0) AS avg_price_at_interval_start_weighted,
    SUM(fh.price_end * fh.overlap_duration_seconds) / NULLIF(SUM(fh.overlap_duration_seconds), 0) AS avg_price_at_interval_end_weighted

FROM
    FinalHourlyDistribution fh
WHERE
    fh.interval_duration_seconds > 0 -- Ensure interval duration is positive before division/distribution
GROUP BY
    fh.coin_id,
    fh.hour_start -- Use the original column name here
ORDER BY
    fh.coin_id,
    fh.hour_start; -- Use the original column name here