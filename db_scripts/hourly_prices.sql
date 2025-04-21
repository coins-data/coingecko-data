CREATE VIEW coingecko.hourly_volume_estimates AS
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