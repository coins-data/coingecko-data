CREATE TABLE coins (
    id VARCHAR(255) PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    website VARCHAR(500),
    image_url VARCHAR(500),
    market_cap_rank INT,
    market_cap_usd BIGINT,
    fully_diluted_valuation BIGINT,
    total_supply NUMERIC(32,0),
    max_supply NUMERIC(32,0),
    circulating_supply NUMERIC(32,0),
    update_hourly BOOLEAN NOT NULL DEFAULT FALSE,
    track_prices BOOLEAN NOT NULL DEFAULT FALSE,
    usd_stable_coin BOOLEAN NOT NULL DEFAULT FALSE,
    wrapped_coin BOOLEAN NOT NULL DEFAULT FALSE,
    archived BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP NOT NULL DEFAULT '0001-01-01 00:00:00'
);

CREATE TABLE continuous_btc_prices (
    coin_id VARCHAR(255) NOT NULL,
    api_last_updated TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    vol_24h INT,
    high_24h DOUBLE PRECISION,
    low_24h DOUBLE PRECISION,
    price_change_percentage_24h REAL
);

CREATE TABLE continuous_usd_prices (
    coin_id VARCHAR(255) NOT NULL,
    api_last_updated TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    vol_24h BIGINT,
    high_24h DOUBLE PRECISION,
    low_24h DOUBLE PRECISION,
    price_change_percentage_24h REAL
);



