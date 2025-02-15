CREATE TABLE coins (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    ticker VARCHAR(15) NOT NULL,
    website VARCHAR(255),
    track_hourly BOOLEAN DEFAULT FALSE,
    total_supply BIGINT,
    max_supply BIGINT,
    circulating_supply BIGINT
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE continuous_btc_prices (
    coin_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    vol_24h INT,
    change_24h REAL,
    PRIMARY KEY (coin_id, timestamp),
    FOREIGN KEY (coin_id) REFERENCES coins(id)
);

CREATE TABLE continuous_usd_prices (
    coin_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    vol_24h BIGINT,
    change_24h REAL,
    PRIMARY KEY (coin_id, timestamp),
    FOREIGN KEY (coin_id) REFERENCES coins(id)
);



