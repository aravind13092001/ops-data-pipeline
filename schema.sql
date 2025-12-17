-- Table to store the transformed data

CREATE TABLE IF NOT EXISTS crypto_market_data (
    coin_id VARCHAR(50) PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    name VARCHAR(100) NOT NULL,
    current_price_usd DECIMAL(18, 8),
    current_price_inr DECIMAL(18, 8), -- This is our "Transformed" field
    market_cap BIGINT,
    api_last_updated TIMESTAMP,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table to store logs 

CREATE TABLE IF NOT EXISTS pipeline_logs (
    id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) CHECK (status IN ('SUCCESS', 'FAILED')),
    records_processed INT DEFAULT 0,
    error_message TEXT
);