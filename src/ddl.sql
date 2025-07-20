CREATE TABLE stock_summary_1min (
    ticker VARCHAR(10) NOT NULL,
    summary_time TIMESTAMP NOT NULL,
    open_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    close_price NUMERIC,
    total_volume BIGINT,
    PRIMARY KEY (ticker, summary_time)
);