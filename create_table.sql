CREATE TABLE IF NOT EXISTS stock_prices (
  id SERIAL PRIMARY KEY,
  ticker VARCHAR(16) NOT NULL,
  ts TIMESTAMP NOT NULL,
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  volume BIGINT,
  created_at TIMESTAMP DEFAULT now(),
  UNIQUE (ticker, ts)
);
