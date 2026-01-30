CREATE TABLE IF NOT EXISTS orders (
    order_id        TEXT PRIMARY KEY,
    status          TEXT NOT NULL,
    date            TIMESTAMP NOT NULL,
    amount          REAL NOT NULL,
    customer_region TEXT NOT NULL,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
