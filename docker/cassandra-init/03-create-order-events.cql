CREATE TABLE IF NOT EXISTS user_events_ks.order_events (
    order_id TEXT PRIMARY KEY,
    user_id TEXT,
    user_name TEXT,
    email TEXT, 
    event_type TEXT,
    timestamp TIMESTAMP,
    product_id TEXT,
    product_name TEXT,
    quantity BIGINT,
    price BIGINT,
    total BIGINT,
    status TEXT
);