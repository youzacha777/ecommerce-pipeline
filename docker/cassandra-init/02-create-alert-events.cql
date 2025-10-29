CREATE TABLE IF NOT EXISTS user_events_ks.alert_events (
    event_id UUID PRIMARY KEY,
    user_id TEXT,
    user_name TEXT,
    email TEXT,      
    event_type TEXT,
    timestamp TIMESTAMP,
    note TEXT,
    keyword TEXT,
    products_displayed LIST<TEXT>,
    results_count BIGINT,
    current_page BIGINT,
    next_page BIGINT,
    alert_type TEXT
);
