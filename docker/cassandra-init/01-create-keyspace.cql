CREATE KEYSPACE IF NOT EXISTS user_events_ks
WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};