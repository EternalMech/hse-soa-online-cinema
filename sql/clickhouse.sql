CREATE TABLE IF NOT EXISTS cinema.movie_events_raw
(
    event_id UUID,
    user_id String,
    movie_id LowCardinality(String),
    event_type LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    device_type LowCardinality(String),
    session_id String,
    progress_seconds Nullable(Int32),
    event_date Date MATERIALIZED toDate(timestamp)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_id);

CREATE TABLE IF NOT EXISTS cinema.movie_events_kafka
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3,
                     'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id String,
    progress_seconds Nullable(Int32)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'cinema-clickhouse-v1',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://registry:8081',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 500;

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.ingest_movie_events
TO cinema.movie_events_raw
AS SELECT event_id, user_id, movie_id, toString(event_type) AS event_type, timestamp,
          toString(device_type) AS device_type, session_id, progress_seconds
FROM cinema.movie_events_kafka;

CREATE TABLE IF NOT EXISTS cinema.daily_snapshots
(
    metric_date Date,
    computed_at DateTime64(6, 'UTC'),
    metrics Array(Tuple(metric String, dimension String, value Nullable(Float64)))
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY metric_date;

CREATE VIEW IF NOT EXISTS cinema.daily_metrics
DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
SELECT metric_date, computed_at, entry.metric AS metric, entry.dimension AS dimension, entry.value AS value
FROM cinema.daily_snapshots FINAL
ARRAY JOIN metrics AS entry;

CREATE USER IF NOT EXISTS grafana IDENTIFIED WITH sha256_password BY 'grafana-local';
GRANT SELECT ON cinema.daily_metrics TO grafana;
