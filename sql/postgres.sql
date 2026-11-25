CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_date date NOT NULL,
    metric text NOT NULL,
    dimension text NOT NULL DEFAULT '',
    value double precision,
    computed_at timestamptz NOT NULL,
    PRIMARY KEY (metric_date, metric, dimension)
);

CREATE TABLE IF NOT EXISTS pending_exports (
    metric_date date PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS export_schedule (
    metric_date date PRIMARY KEY,
    scheduled_on date NOT NULL
);
