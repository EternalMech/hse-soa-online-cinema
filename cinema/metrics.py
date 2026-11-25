import json
import logging
import time
from datetime import UTC, datetime, timedelta

import psycopg

from cinema.storage import BUCKET, SQL, ClickHouse, postgres, s3

log = logging.getLogger("uvicorn.error")
DAY = timedelta(days=1)
LOCK_NAMESPACE = 5025


class MissingMetrics(Exception):
    pass


def utc_today():
    return datetime.now(UTC).date()


def lock_date(pg, day):
    pg.execute("SELECT pg_advisory_xact_lock(%s, %s)", (LOCK_NAMESPACE, day.toordinal()))


def read_metrics(ch, day):
    as_of = utc_today() - DAY
    daily = ch.query((SQL / "daily.sql").read_text(), day=day)[0]
    top = ch.query((SQL / "top.sql").read_text(), day=day)
    cohort = ch.query((SQL / "retention.sql").read_text(), day=day, as_of=as_of)[0]
    started, finished = int(daily["started"]), int(daily["finished"])
    size = int(cohort["cohort_size"])
    metrics = [
        ("dau", "", int(daily["dau"])),
        ("avg_watch_seconds", "", daily["avg_watch_seconds"]),
        ("view_conversion", "", finished / started if started else None),
    ]
    metrics.extend(("top_movie", row["movie_id"], int(row["views"])) for row in top)
    metrics.append(("cohort_size", "", size))
    for age in range(8):
        is_observed = day + DAY * age <= as_of
        value = int(cohort["retained"][age]) / size if size and is_observed else None
        metrics.append((f"retention_d{age}", "", value))
    return int(daily["events"]), metrics


def save_snapshot(day, *, scheduled_on):
    with postgres() as pg, ClickHouse() as ch:
        lock_date(pg, day)
        events, metrics = read_metrics(ch, day)
        computed_at = pg.execute("SELECT clock_timestamp()").fetchone()[0]
        row = {
            "metric_date": day.isoformat(),
            "computed_at": computed_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f"),
            "metrics": [
                {"metric": metric, "dimension": dimension, "value": value}
                for metric, dimension, value in metrics
            ],
        }
        ch.execute("INSERT INTO cinema.daily_snapshots FORMAT JSONEachRow",
                   data=json.dumps(row, allow_nan=False).encode())
        pg.execute("DELETE FROM daily_metrics WHERE metric_date = %s", (day,))
        with pg.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO daily_metrics VALUES (%s, %s, %s, %s, %s)",
                [(day, metric, dimension, value, computed_at) for metric, dimension, value in metrics],
            )
        if scheduled_on is not None:
            updated = pg.execute(
                """INSERT INTO export_schedule VALUES (%s, %s)
                   ON CONFLICT (metric_date) DO UPDATE SET scheduled_on = EXCLUDED.scheduled_on
                   WHERE export_schedule.scheduled_on < EXCLUDED.scheduled_on
                   RETURNING metric_date""", (day, scheduled_on),
            ).fetchone()
            if updated:
                pg.execute("INSERT INTO pending_exports VALUES (%s) ON CONFLICT DO NOTHING", (day,))
    return events, len(metrics)


def recompute(day, *, scheduled_on=None):
    started_at = time.monotonic()
    log.info("aggregation_started date=%s", day)
    for attempt in range(1, 4):
        try:
            events, rows = save_snapshot(day, scheduled_on=scheduled_on)
            log.info("aggregation_completed date=%s events=%s metrics=%s elapsed_ms=%d",
                     day, events, rows, 1000 * (time.monotonic() - started_at))
            return {"date": day, "events": events, "metrics": rows}
        except (psycopg.OperationalError, psycopg.errors.SerializationFailure, psycopg.errors.DeadlockDetected) as exc:
            if attempt == 3:
                raise
            log.warning("postgres_retry date=%s attempt=%s error=%s", day, attempt, exc)
            time.sleep(0.25 * 2 ** (attempt - 1))


def export(day, *, request=False):
    if request:
        with postgres() as pg:
            lock_date(pg, day)
            if pg.execute("SELECT 1 FROM daily_metrics WHERE metric_date = %s LIMIT 1", (day,)).fetchone() is None:
                raise MissingMetrics
            pg.execute("INSERT INTO pending_exports VALUES (%s) ON CONFLICT DO NOTHING", (day,))
    with postgres() as pg:
        lock_date(pg, day)
        rows = pg.execute(
            """SELECT metric, dimension, value, computed_at FROM daily_metrics
               WHERE metric_date = %s ORDER BY metric, dimension""", (day,)
        ).fetchall()
        if not rows:
            raise MissingMetrics
        result = {
            "metric_date": day.isoformat(),
            "computed_at": rows[0][3].astimezone(UTC).isoformat(),
            "metrics": [
                {"metric": metric, "dimension": dimension, "value": value}
                for metric, dimension, value, _ in rows
            ],
        }
        key = f"daily/{day}/aggregates.json"
        with s3() as client:
            client.put_object(
                Bucket=BUCKET, Key=key, Body=json.dumps(result, allow_nan=False).encode(),
                ContentType="application/json",
            )
        pg.execute("DELETE FROM pending_exports WHERE metric_date = %s", (day,))
    log.info("s3_export_completed date=%s metrics=%s key=%s", day, len(rows), key)
    return {"date": day, "object": f"s3://{BUCKET}/{key}", "metrics": len(rows)}


def scheduler(stop, interval):
    while not stop.is_set():
        today = utc_today()
        for age in range(1, 9):
            if stop.is_set():
                return
            day = today - DAY * age
            try:
                recompute(day, scheduled_on=today if age in (1, 8) else None)
            except Exception:
                log.exception("aggregation_failed date=%s", day)
        try:
            with postgres() as pg:
                pending = pg.execute("SELECT metric_date FROM pending_exports ORDER BY metric_date").fetchall()
            for (day,) in pending:
                if stop.is_set():
                    return
                try:
                    export(day)
                except Exception:
                    log.exception("s3_export_failed date=%s; will retry next cycle", day)
        except Exception:
            log.exception("s3_export_queue_failed; will retry next cycle")
        stop.wait(interval)
