import json
import time
from datetime import UTC, date, datetime, timedelta
from random import SystemRandom
from uuid import uuid4

import httpx
from confluent_kafka.admin import AdminClient, ConfigResource

from cinema.storage import BROKERS, BUCKET, REGISTRY, SCHEMA_PATH, SUBJECT, TOPIC, ClickHouse, postgres, s3


def verify_kafka():
    client = AdminClient({"bootstrap.servers": BROKERS})
    partitions = client.list_topics(topic=TOPIC, timeout=6).topics[TOPIC].partitions
    assert len(partitions) >= 3
    assert all(len(p.replicas) >= 2 for p in partitions.values())
    topic = ConfigResource(ConfigResource.Type.TOPIC, TOPIC)
    config = client.describe_configs([topic], request_timeout=8)[topic].result()
    assert int(config["min.insync.replicas"].value) >= 1
    response = httpx.get(f"{REGISTRY}/subjects/{SUBJECT}/versions/1", timeout=6).raise_for_status().json()
    assert json.loads(response["schema"]) == json.loads(SCHEMA_PATH.read_text())


def publish_events(producer, ch):
    start_day = date(2000, 1, 1) + timedelta(days=SystemRandom().randrange(0, 6800))
    while ch.query("SELECT event_id FROM cinema.movie_events_raw WHERE event_date BETWEEN {day:Date} AND addDays({day:Date},7) LIMIT 1", day=start_day):
        start_day += timedelta(days=8)
    tag = uuid4().hex
    expected = {}

    def publish(user, kind, progress, *, age=0, movie="alpha", seconds=0, duplicate=False):
        stamp = datetime.combine(start_day + timedelta(days=age), datetime.min.time(), UTC)
        stamp += timedelta(hours=10, seconds=seconds, milliseconds=125)
        event = {
            "event_id": str(uuid4()), "user_id": tag + user, "movie_id": tag + movie,
            "event_type": kind, "timestamp": stamp.isoformat(timespec="milliseconds"),
            "device_type": "TV", "session_id": f"{tag}-{user}-{age}", "progress_seconds": progress,
        }
        for _ in range(2 if duplicate else 1):
            response = producer.post("/events", json=event).raise_for_status()
            assert response.status_code == 201 and response.json() == {"event_id": event["event_id"]}
        expected[event["event_id"]] = dict(event, timestamp=stamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])

    bad = {"event_id": "invalid"}
    assert producer.post("/events", json=bad).status_code == 422
    publish("a", "VIEW_STARTED", 0, duplicate=True)
    publish("a", "VIEW_FINISHED", 100, seconds=105)
    publish("b", "VIEW_STARTED", 0)
    publish("b", "VIEW_FINISHED", 300, seconds=305)
    publish("c", "SEARCHED", None)
    publish("d", "VIEW_STARTED", 0, movie="beta")
    publish("a", "VIEW_STARTED", 0, age=1)
    publish("a", "VIEW_STARTED", 0, age=7)
    publish("b", "VIEW_STARTED", 0, age=7)
    end = time.monotonic() + 45
    while time.monotonic() < end:
        rows = ch.query(
            """SELECT event_id, user_id, movie_id, event_type, timestamp, device_type, session_id, progress_seconds
               FROM cinema.movie_events_raw FINAL
               WHERE event_date BETWEEN {day:Date} AND addDays({day:Date},7) AND startsWith(user_id,{tag:String})""",
            day=start_day, tag=tag,
        )
        if len(rows) == len(expected):
            assert {row["event_id"]: row for row in rows} == expected, rows
            print(f"HTTP → Kafka Avro → ClickHouse OK: {len(rows)} exact events")
            return start_day, tag
        time.sleep(0.5)
    raise AssertionError(f"Only {len(rows)} of {len(expected)} events arrived in ClickHouse")


def verify_analytics(aggregation, ch, day, tag):
    wanted = {
        ("dau", ""): 4, ("avg_watch_seconds", ""): 200, ("view_conversion", ""): 2/3,
        ("top_movie", tag+"alpha"): 2, ("top_movie", tag+"beta"): 1, ("cohort_size", ""): 3,
        **{(f"retention_d{age}", ""): {0: 1, 1: 1/3, 7: 2/3}.get(age, 0) for age in range(8)},
    }
    for _ in range(2):
        body = aggregation.post(f"/recompute/{day}").raise_for_status().json()
        assert body["metrics"] == len(wanted) and body["events"] == 6, body
    with postgres() as pg:
        rows = pg.execute(
            "SELECT metric, dimension, value, computed_at FROM daily_metrics WHERE metric_date = %s", (day,)
        ).fetchall()
    assert len(rows) == len(wanted) and {(m, d): v for m, d, v, _ in rows} == wanted
    snapshots = ch.query(
        "SELECT metric, dimension, value FROM cinema.daily_metrics WHERE metric_date = {day:Date}", day=day
    )
    assert len(snapshots) == len(wanted)
    assert {(r["metric"], r["dimension"]): r["value"] for r in snapshots} == wanted
    for _ in range(2):
        aggregation.post(f"/export/{day}").raise_for_status()
    key = f"daily/{day}/aggregates.json"
    with s3() as client:
        data = json.load(client.get_object(Bucket=BUCKET, Key=key)["Body"])
        objects = client.list_objects_v2(Bucket=BUCKET, Prefix=f"daily/{day}/")["Contents"]
    assert [obj["Key"] for obj in objects] == [key]
    assert data["metric_date"] == day.isoformat()
    assert datetime.fromisoformat(data["computed_at"]) == rows[0][3]
    assert {(r["metric"], r["dimension"]): r["value"] for r in data["metrics"]} == wanted
    print("Metrics, retention, idempotent recomputation and S3 export OK")


def verify_grafana(day):
    with httpx.Client(base_url="http://grafana:3000", auth=("admin", "admin"), timeout=10) as grafana:
        dashboard = grafana.get("/api/dashboards/uid/online-cinema").raise_for_status().json()["dashboard"]
        panels = dashboard["panels"]
        assert len(panels) >= 3 and panels[0]["title"] == "Retention Cohort Heatmap"
        assert all("cinema.daily_metrics" in p["targets"][0]["rawSql"] for p in panels)
        query = dict(panels[0]["targets"][0])
        query["rawSql"] = query["rawSql"].replace("$__dateFilter(metric_date)", f"metric_date = toDate('{day}')")
        result = grafana.post("/api/ds/query", json={"queries": [query], "from": "now-30d", "to": "now"})
        frame = result.raise_for_status().json()["results"]["A"]["frames"][0]
        assert frame["data"]["values"][0] == [day.isoformat()], frame
        assert frame["data"]["values"][-1] == [2/3], frame
    print("Grafana datasource and cohort query OK")


def main():
    verify_kafka()
    with ClickHouse() as ch, httpx.Client(base_url="http://producer:8000", timeout=20) as producer, httpx.Client(
        base_url="http://aggregation:8000", timeout=90,
    ) as aggregation:
        day, tag = publish_events(producer, ch)
        verify_analytics(aggregation, ch, day, tag)
        verify_grafana(day)


if __name__ == "__main__":
    main()
