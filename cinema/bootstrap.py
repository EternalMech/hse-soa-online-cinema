import logging
import time

import httpx
import psycopg
from botocore.exceptions import BotoCoreError, ClientError
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

from cinema.storage import BROKERS, BUCKET, REGISTRY, SCHEMA_PATH, SQL, SUBJECT, TOPIC, ClickHouse, postgres, s3
log = logging.getLogger(__name__)


def setup_kafka():
    admin = AdminClient({"bootstrap.servers": BROKERS})
    topic = NewTopic(TOPIC, num_partitions=3, replication_factor=3, config={"min.insync.replicas": "2"})
    try:
        admin.create_topics([topic], operation_timeout=10, request_timeout=12)[TOPIC].result()
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
    partitions = admin.list_topics(topic=TOPIC, timeout=10).topics[TOPIC].partitions
    if len(partitions) < 3 or any(len(p.replicas) < 2 for p in partitions.values()):
        raise ValueError("Existing movie-events needs at least 3 partitions and 2 replicas each")
    resource = ConfigResource(ConfigResource.Type.TOPIC, TOPIC)
    config = admin.describe_configs([resource], request_timeout=10)[resource].result()
    if int(config["min.insync.replicas"].value) < 1:
        raise ValueError("Existing movie-events needs min.insync.replicas >= 1")
    with httpx.Client(base_url=REGISTRY, timeout=10) as client:
        config = client.put(f"/config/{SUBJECT}", json={"compatibility": "BACKWARD"})
        config.raise_for_status()
        schema = {"schema": SCHEMA_PATH.read_text(), "schemaType": "AVRO"}
        registration = client.post(f"/subjects/{SUBJECT}/versions", json=schema)
        registration.raise_for_status()
        version = client.post(f"/subjects/{SUBJECT}", json=schema)
        version.raise_for_status()
        log.info("schema_registered subject=%s version=%s id=%s", SUBJECT, version.json()["version"], version.json()["id"])


def setup_storage():
    with ClickHouse() as ch:
        for statement in (SQL / "clickhouse.sql").read_text().split(";"):
            if statement.strip():
                ch.execute(statement)
    with postgres() as pg:
        pg.execute((SQL / "postgres.sql").read_text())
    with s3() as client:
        if BUCKET not in {bucket["Name"] for bucket in client.list_buckets()["Buckets"]}:
            client.create_bucket(Bucket=BUCKET)


def main():
    logging.basicConfig(level=logging.INFO)
    for attempt in range(1, 5):
        try:
            setup_kafka()
            setup_storage()
            log.info("bootstrap_complete")
            return
        except (ValueError, httpx.HTTPError, KafkaException, RuntimeError, OSError,
                psycopg.OperationalError, BotoCoreError, ClientError) as exc:
            if attempt == 4:
                raise
            log.warning("bootstrap_retry attempt=%s error=%s", attempt, exc)
            time.sleep(2 ** attempt)


if __name__ == "__main__":
    main()
