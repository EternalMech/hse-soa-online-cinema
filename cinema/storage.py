import json
import os
from contextlib import contextmanager
from pathlib import Path

import boto3
import httpx
import psycopg
from botocore.config import Config

TOPIC = "movie-events"
SUBJECT = f"{TOPIC}-value"
BUCKET = "movie-analytics"
BROKERS = os.environ.get("KAFKA_BOOTSTRAP", "kafka1:9092,kafka2:9092,kafka3:9092")
REGISTRY = os.environ.get("SCHEMA_REGISTRY", "http://registry:8081")
ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "schemas" / "movie-event-v1.avsc"
SQL = ROOT / "sql"


def postgres():
    return psycopg.connect(os.environ["POSTGRES_DSN"], connect_timeout=4)


@contextmanager
def s3():
    client = boto3.client(
        "s3", endpoint_url=os.environ["S3_ENDPOINT"],
        config=Config(connect_timeout=3, read_timeout=5, retries={"total_max_attempts": 1},
                      s3={"addressing_style": "path"}),
    )
    try:
        yield client
    finally:
        client.close()


class ClickHouse:
    def __init__(self):
        self.client = httpx.Client(
            base_url=os.environ["CLICKHOUSE_URL"],
            auth=(os.environ["CLICKHOUSE_USER"], os.environ["CLICKHOUSE_PASSWORD"]),
            timeout=httpx.Timeout(45, connect=4),
        )

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.client.close()

    def execute(self, sql, *, data=None, **parameters):
        params = {f"param_{key}": str(value) for key, value in parameters.items()}
        if data is None:
            response = self.client.post("/", params=params, content=sql.encode())
        else:
            response = self.client.post("/", params={**params, "query": sql}, content=data)
        if response.is_error:
            raise RuntimeError(f"ClickHouse {response.status_code}: {response.text.strip()}")
        return response.text

    def query(self, sql, **parameters):
        text = self.execute(sql + "\nFORMAT JSONEachRow", **parameters)
        return [json.loads(line) for line in text.splitlines() if line]
