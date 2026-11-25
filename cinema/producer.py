import json
import logging
import os
import struct
from concurrent.futures import Future
from contextlib import asynccontextmanager
from io import BytesIO
from threading import Event as StopEvent, Thread

import httpx
from confluent_kafka import KafkaException, Producer
from fastapi import FastAPI, HTTPException
from fastavro import parse_schema, schemaless_writer

from cinema.events import Event
from cinema.storage import BROKERS, REGISTRY, SCHEMA_PATH, SUBJECT, TOPIC

log = logging.getLogger("uvicorn.error")


class Publisher:
    def __init__(self):
        schema_text = SCHEMA_PATH.read_text()
        response = httpx.post(f"{REGISTRY}/subjects/{SUBJECT}", json={"schema": schema_text}, timeout=5)
        response.raise_for_status()
        schema_id = response.json()["id"]
        self.header = struct.pack(">BI", 0, schema_id)
        self.schema = parse_schema(json.loads(schema_text))
        self.client = Producer({
            "bootstrap.servers": BROKERS,
            "acks": "all",
            "enable.idempotence": True,
            "retry.backoff.ms": 100,
            "retry.backoff.max.ms": 2000,
            "delivery.timeout.ms": 12000,
            "request.timeout.ms": 4000,
            "partitioner": "murmur2_random",
        })
        self.stop = StopEvent()
        self.poller = Thread(target=self._poll, daemon=True)
        self.poller.start()

    def _poll(self):
        while not self.stop.is_set():
            self.client.poll(0.1)

    def publish(self, event):
        buffer = BytesIO(self.header)
        buffer.seek(len(self.header))
        schemaless_writer(buffer, self.schema, event.model_dump(mode="python"), strict=True)
        delivery = Future()

        def completed(error, message):
            if error:
                delivery.set_exception(KafkaException(error))
            else:
                delivery.set_result(message)

        self.client.produce(TOPIC, key=event.user_id.encode(), value=buffer.getvalue(), on_delivery=completed)
        message = delivery.result(timeout=15)
        log.info("published event_id=%s event_type=%s timestamp=%s partition=%s offset=%s",
                 event.event_id, event.event_type, event.timestamp.isoformat(), message.partition(), message.offset())

    def close(self):
        self.stop.set()
        self.poller.join()
        self.client.flush(3)


@asynccontextmanager
async def lifespan(app):
    from cinema.generator import generate_forever

    publisher = Publisher()
    app.state.publisher = publisher
    stop = StopEvent()
    generator = None
    try:
        interval = float(os.environ.get("GENERATOR_INTERVAL_SECONDS", "0"))
        if interval > 0:
            generator = Thread(target=generate_forever, args=(publisher.publish, interval, stop), daemon=True)
            generator.start()
        yield
    finally:
        stop.set()
        if generator:
            generator.join(timeout=16)
        publisher.close()


app = FastAPI(title="Movie Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/events", status_code=201)
def publish(event: Event):
    try:
        app.state.publisher.publish(event)
    except (KafkaException, BufferError, TimeoutError) as exc:
        log.warning("publish_failed event_id=%s error=%s", event.event_id, exc)
        raise HTTPException(503, detail="Kafka acknowledgement unavailable") from exc
    return {"event_id": event.event_id}
