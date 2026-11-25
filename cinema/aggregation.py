import logging
import os
from contextlib import asynccontextmanager
from datetime import date
from threading import Event, Thread

from fastapi import FastAPI, HTTPException

from cinema.metrics import MissingMetrics, export, recompute, scheduler, utc_today

log = logging.getLogger("uvicorn.error")


@asynccontextmanager
async def lifespan(app):
    interval = float(os.environ.get("AGGREGATION_INTERVAL_SECONDS", "60"))
    if interval <= 0:
        raise ValueError("AGGREGATION_INTERVAL_SECONDS must be positive")
    stop = Event()
    thread = Thread(target=scheduler, args=(stop, interval), daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=10)


app = FastAPI(title="Aggregation Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


def check_date(day):
    if day < date(1970, 1, 1) or day >= utc_today():
        raise HTTPException(422, detail="Expected a completed UTC day from 1970 onward")


@app.post("/recompute/{day}")
def recompute_date(day: date):
    check_date(day)
    try:
        return recompute(day)
    except Exception as exc:
        log.exception("aggregation_failed date=%s", day)
        raise HTTPException(503, detail="Could not compute and save metrics") from exc


@app.post("/export/{day}")
def export_date(day: date):
    check_date(day)
    try:
        return export(day, request=True)
    except MissingMetrics as exc:
        raise HTTPException(404, detail="Recompute metrics for this date first") from exc
    except Exception as exc:
        log.exception("s3_export_failed date=%s", day)
        raise HTTPException(503, detail="Could not export metrics") from exc
