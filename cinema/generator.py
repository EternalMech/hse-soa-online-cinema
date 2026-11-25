import argparse
import logging
import random
from datetime import UTC, datetime, time, timedelta
from uuid import NAMESPACE_URL, uuid4, uuid5

from cinema.events import DeviceType, Event, EventType

log = logging.getLogger("uvicorn.error")


def session(user, movie, start, rng, *, complete, session_id=None):
    session_id = session_id or str(uuid4())
    device = rng.choice(list(DeviceType))
    sequence = [
        (0, EventType.SEARCHED, None),
        (15, EventType.VIEW_STARTED, 0),
        (315, EventType.VIEW_PAUSED, 300),
        (375, EventType.VIEW_RESUMED, 300),
    ]
    if complete:
        duration = rng.randint(3600, 7200)
        sequence.append((duration + 75, EventType.VIEW_FINISHED, duration))
        if rng.random() < 0.45:
            sequence.append((duration + 85, EventType.LIKED, None))
    for index, (offset, kind, progress) in enumerate(sequence):
        yield Event(
            event_id=uuid5(NAMESPACE_URL, f"{session_id}/{index}"),
            user_id=user, movie_id=movie, event_type=kind,
            timestamp=start + timedelta(seconds=offset),
            device_type=device, session_id=session_id, progress_seconds=progress,
        )


def demo():
    last = datetime.now(UTC).date() - timedelta(days=1)
    for cohort_offset in range(8):
        cohort = last - timedelta(days=7 - cohort_offset)
        for index in range(5):
            user = f"demo-{cohort}-{index}"
            for age in range(8 - cohort_offset):
                day = cohort + timedelta(days=age)
                session_id = f"{user}/{day}"
                rng = random.Random(session_id)
                if age > 0 and rng.random() > (5 - index) / (6 + age):
                    continue
                start = datetime.combine(day, time(9 + index), UTC)
                yield from session(
                    user, f"film-{rng.randrange(1, 8)}", start, rng,
                    complete=rng.random() < 0.75, session_id=session_id,
                )


def generate_forever(publish, interval, stop):
    rng = random.Random()
    while not stop.is_set():
        user = f"stream-{rng.randrange(1, 101)}"
        start = datetime.now(UTC) - timedelta(hours=3)
        for event in session(user, f"film-{rng.randrange(1, 8)}", start, rng, complete=rng.random() < 0.75):
            if stop.is_set():
                return
            try:
                publish(event)
            except Exception:
                log.exception("generator_publish_failed event_id=%s", event.event_id)
                break
        stop.wait(interval)


def main():
    import httpx

    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    total = 0
    with httpx.Client(base_url=args.url, timeout=18) as client:
        for event in demo():
            client.post("/events", json=event.model_dump(mode="json")).raise_for_status()
            total += 1
    print(f"published {total} demo events")


if __name__ == "__main__":
    main()
