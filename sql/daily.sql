SELECT
    count() AS events,
    uniqExact(user_id) AS dau,
    countIf(event_type = 'VIEW_STARTED') AS started,
    countIf(event_type = 'VIEW_FINISHED') AS finished,
    avgOrNullIf(progress_seconds, event_type = 'VIEW_FINISHED') AS avg_watch_seconds
FROM cinema.movie_events_raw FINAL
WHERE event_date = {day:Date}
