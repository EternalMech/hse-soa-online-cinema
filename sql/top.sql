SELECT movie_id, countIf(event_type = 'VIEW_STARTED') AS views
FROM cinema.movie_events_raw FINAL
WHERE event_date = {day:Date}
GROUP BY movie_id
HAVING views > 0
ORDER BY views DESC, movie_id ASC
LIMIT 10
