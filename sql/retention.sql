WITH user_activity AS
(
    SELECT user_id,
           min(event_date) AS first_view,
           groupUniqArrayIf(event_date, event_date >= {day:Date}) AS return_days
    FROM cinema.movie_events_raw FINAL
    WHERE event_type = 'VIEW_STARTED' AND event_date <= least(addDays({day:Date}, 7), {as_of:Date})
    GROUP BY user_id
)
SELECT count() AS cohort_size,
       sumForEach(arrayMap(age -> toUInt64(has(return_days, addDays({day:Date}, age))), range(8))) AS retained
FROM user_activity
WHERE first_view = {day:Date}
