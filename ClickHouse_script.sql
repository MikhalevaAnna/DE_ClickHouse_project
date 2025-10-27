-- Создается таблица событий пользователей user_events
DROP TABLE IF EXISTS user_events;
CREATE TABLE user_events (
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;


-- Создается таблица для агрегированных данных user_events_agg
DROP TABLE IF EXISTS user_events_agg;
CREATE TABLE user_events_agg (
    event_date Date,
    event_type String,
    users_unique AggregateFunction(uniq, UInt32),
    spent_sum AggregateFunction(sum, UInt32),
    actions_count AggregateFunction(count, UInt8)
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;


-- Создается Materialized View user_events_mv, с использованием state-функций
DROP VIEW IF EXISTS user_events_mv;
CREATE MATERIALIZED VIEW user_events_mv TO user_events_agg AS
SELECT
    toDate(event_time) AS event_date,
    event_type,
    uniqState(user_id) AS users_unique,
    sumState(points_spent) AS spent_sum,
    countState() AS actions_count
FROM user_events
GROUP BY event_date, event_type;


-- Добавляются данные в таблицу user_events
INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());


-- Создается запрос, считающий Retention, сколько пользователей вернулись в течение следующих 7 дней
WITH
    first_visits AS (
        SELECT
            user_id,
            MIN(event_time) AS first_day
        FROM user_events
        WHERE event_type IN ('login', 'signup')
        GROUP BY user_id
    ),
    retention_data AS (
        SELECT
            fv.user_id,
            fv.first_day,
            MAX(If(ue.event_time BETWEEN fv.first_day + INTERVAL 1 DAY AND fv.first_day + INTERVAL 7 DAY, 1, 0)) AS rd_returned_in_7_days
        FROM first_visits fv
        LEFT JOIN user_events ue ON (fv.user_id = ue.user_id) AND (ue.event_type IN ('login', 'purchase'))
        GROUP BY fv.user_id, fv.first_day
    )
SELECT
    COUNT(*) AS total_users_day_0,
    SUM(rd_returned_in_7_days) AS returned_in_7_days,
    ROUND(returned_in_7_days / total_users_day_0 * 100, 2) AS retention_7d_percent
FROM retention_data


-- Создается запрос с группировками по быстрой аналитике по дням, с использованием merge-функций
SELECT
	event_date,
	event_type,
	uniqMerge(users_unique) AS unique_users,
    sumMerge(spent_sum) AS total_spent,
    countMerge(actions_count) AS total_actions
FROM user_events_agg
WHERE event_type IN ('login', 'signup', 'purchase')
GROUP BY
    event_date, event_type
ORDER BY
    event_date, event_type;