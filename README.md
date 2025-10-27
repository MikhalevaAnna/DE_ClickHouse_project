#  Работа с ClickHouse 

## Описание:

1) Есть события пользователей, которые записываются в Clickhouse в таблицу `user_events`: </br>
<img width="303" height="270" alt="image" src="https://github.com/user-attachments/assets/e8f03e10-e883-43ec-964c-bdb2857f1b82" /> </br>
2) Необходимо поддерживать сырые логи событий (схема выше). Данные в этой таблице должны хранится **30** дней.</br>
3) Построить агрегированную таблицу. Храним агрегаты **180** дней, чтобы делать трендовый анализ: </br>
   - уникальные пользователи по `event_type` и `event_date`
   - сумма потраченных баллов
   - количество действий
4) Сделать **Materialized View**, которая: </br>
   - при вставке данных в таблицу сырых логов событий, будет обновлять агрегированную таблицу
   - использует `sumState`, `uniqState`, `countState`
5) Создать запрос, показывающий: </br>
   - **Retention**: сколько пользователей вернулись в течение следующих **7** дней.
   - Формат результата - | _total_users_day_0_ | _returned_in_7_days_ | _retention_7d_percent_ |
6) Создать запрос с группировками по быстрой аналитике по дням:</br>
   - Использовать **merge** 
   - Формат результата - | _event_date_ | _evenet_type_ | _unique_users_ | _total_spent_ | _total_actions_ |

## Реализация:
1) Создана таблица событий пользователей `user_events` по схеме, описанной выше, данные в этой таблице хранятся 30 дней:
``` 
CREATE TABLE user_events (
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;
```
2) Создана агрегированная таблица, данные в которой хранятся 180 дней, чтобы делать трендовый анализ:
```
CREATE TABLE user_events_agg (
    event_date Date,
    event_type String,
    users_unique AggregateFunction(uniq, UInt32),
    spent_sum AggregateFunction(sum, UInt32),
    actions_count AggregateFunction(count, UInt8)
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;
```

- `users_unique` - уникальные пользователи по `event_type` и `event_date`,
- `spent_sum` - сумма потраченных баллов,
- `actions_count` - количество действий пользователей.

3) Создано **Materialized View**, которое будет обновлять агрегированную таблицу `user_events_agg` и </br>использует функции: `sumState`, `uniqState`, `countState`; </br>
при вставке данных в таблицу сырых логов событий `user_events`:
```
CREATE MATERIALIZED VIEW user_events_mv TO user_events_agg AS
SELECT
    toDate(event_time) AS event_date,
    event_type,
    uniqState(user_id) AS users_unique,
    sumState(points_spent) AS spent_sum,
    countState() AS actions_count
FROM user_events
GROUP BY event_date, event_type;
```
4) Создан запрос, расчитывающий **Retention** и показывающий сколько пользователей вернулись в течение следующих 7 дней.
   
| _total_users_day_0_ | _returned_in_7_days_ | _retention_7d_percent_ |
|---------------------|----------------------|------------------------|
|                   6 |	                   4 |	                66.67 | 

6) Создан запрос с группировками по быстрой аналитике по дням с использованием функций: `uniqMerge`, `sumMerge`, `countMerge` для значимых событий:
```
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
```   
8) В репозитории представлен **sql-скрипт** с тестовыми данными, при запуске котрого выполняются действия для получения итоговых результатов.
