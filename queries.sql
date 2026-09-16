-- 1. Сверка Spark и Trino (Аналитический запрос)
-- Выполнить в Trino (DataGrip или CLI)
SELECT 
    member_casual, 
    ROUND(AVG(CAST(duration_sec AS DECIMAL(18,2))) / 60.0, 2) as avg_duration_minutes, 
    COUNT(*) as trip_count
FROM lakehouse.student_01.divvy_iceberg
WHERE member_casual IS NOT NULL
GROUP BY member_casual
ORDER BY trip_count DESC;

-- 2. Проверка истории Snapshots в Trino
-- Имя таблицы с суффиксом берется в двойные кавычки
SELECT snapshot_id, committed_at 
FROM lakehouse.student_01."divvy_iceberg$snapshots"
ORDER BY committed_at DESC;

-- 3. Федеративный JOIN (Критерий: два каталога + группа "Не сопоставлен")
-- Создаем временный справочник в каталоге memory (эмуляция внешней БД)
CREATE TABLE IF NOT EXISTS memory.default.member_types (
    type_code VARCHAR,
    description VARCHAR
);
INSERT INTO memory.default.member_types VALUES 
    ('member', 'Годовой подписчик Divvy'),
    ('casual', 'Разовый пользователь (24h/3h pass)');

-- Выполняем LEFT JOIN. Если типа нет в справочнике, пишем 'Не сопоставлен'
SELECT 
    COALESCE(mt.description, 'Не сопоставлен') AS client_type_description,
    d.member_casual AS raw_code,
    COUNT(d.ride_id) AS total_trips
FROM lakehouse.student_01.divvy_iceberg d
LEFT JOIN memory.default.member_types mt 
    ON d.member_casual = mt.type_code
GROUP BY COALESCE(mt.description, 'Не сопоставлен'), d.member_casual
ORDER BY total_trips DESC;
