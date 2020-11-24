-- информация по профилю нагрузки
-- создание в минуту
SELECT to_char(tord.creation_time, 'yyyy-mm-dd hh24:mi') as creation_time, count(*)
FROM tmp_orders tord
GROUP BY to_char(tord.creation_time, 'yyyy-mm-dd hh24:mi');

-- начало исполнения в минуту
SELECT to_char(tord.exec_start_time, 'yyyy-mm-dd hh24:mi') as exec_start_time, count(*)
FROM tmp_orders tord
GROUP BY to_char(tord.exec_start_time, 'yyyy-mm-dd hh24:mi');

-- окончание исполнения в минуту
SELECT to_char(tord.exec_end_time, 'yyyy-mm-dd hh24:mi') as exec_end_time, count(*)
FROM tmp_orders tord
GROUP BY to_char(tord.exec_end_time, 'yyyy-mm-dd hh24:mi');

-- соотношение по приоритетам
SELECT tord.priority, count(*)
FROM tmp_orders tord
GROUP BY tord.priority;

-- соотношение по типу обработки
SELECT tord.processing_type, count(*)
FROM tmp_orders tord
GROUP BY tord.processing_type;