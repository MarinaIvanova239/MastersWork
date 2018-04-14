-- Количество fail заявок
SELECT count(*)
FROM orders o
WHERE o.workflow_name = $1
	AND o.update_time > sysdate-1
	AND o.state_id = 4;