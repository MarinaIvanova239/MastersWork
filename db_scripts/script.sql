-- ВЫЗОВ ПРОЦЕДУР

DECLARE
  wf_name   VARCHAR2(64) := 'dummyWorkflow';
BEGIN
  execute immediate 'DELETE FROM tmp_orders';
  execute immediate 'DELETE FROM tmp_transitions';
  stats_prepare(wf_name);
END;

VARIABLE my_cursor REFCURSOR;
execute by_creation_time_count(:my_cursor);
PRINT my_cursor;


-- считаем количество fail заявок и понимаем, можно ли их исключить
SELECT count(*)
FROM orders o
WHERE o.workflow_name='<имя_workflow>'
	AND o.creation_time > sysdate-1
	AND o.state_id = 4;