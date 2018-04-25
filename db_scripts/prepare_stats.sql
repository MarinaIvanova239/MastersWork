-- подготовка статистики
DECLARE
  wf_name   VARCHAR2(64) := 'dummyWorkflow';
BEGIN
  execute immediate 'DELETE FROM tmp_orders';
  execute immediate 'DELETE FROM tmp_transitions';
  execute immediate 'commit';
  stats_prepare(wf_name);
END;
