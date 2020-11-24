-- статистикв по времени в конкретном состоянии
DECLARE
  wf_state   VARCHAR2(64) := 'TERMINAL_STATE';
BEGIN
  execute immediate 'DELETE FROM tmp_stats';
  execute immediate 'DELETE FROM tmp_stats_new';
  execute immediate 'commit';
  time_in_state_count(wf_state);
END;