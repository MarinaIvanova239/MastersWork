-- статистикв по времени в конкретном состоянии
DECLARE
  wf_state   VARCHAR2(64) := 'TERMINAL_STATE';
BEGIN
  time_in_state_count(wf_state);
END;