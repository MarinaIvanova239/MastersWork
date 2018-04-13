
CREATE OR REPLACE PROCEDURE stats_prepare(wf_name VARCHAR2)
AS
BEGIN
	INSERT INTO tmp_orders(external_id, state_id, priority, processing_type, creation_time, exec_start_time, exec_end_time)
	SELECT o.external_id, o.state_id, o.priority, o.processing_type, o.creation_time, o.exec_start_time, o.exec_end_time
	FROM orders o
	WHERE o.workflow_name=wf_name
	AND o.creation_time > sysdate-1
	AND o.state_id = 3;

	INSERT INTO tmp_transitions(external_id, transition_time, from_state, to_state, event_name, last_action)
	SELECT wt.external_id, wt.transition_time, wt.from_state, wt.to_state, wt.event, wt.last_action
	FROM workflow_transitions wt
	WHERE wt.external_id IN (SELECT tord.external_id FROM tmp_orders tord);
END;


CREATE OR REPLACE PROCEDURE by_creation_time_count(by_creation_time_stat OUT SYS_REFCURSOR)
AS
BEGIN
    OPEN by_creation_time_stat FOR
    SELECT to_char(tord.creation_time, 'yyyy-mm-dd hh24:mi'), count(*)
	FROM tmp_orders tord
	GROUP BY to_char(tord.creation_time, 'yyyy-mm-dd hh24:mi');
END;


CREATE OR REPLACE PROCEDURE by_exec_start_time_count(by_exec_start_time_stat OUT SYS_REFCURSOR)
AS
BEGIN
	OPEN by_exec_start_time_stat FOR
	SELECT to_char(tord.exec_start_time, 'yyyy-mm-dd hh24:mi'), count(*)
	FROM tmp_orders tord
	GROUP BY to_char(tord.exec_start_time, 'yyyy-mm-dd hh24:mi');
END;

CREATE OR REPLACE PROCEDURE by_exec_end_time_count(by_exec_end_time_stat OUT SYS_REFCURSOR)
AS
BEGIN
	OPEN by_exec_end_time_stat FOR
	SELECT to_char(tord.exec_end_time, 'yyyy-mm-dd hh24:mi'), count(*)
	FROM tmp_orders tord
	GROUP BY to_char(tord.exec_end_time, 'yyyy-mm-dd hh24:mi');
END;

CREATE OR REPLACE PROCEDURE by_priority_count(by_priority_stat OUT SYS_REFCURSOR)
AS
BEGIN
	OPEN by_priority_stat FOR
	SELECT tord.priority, count(*)
	FROM tmp_orders tord
	GROUP BY tord.priority;
END;

CREATE OR REPLACE PROCEDURE by_processing_type_count(by_processing_type_stat OUT SYS_REFCURSOR)
AS
BEGIN
	OPEN by_processing_type_stat FOR
	SELECT tord.processing_type, count(*)
	FROM tmp_orders tord
	GROUP BY tord.processing_type;
END;

-- отсюда нужно будет выбирать с группировкой по from_state
-- не для первого стейта
-- отсюда - куда выходили и сколько раз
CREATE OR REPLACE PROCEDURE time_in_state_count(state_name VARCHAR2)
AS
    tr_to_time      TIMESTAMP;
    to_state_name   VARCHAR2(64);
BEGIN
	execute immediate 'DELETE FROM tmp_stats';

	INSERT INTO tmp_stats(state, external_id, from_state, to_state, to_state_time, from_state_time, in_state_time)
	SELECT state_name, tt.external_id, tt.from_state, NULL, tt.transition_time, NULL, NULL
	FROM tmp_transitions tt
	WHERE tt.to_state = state_name;

	FOR e_id IN (SELECT DISTINCT external_id FROM tmp_stats)
    LOOP
		execute immediate 'DELETE FROM tmp_stats_new';

		INSERT INTO tmp_stats_new(external_id, transition_time, to_state)
		SELECT tt.external_id, tt.transition_time, tt.to_state
		FROM tmp_transitions tt
		WHERE tt.from_state = state_name AND tt.external_id = e_id.external_id;

		FOR tr_time IN (SELECT to_state_time FROM tmp_stats WHERE external_id = e_id.external_id)
        LOOP
			SELECT tsn_time
			INTO tr_to_time
			FROM
			(SELECT tsn.transition_time as tsn_time
             FROM tmp_stats_new tsn
             WHERE tsn.transition_time > tr_time.to_state_time
             ORDER BY tsn.transition_time ASC)
			 WHERE ROWNUM = 1;

			SELECT tsn_to_state
			INTO to_state_name
			FROM
            (SELECT tsn.to_state as tsn_to_state
             FROM tmp_stats_new tsn
             WHERE tsn.transition_time > tr_time.to_state_time
             ORDER BY tsn.transition_time ASC)
			 WHERE ROWNUM = 1;

			UPDATE tmp_stats
			SET from_state_time = tr_to_time,
				to_state = to_state_name,
				in_state_time = (tr_to_time - to_state_time)
			WHERE external_id = e_id.external_id AND to_state_time = tr_time.to_state_time;
		END LOOP;

		UPDATE tmp_stats
		SET in_state_time = ((SELECT exec_end_time FROM tmp_orders WHERE external_id = e_id.external_id) - to_state_time)
		WHERE in_state_time = NULL;

	END LOOP;

    COMMIT;

END;

-- отсюда нужно будет выбирать с группировкой по from_state
-- только для первого стейта
-- отсюда - куда выходили и сколько раз
CREATE OR REPLACE PROCEDURE first_time_in_state_count(state_name VARCHAR2)
AS
    tr_time         TIMESTAMP;
    tr_to_time      TIMESTAMP;
    to_state_name   VARCHAR2(64);
BEGIN
	execute immediate 'DELETE FROM tmp_stats';

	FOR e_id IN (SELECT DISTINCT external_id FROM tmp_stats)
    LOOP
		INSERT INTO tmp_stats(state, external_id, from_state, to_state, to_state_time, from_state_time, in_state_time)
		SELECT state_name, tord.external_id, NULL, NULL, tord.exec_start_time, NULL, NULL
		FROM tmp_orders tord
		WHERE tord.external_id = e_id.external_id;
	END LOOP;

	FOR e_id IN (SELECT DISTINCT external_id FROM tmp_stats)
    LOOP
		execute immediate 'DELETE FROM tmp_stats_new';

		INSERT INTO tmp_stats_new(external_id, transition_time, to_state)
		SELECT tt.external_id, tt.transition_time, tt.to_state
		FROM tmp_transitions tt
		WHERE tt.from_state = state_name and tt.external_id = e_id.external_id;

		SELECT to_state_time
        INTO tr_time
        FROM tmp_stats
        WHERE external_id = e_id.external_id AND ROWNUM = 1;

        SELECT tsn_time
        INTO tr_to_time
        FROM
        (SELECT tsn.transition_time as tsn_time
         FROM tmp_stats_new tsn
         WHERE tsn.transition_time > tr_time
         ORDER BY tsn.transition_time ASC)
         WHERE ROWNUM = 1;

        SELECT tsn_to_state
        INTO to_state_name
        FROM
        (SELECT tsn.to_state as tsn_to_state
         FROM tmp_stats_new tsn
         WHERE tsn.transition_time > tr_time
         ORDER BY tsn.transition_time ASC)
         WHERE ROWNUM = 1;

		UPDATE tmp_stats
		SET from_state_time = tr_to_time,
			to_state = to_state_name,
			in_state_time = (tr_to_time - to_state_time)
		WHERE external_id = e_id.external_id AND to_state_time = tr_time;

		UPDATE tmp_stats
		SET in_state_time = ((SELECT exec_end_time FROM tmp_orders WHERE external_id = e_id.external_id) - tr_time)
		WHERE in_state_time = NULL;

	END LOOP;
END;


CREATE OR REPLACE PROCEDURE transitions_to_states_count (state_name VARCHAR2, tr_to_state_stat OUT SYS_REFCURSOR)
AS
BEGIN
    OPEN tr_to_state_stat FOR
	SELECT ts.to_state, count(*)
	FROM tmp_stats ts
	WHERE ts.from_state = state_name
	GROUP BY ts.to_state;
END;

