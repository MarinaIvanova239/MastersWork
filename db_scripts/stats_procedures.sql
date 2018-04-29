
CREATE OR REPLACE PROCEDURE stats_prepare(wf_name VARCHAR2)
AS
BEGIN
	INSERT INTO tmp_orders(external_id, priority, processing_type, creation_time, exec_start_time, exec_end_time)
	SELECT o.external_id, o.priority, o.processing_type, o.creation_time, o.exec_start_time, o.exec_end_time
	FROM orders o
	WHERE o.workflow_name=wf_name
	AND o.update_time > sysdate-1
	AND o.state_id = 3;

	INSERT INTO tmp_transitions(external_id, transition_time, from_state, to_state, event_name, last_action)
	SELECT wt.external_id, wt.transition_time, wt.from_state, wt.to_state, wt.event, wt.last_action
	FROM workflow_transitions wt
	WHERE wt.external_id IN (SELECT tord.external_id FROM tmp_orders tord);

	COMMIT;
END;

-- не для первого и последнего стейта
CREATE OR REPLACE PROCEDURE time_in_state_count(state_name VARCHAR2)
AS
    transition_from_state_time      TIMESTAMP;
    to_state_name                   VARCHAR2(64);
BEGIN
	execute immediate 'DELETE FROM tmp_stats';

    -- сохраняем все переходы в исследуемое состояние
	INSERT INTO tmp_stats(state, external_id, from_state, to_state, to_state_time, from_state_time, in_state_time)
	SELECT state_name, tt.external_id, tt.from_state, NULL, tt.transition_time, NULL, NULL
	FROM tmp_transitions tt
	WHERE tt.to_state = state_name;

	FOR e_id IN (SELECT DISTINCT external_id FROM tmp_stats)
    LOOP
		execute immediate 'DELETE FROM tmp_stats_new';

        -- по одной заявке сохраняем все переходы из исследуемого состояния
		INSERT INTO tmp_stats_new(external_id, transition_time, state)
		SELECT tt.external_id, tt.transition_time, tt.to_state
		FROM tmp_transitions tt
		WHERE tt.from_state = state_name AND tt.external_id = e_id.external_id;

        -- сопоставляем по времени все переходы в состояние с переходами из состояния
		FOR tr_time IN (SELECT to_state_time FROM tmp_stats WHERE external_id = e_id.external_id)
        LOOP
            -- находим время ближайшего перехода из состояния и куда он был совершен
			SELECT tsn_time, tsn_to_state
			INTO transition_from_state_time, to_state_name
			FROM
			(SELECT tsn.transition_time as tsn_time, tsn.state as tsn_to_state
             FROM tmp_stats_new tsn
             WHERE tsn.transition_time >= tr_time.to_state_time
             ORDER BY tsn.transition_time ASC)
			 WHERE ROWNUM = 1;

            -- записываем полученные данные в таблицу
			UPDATE tmp_stats
			SET from_state_time = transition_from_state_time,
				to_state = to_state_name,
				in_state_time = (transition_from_state_time - to_state_time)
			WHERE external_id = e_id.external_id AND to_state_time = tr_time.to_state_time;
		END LOOP;

	END LOOP;

    COMMIT;
END;

-- только для первого стейта
CREATE OR REPLACE PROCEDURE first_time_in_state_count(state_name VARCHAR2)
AS
    order_exec_start_time           TIMESTAMP;
    transition_from_state_time      TIMESTAMP;
    to_state_name                   VARCHAR2(64);
BEGIN
	execute immediate 'DELETE FROM tmp_stats';

    -- создаем уникальные записи для каждой заявки с временем перехода в состояние = времени начала исполнения заявки
	FOR e_id IN (SELECT DISTINCT external_id FROM tmp_orders)
    LOOP
		INSERT INTO tmp_stats(state, external_id, from_state, to_state, to_state_time, from_state_time, in_state_time)
		SELECT state_name, tord.external_id, NULL, NULL, tord.exec_start_time, NULL, NULL
		FROM tmp_orders tord
		WHERE tord.external_id = e_id.external_id;
	END LOOP;

	FOR e_id IN (SELECT DISTINCT external_id FROM tmp_stats)
    LOOP
		execute immediate 'DELETE FROM tmp_stats_new';

        -- по одной заявке сохраняем все переходы из исследуемого состояния
		INSERT INTO tmp_stats_new(external_id, transition_time, state)
		SELECT tt.external_id, tt.transition_time, tt.to_state
		FROM tmp_transitions tt
		WHERE tt.from_state = state_name and tt.external_id = e_id.external_id;

        -- находим первый переход из исследуемого состояния
		SELECT to_state_time
        INTO order_exec_start_time
        FROM tmp_stats
        WHERE external_id = e_id.external_id AND ROWNUM = 1;

         -- получаем время певого перехода и в какое состояние он был произведен
        SELECT tsn_time, tsn_to_state
        INTO transition_from_state_time, to_state_name
        FROM
        (SELECT tsn.transition_time as tsn_time, tsn.state as tsn_to_state
         FROM tmp_stats_new tsn
         WHERE tsn.transition_time >= order_exec_start_time
         ORDER BY tsn.transition_time ASC)
         WHERE ROWNUM = 1;

        -- записываем полученные данные в таблицу
		UPDATE tmp_stats
		SET from_state_time = transition_from_state_time,
			to_state = to_state_name,
			in_state_time = (transition_from_state_time - to_state_time)
		WHERE external_id = e_id.external_id;

	END LOOP;

	COMMIT;
END;

-- только для последнего стейта
CREATE OR REPLACE PROCEDURE last_time_in_state_count(state_name VARCHAR2)
AS
    order_exec_end_time             TIMESTAMP;
    transition_to_state_time        TIMESTAMP;
    from_state_name                 VARCHAR2(64);
BEGIN
	execute immediate 'DELETE FROM tmp_stats';

    -- создаем уникальные записи для каждой заявки с временем выхода из состояния = времени окончания исполнения заявки
	FOR e_id IN (SELECT DISTINCT external_id FROM tmp_orders)
    LOOP
		INSERT INTO tmp_stats(state, external_id, from_state, to_state, to_state_time, from_state_time, in_state_time)
		SELECT state_name, tord.external_id, NULL, NULL, NULL, tord.exec_end_time, NULL
		FROM tmp_orders tord
		WHERE tord.external_id = e_id.external_id;
	END LOOP;

	FOR e_id IN (SELECT DISTINCT external_id FROM tmp_stats)
    LOOP
		execute immediate 'DELETE FROM tmp_stats_new';

        -- по одной заявке сохраняем все переходы в исследуемое состояние
		INSERT INTO tmp_stats_new(external_id, transition_time, state)
		SELECT tt.external_id, tt.transition_time, tt.from_state
		FROM tmp_transitions tt
		WHERE tt.to_state = state_name and tt.external_id = e_id.external_id;

        -- находим последний переход в исследуемое состояние
		SELECT from_state_time
        INTO exec_end_time
        FROM tmp_stats
        WHERE external_id = e_id.external_id;

        -- получаем время последнего перехода и из какого состояния он был произведен
        SELECT tsn_time, tsn_from_state
        INTO transition_to_state_time, from_state_name
        FROM
        (SELECT tsn.transition_time as tsn_time, tsn.state as tsn_from_state
         FROM tmp_stats_new tsn
         WHERE tsn.transition_time <= order_exec_end_time
         ORDER BY tsn.transition_time DESC)
         WHERE ROWNUM = 1;

        -- записываем полученные данные в таблицу
		UPDATE tmp_stats
		SET to_state_time = transition_to_state_time,
			from_state = from_state_name,
			in_state_time = (from_state_time - transition_to_state_time)
		WHERE external_id = e_id.external_id;

	END LOOP;

	COMMIT;
END;