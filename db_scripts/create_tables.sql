-- usage: sqlplus sys_user/sys_password @create_tables.sql <tablespace_name>

CREATE TABLE tmp_orders(
	external_id VARCHAR2(512) not null,
	state_id NUMBER not null,
	priority NUMBER,
	processing_type NUMBER not null,
	creation_time TIMESTAMP not null,
	exec_start_time TIMESTAMP,
	exec_end_time TIMESTAMP
);

CREATE TABLE tmp_transitions(
	external_id VARCHAR2(512) not null,
	transition_time TIMESTAMP not null,
	from_state VARCHAR2(64) not null,
	to_state VARCHAR2(64) not null,
	event_name VARCHAR2(128) not null,
	last_action CLOB not null
);

-- сколько времени заявка провела в состоянии, до того, как выйти по какому либо событию.
CREATE TABLE tmp_stats(
    state VARCHAR2(64),                     -- исследуемое состояние
	external_id VARCHAR2(512) not null,
	from_state VARCHAR2(64),                -- откуда перешли в исследуемого состояния
	to_state VARCHAR2(64),                  -- куда перешли из исследуемого состояния
	to_state_time TIMESTAMP not null,       -- время перехода в это состояние
	from_state_time TIMESTAMP,              -- время выхода из состояния
	in_state_time INTERVAL DAY TO SECOND    -- время в исследуемом состоянии
);

-- вспомогательная табличка
CREATE TABLE tmp_stats_new(
	external_id VARCHAR2(512) not null,
	transition_time TIMESTAMP not null,
	to_state VARCHAR2(64)
);

-- создаем индексы

-- tmp_orders
CREATE INDEX TORD_EXTID_I ON tmp_orders(external_id) INITRANS 40 TABLESPACE &1;

-- tmp_transitions
CREATE INDEX TTRANS_EXTID_I ON tmp_transitions(external_id) INITRANS 40 TABLESPACE &1;
CREATE INDEX TTRANS_FROM_STATE_I ON tmp_transitions(from_state) INITRANS 40 TABLESPACE &1;
CREATE INDEX TTRANS_TO_STATE_I ON tmp_transitions(to_state) INITRANS 40 TABLESPACE &1;

-- tmp_stats
CREATE INDEX TSTATS_EXTID_I ON tmp_stats(external_id) INITRANS 40 TABLESPACE &1;
CREATE INDEX TSTATS_TO_STATE_TIME_I ON tmp_stats(to_state_time) INITRANS 40 TABLESPACE &1;

-- tmp_stats_new
CREATE INDEX TSTATSNEW_TO_STATE_TIME_I ON tmp_stats_new(transition_time) INITRANS 40 TABLESPACE &1;