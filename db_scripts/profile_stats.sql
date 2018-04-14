-- информация по профилю нагрузки
VARIABLE creation_time_cursor REFCURSOR;
VARIABLE exec_start_time_cursor REFCURSOR;
VARIABLE exec_end_time_cursor REFCURSOR;
VARIABLE priority_cursor REFCURSOR;
VARIABLE processing_type_cursor REFCURSOR;
execute by_creation_time_count(:creation_time_cursor);
execute by_exec_start_time_count(:exec_start_time_cursor);
execute by_exec_end_time_count(:exec_end_time_cursor);
execute by_priority_count(:priority_cursor);
execute by_processing_type_count(:processing_type_cursor);
PRINT creation_time_cursor;
PRINT exec_start_time_cursor;
PRINT exec_end_time_cursor;
PRINT priority_cursor;
PRINT processing_type_cursor;