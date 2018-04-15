VARIABLE my_cursor REFCURSOR;
execute transitions_to_states_count($1, :my_cursor);
PRINT my_cursor;