import cx_Oracle

connection = cx_Oracle.connect("user", "password", "host/service")
print connection.version

cursor = connection.cursor()
cursor.execute("""
    SELECT external_id, from_state, to_state, to_state_time, from_state_time, in_state_time
	FROM tmp_stats""")

#cursor.execute("""
#    SELECT external_id, priority, processing_type, creation_time, exec_start_time, exec_end_time
#	FROM tmp_orders""")

#cursor.execute("""
#    SELECT to_char(tord.creation_time, 'yyyy-mm-dd hh24:mi') as creation_time, count(*)
#	FROM tmp_orders tord
#	GROUP BY to_char(tord.creation_time, 'yyyy-mm-dd hh24:mi')""")

csv = open("result.csv", "w")

for e_id, from_state, to_state, to_state_time, from_state_time, in_state_time in cursor:
	row = e_id + "," + from_state + "," + to_state + "," + str(to_state_time) + "," + str(from_state_time) + "," + str(in_state_time) + "\n"
	csv.write(row)

#for e_id, priority, processing_type, creation_time, exec_start_time, exec_end_time in cursor:
#	row = e_id + "," + str(priority) + "," + str(processing_type) + "," + str(creation_time) + "," + str(exec_start_time) + "," + str(exec_end_time) + "\n"
#	csv.write(row)

#for time, cnt in cursor:
#	row = str(time) + "," + str(cnt) + "\n"
#	csv.write(row)

csv.close()
connection.close()