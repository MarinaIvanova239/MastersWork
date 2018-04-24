import cx_Oracle
connection = cx_Oracle.connect("crab_akka", "crab_akka", "srv7-furyik/oracle.net.billing.ru")
print connection.version
cursor = connection.cursor()
cursor.execute("""
    SELECT external_id, from_state, to_state, to_state_time, from_state_time, in_state_time
	FROM tmp_stats""")

filename = "example.csv"
csv = open(filename, "w")

for e_id, from_state, to_state, to_state_time, from_state_time, in_state_time in cursor:
	row = e_id + "," + from_state + "," + to_state + "," + to_state_time.strftime("%Y-%m-%d %H:%M:%S") + \
		  "," + from_state_time.strftime("%Y-%m-%d %H:%M:%S") + "," + str(in_state_time) + "\n"
	csv.write(row)

connection.close()