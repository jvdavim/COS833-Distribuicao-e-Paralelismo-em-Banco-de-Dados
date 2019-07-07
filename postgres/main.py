import postgres

CSV_DIR = '/opt/db/tpch-tool/2.18.0_rc2/dbgen'

tpch = postgres.TPCH()

tpch.create_tables()
tpch.load_data_from_dir(CSV_DIR)

q3 = tpch.select(tpch.query3)

q6 = tpch.select(tpch.query6)

q10 = tpch.select(tpch.query10)

print("HELLO")
