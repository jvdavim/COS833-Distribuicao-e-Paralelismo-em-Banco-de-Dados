import postgres

CSV_DIR = '/opt/db/tpch-tool/2.18.0_rc2/dbgen'

tpch = postgres.TPCH()

tpch.create_tables()

nations = tpch.select('SELECT * FROM nation')

tpch.load_data_from_dir(CSV_DIR)

nations = tpch.select('SELECT * FROM nation')

print("Hello")
