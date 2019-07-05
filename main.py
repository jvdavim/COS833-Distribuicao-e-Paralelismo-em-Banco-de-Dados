import postgres

CSV_DIR = '/opt/db/tpch-tool/2.18.0_rc2/dbgen'

tpch = postgres.TPCH()

tpch.load_data_from_dir(CSV_DIR)