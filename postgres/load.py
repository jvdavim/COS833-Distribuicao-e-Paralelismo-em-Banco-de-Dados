import postgres
import time

LOAD_DIR = '/opt/db/tpch-tool/2.18.0_rc2/dbgen'

loader = postgres.DatabaseLoader(LOAD_DIR)

loader.drop()
loader.create()
loader.load()

# for i in range(2):
#     print(f'Execução {i+1}:')
#     loader.drop()
#     start = time.time()
#     loader.create()
#     end = time.time()
#     print(f'create time: {end-start}')
#     start = time.time()
#     loader.load()
#     end = time.time()
#     print(f'load time: {end-start}')
