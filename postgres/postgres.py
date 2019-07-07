import psycopg2
import os


class TPCH:
    def __init__(self):
        self.conn = psycopg2.connect("host=localhost dbname=tpch user=postgres")
        self.cur = self.conn.cursor()
        self.folder = os.path.dirname(os.path.abspath(__file__))
        self.query3 = open(os.path.join(self.folder, '3.sql'), "r").read()
        self.query6 = open(os.path.join(self.folder, '6.sql'), "r").read()
        self.query10 = open(os.path.join(self.folder, '10.sql'), "r").read()

    def create_tables(self):
        try:
            self.cur.execute('BEGIN')
            self.cur.execute('DROP TABLE IF EXISTS lineitem')
            self.cur.execute('DROP TABLE IF EXISTS orders')
            self.cur.execute('DROP TABLE IF EXISTS partsupp')
            self.cur.execute('DROP TABLE IF EXISTS customer')
            self.cur.execute('DROP TABLE IF EXISTS supplier')
            self.cur.execute('DROP TABLE IF EXISTS nation')
            self.cur.execute('DROP TABLE IF EXISTS part')
            self.cur.execute('DROP TABLE IF EXISTS region')
            self.cur.execute('CREATE TABLE part('
                             'P_PARTKEY		SERIAL PRIMARY KEY,'
                             'P_NAME			VARCHAR(55),'
                             'P_MFGR			CHAR(25),'
                             'P_BRAND			CHAR(10),'
                             'P_TYPE			VARCHAR(25),'
                             'P_SIZE			INTEGER,'
                             'P_CONTAINER		CHAR(10),'
                             'P_RETAILPRICE	DECIMAL,'
                             'P_COMMENT		VARCHAR(23)'
                             ')')
            self.cur.execute('CREATE TABLE REGION ('
                             'R_REGIONKEY	SERIAL PRIMARY KEY,'
                             'R_NAME		CHAR(25),'
                             'R_COMMENT	VARCHAR(152)'
                             ')')
            self.cur.execute('CREATE TABLE nation ('
                             'N_NATIONKEY		SERIAL PRIMARY KEY,'
                             'N_NAME			CHAR(25),'
                             'N_REGIONKEY		BIGINT NOT NULL,'
                             'N_COMMENT		VARCHAR(152)'
                             ')')
            self.cur.execute('CREATE TABLE supplier ('
                             'S_SUPPKEY		SERIAL PRIMARY KEY,'
                             'S_NAME			CHAR(25),'
                             'S_ADDRESS		VARCHAR(40),'
                             'S_NATIONKEY		BIGINT NOT NULL,'
                             'S_PHONE			CHAR(15),'
                             'S_ACCTBAL		DECIMAL,'
                             'S_COMMENT		VARCHAR(101)'
                             ')')
            self.cur.execute('CREATE TABLE customer ('
                             'C_CUSTKEY		SERIAL PRIMARY KEY,'
                             'C_NAME			VARCHAR(25),'
                             'C_ADDRESS		VARCHAR(40),'
                             'C_NATIONKEY		BIGINT NOT NULL,'
                             'C_PHONE			CHAR(15),'
                             'C_ACCTBAL		DECIMAL,'
                             'C_MKTSEGMENT	CHAR(10),'
                             'C_COMMENT		VARCHAR(117)'
                             ')')
            self.cur.execute('CREATE TABLE partsupp ('
                             'PS_PARTKEY		BIGINT NOT NULL,'
                             'PS_SUPPKEY		BIGINT NOT NULL,'
                             'PS_AVAILQTY		INTEGER,'
                             'PS_SUPPLYCOST	DECIMAL,'
                             'PS_COMMENT		VARCHAR(199),'
                             'PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)'
                             ')')
            self.cur.execute('CREATE TABLE orders ('
                             'O_ORDERKEY		SERIAL PRIMARY KEY,'
                             'O_CUSTKEY		BIGINT NOT NULL,'
                             'O_ORDERSTATUS	CHAR(1),'
                             'O_TOTALPRICE	DECIMAL,'
                             'O_ORDERDATE		DATE,'
                             'O_ORDERPRIORITY	CHAR(15),'
                             'O_CLERK			CHAR(15),'
                             'O_SHIPPRIORITY	INTEGER,'
                             'O_COMMENT		VARCHAR(79)'
                             ')')
            self.cur.execute('CREATE TABLE lineitem ('
                             'L_ORDERKEY		BIGINT NOT NULL,'
                             'L_PARTKEY		BIGINT NOT NULL,'
                             'L_SUPPKEY		BIGINT NOT NULL,'
                             'L_LINENUMBER	INTEGER,'
                             'L_QUANTITY		DECIMAL,'
                             'L_EXTENDEDPRICE	DECIMAL,'
                             'L_DISCOUNT		DECIMAL,'
                             'L_TAX			DECIMAL,'
                             'L_RETURNFLAG	CHAR(1),'
                             'L_LINESTATUS	CHAR(1),'
                             'L_SHIPDATE		DATE,'
                             'L_COMMITDATE	DATE,'
                             'L_RECEIPTDATE	DATE,'
                             'L_SHIPINSTRUCT	CHAR(25),'
                             'L_SHIPMODE		CHAR(10),'
                             'L_COMMENT		VARCHAR(44),'
                             'PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)  '
                             ')')
            self.cur.execute('COMMIT')
        except Exception as e:
            print(f'Erro ao criar tabelas: {e}')
            self.cur.execute('ABORT')

    def load_data_from_dir(self, dir):
        try:
            self.cur.execute('BEGIN')
            self.cur.execute('COPY region FROM \'' + str(dir) + '/region.csv\' WITH DELIMITER AS \'|\'')
            self.cur.execute('COPY nation FROM \'' + str(dir) + '/nation.csv\' WITH DELIMITER AS \'|\'')
            self.cur.execute('COPY customer FROM \'' + str(dir) + '/customer.csv\' WITH DELIMITER AS \'|\'')
            self.cur.execute('COPY orders FROM \'' + str(dir) + '/orders.csv\' WITH DELIMITER AS \'|\'')
            self.cur.execute('COPY part FROM \'' + str(dir) + '/part.csv\' WITH DELIMITER AS \'|\'')
            self.cur.execute('COPY supplier FROM \'' + str(dir) + '/supplier.csv\' WITH DELIMITER AS \'|\'')
            self.cur.execute('COPY partsupp FROM \'' + str(dir) + '/partsupp.csv\' WITH DELIMITER AS \'|\'')
            self.cur.execute('COPY lineitem FROM \'' + str(dir) + '/lineitem.csv\' WITH DELIMITER AS \'|\'')
            self.cur.execute('COMMIT')
        except Exception as e:
            print(f'Erro ao carregar dados: {e}')
            self.cur.execute('ABORT')

    def do_query(self, query):
        try:
            self.cur.execute('BEGIN')
            self.cur.execute(query)
            self.cur.execute('COMMIT')
        except Exception as e:
            print(f'Erro ao executar query: {e}')
            self.cur.execute('ABORT')

    def select(self, query):
        try:
            self.cur.execute('BEGIN')
            self.cur.execute(query)
            result = self.cur.fetchall()
            self.cur.execute('COMMIT')
            return result
        except Exception as e:
            print(f'Erro ao executar select: {e}')
            self.cur.execute('ABORT')
            return None
