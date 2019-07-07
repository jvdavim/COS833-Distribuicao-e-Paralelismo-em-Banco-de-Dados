from threading import Thread
import psycopg2


class DatabaseLoader():
    def __init__(self, directory):
        self.connection = psycopg2.connect("host=localhost dbname=tpch user=postgres")
        self.cursor = self.connection.cursor()
        self.load_directory = directory

    def drop(self):
        try:
            self.cursor.execute('BEGIN')
            self.cursor.execute('DROP TABLE IF EXISTS lineitem')
            self.cursor.execute('DROP TABLE IF EXISTS orders')
            self.cursor.execute('DROP TABLE IF EXISTS partsupp')
            self.cursor.execute('DROP TABLE IF EXISTS customer')
            self.cursor.execute('DROP TABLE IF EXISTS supplier')
            self.cursor.execute('DROP TABLE IF EXISTS nation')
            self.cursor.execute('DROP TABLE IF EXISTS part')
            self.cursor.execute('DROP TABLE IF EXISTS region')
            self.cursor.execute('COMMIT')
        except Exception as e:
            print(f'Erro ao criar tabelas: {e}')
            self.cursor.execute('ABORT')

    def create(self):
        try:
            self.cursor.execute('BEGIN')
            self.cursor.execute('CREATE TABLE part('
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
            self.cursor.execute('CREATE TABLE REGION ('
                                'R_REGIONKEY	SERIAL PRIMARY KEY,'
                                'R_NAME		CHAR(25),'
                                'R_COMMENT	VARCHAR(152)'
                                ')')
            self.cursor.execute('CREATE TABLE nation ('
                                'N_NATIONKEY		SERIAL PRIMARY KEY,'
                                'N_NAME			CHAR(25),'
                                'N_REGIONKEY		BIGINT NOT NULL,'
                                'N_COMMENT		VARCHAR(152)'
                                ')')
            self.cursor.execute('CREATE TABLE supplier ('
                                'S_SUPPKEY		SERIAL PRIMARY KEY,'
                                'S_NAME			CHAR(25),'
                                'S_ADDRESS		VARCHAR(40),'
                                'S_NATIONKEY		BIGINT NOT NULL,'
                                'S_PHONE			CHAR(15),'
                                'S_ACCTBAL		DECIMAL,'
                                'S_COMMENT		VARCHAR(101)'
                                ')')
            self.cursor.execute('CREATE TABLE customer ('
                                'C_CUSTKEY		SERIAL PRIMARY KEY,'
                                'C_NAME			VARCHAR(25),'
                                'C_ADDRESS		VARCHAR(40),'
                                'C_NATIONKEY		BIGINT NOT NULL,'
                                'C_PHONE			CHAR(15),'
                                'C_ACCTBAL		DECIMAL,'
                                'C_MKTSEGMENT	CHAR(10),'
                                'C_COMMENT		VARCHAR(117)'
                                ')')
            self.cursor.execute('CREATE TABLE partsupp ('
                                'PS_PARTKEY		BIGINT NOT NULL,'
                                'PS_SUPPKEY		BIGINT NOT NULL,'
                                'PS_AVAILQTY		INTEGER,'
                                'PS_SUPPLYCOST	DECIMAL,'
                                'PS_COMMENT		VARCHAR(199),'
                                'PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)'
                                ')')
            self.cursor.execute('CREATE TABLE orders ('
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
            self.cursor.execute('CREATE TABLE lineitem ('
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
            self.cursor.execute('COMMIT')
        except Exception as e:
            print(f'Erro ao criar tabelas: {e}')
            self.cursor.execute('ABORT')

    def load(self):
        try:
            self.cursor.execute('BEGIN')
            self.cursor.execute('COPY region FROM \'' + str(self.load_directory) +
                                '/region.csv\' WITH DELIMITER AS \'|\'')
            self.cursor.execute('COPY nation FROM \'' + str(self.load_directory) +
                                '/nation.csv\' WITH DELIMITER AS \'|\'')
            self.cursor.execute('COPY customer FROM \'' + str(self.load_directory) +
                                '/customer.csv\' WITH DELIMITER AS \'|\'')
            self.cursor.execute('COPY orders FROM \'' + str(self.load_directory) +
                                '/orders.csv\' WITH DELIMITER AS \'|\'')
            self.cursor.execute('COPY part FROM \'' + str(self.load_directory) + '/part.csv\' WITH DELIMITER AS \'|\'')
            self.cursor.execute('COPY supplier FROM \'' + str(self.load_directory) +
                                '/supplier.csv\' WITH DELIMITER AS \'|\'')
            self.cursor.execute('COPY partsupp FROM \'' + str(self.load_directory) +
                                '/partsupp.csv\' WITH DELIMITER AS \'|\'')
            self.cursor.execute('COPY lineitem FROM \'' + str(self.load_directory) +
                                '/lineitem.csv\' WITH DELIMITER AS \'|\'')
            self.cursor.execute('COMMIT')
        except Exception as e:
            print(f'Erro ao carregar dados: {e}')
            self.cursor.execute('ABORT')


class DatabaseWorker(Thread):
    def __init__(self, query):
        Thread.__init__(self)
        self.query = query
        self.result = None

    def run(self):
        try:
            self.connection = psycopg2.connect("host=localhost dbname=tpch user=postgres")
            self.cursor = self.connection.cursor()
            self.cursor.execute('BEGIN')
            self.cursor.execute(self.query)
            print(self.cursor.fetchone())
            self.cursor.execute('COMMIT')
        except Exception as e:
            print(f'Erro ao executar thread: {e}')
            self.cursor.execute('ABORT')
