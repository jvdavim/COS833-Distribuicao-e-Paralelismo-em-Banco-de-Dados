import psycopg2

class TPCH:
    def __init__(self):
        self.conn = psycopg2.connect("host=localhost dbname=tpch user=postgres")
        self.cur = conn.cursor()
    
    def load_data_from_dir(self, dir):
        self.cur.execute('COPY customer FROM ' + str(dir) + '/customer.csv WITH DELIMITER AS \'|\'')
        self.cur.execute('COPY lineitem FROM ' + str(dir) + '/lineitem.csv WITH DELIMITER AS \'|\'')
        self.cur.execute('COPY nation FROM ' + str(dir) + '/nation.csv WITH DELIMITER AS \'|\'')
        self.cur.execute('COPY orders FROM ' + str(dir) + '/orders.csv WITH DELIMITER AS \'|\'')
        self.cur.execute('COPY part FROM ' + str(dir) + '/part.csv WITH DELIMITER AS \'|\'')
        self.cur.execute('COPY partsupp FROM ' + str(dir) + '/partsupp.csv WITH DELIMITER AS \'|\'')
        self.cur.execute('COPY region FROM ' + str(dir) + '/region.csv WITH DELIMITER AS \'|\'')
        self.cur.execute('COPY supplier FROM ' + str(dir) + '/supplier.csv WITH DELIMITER AS \'|\'')