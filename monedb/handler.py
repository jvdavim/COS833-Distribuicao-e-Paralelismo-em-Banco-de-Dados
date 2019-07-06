import pymonetdb
import time

def runAndMeasure(cursor, query):
    start = time.time()
    cursor.execute(query)
    cursor.fetchall()
    end = time.time()
    return (end - start)

connection = pymonetdb.connect(
    username="voc", password="voc", hostname="localhost", database="voc"
    )

cursor = connection.cursor()
query = 'SELECT * FROM voc.craftsmen;'
result = runAndMeasure(cursor, query)
print(result)


runAndMeasure(cursor, query)