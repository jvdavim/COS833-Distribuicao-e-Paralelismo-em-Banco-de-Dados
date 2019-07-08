import postgres
import time
import os

query3 = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '3.sql'), "r").read()
query6 = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '6.sql'), "r").read()
query10 = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '10.sql'), "r").read()

worker1 = postgres.DatabaseWorker([query3, query6, query10])
# worker2 = postgres.DatabaseWorker(query)

worker1.start()
# worker2.start()

workers = []
workers.append(worker1)
# workers.append(worker2)

for worker in workers:
    worker.join()

print("Terminated")

# self.folder = os.path.dirname(os.path.abspath(__file__))
# self.query3 = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), '3.sql'), "r").read()
# self.query6 = open(os.path.join(self.folder, '6.sql'), "r").read()
# self.query10 = open(os.path.join(self.folder, '10.sql'), "r").read()
