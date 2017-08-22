from mapr_streams_python import Consumer, KafkaError
from sys import argv
import multiprocessing
from pychbase import Connection,Table

class MyConsumer(multiprocessing.Process):

	def __init__(self,tableObj):
		self table = tableObj
		threading.Thread.__init__(self)

	def run(self):
		consumer = Consumer({'group.id': 'maprdb-1','default.topic.config': {'auto.offset.reset': 'latest'}})
		consumer.subscribe(['/zaki-stream:'+argv[1]])
		#consumer.seek(0,2)

		while True:
		  msg = consumer.poll(timeout=1.0)
		  if msg is None: continue
		  if not msg.error():
			print('Received message: %s' % msg.value())
			#maprdb routines
			
		  elif msg.error().code() != KafkaError._PARTITION_EOF:
			print(msg.error())
			running = False
		consumer.close()

def main():
	#connection to db
	conn = Connection()
	table = conn.table('ms_ex_product_table')
	
	MyConsumer().start()
	
if __name__ == "__main__":
	main()