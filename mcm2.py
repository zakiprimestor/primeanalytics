from mapr_streams_python import Consumer, KafkaError
from sys import argv
import multiprocessing
from pychbase import Connection,Table
from time import sleep
def main():
	#Consumer init
	consumer = Consumer({'group.id': 'maprdb-1','default.topic.config': {'auto.offset.reset': 'earliest'}})
	consumer.subscribe(['/zaki-stream:'+argv[1]])
	#consumer.seek(0,2)
	#connection to db
	conn = Connection()
	#conn.create_table('mstohive2',{'smsg':{}})
	table = conn.table('mstohive2')
	while True:
		msg_db = {}
		str_db = ''
		msg = consumer.poll(timeout=2.0)
		if msg is None: 
			continue
		if not msg.error():
			try:
				str_db = msg.value()
				str_db_1 = str_db.replace('{','').replace('}','').split(",")
				for i in str_db_1:
					k,v = i.split(':')
					msg_db[k.replace('"',"").strip()] =  v.replace('"',"").strip()
				print 'Received message: %s' % (str_db, )
				print 'Dictionary Created %s' % (str(msg_db),)
				table.put(msg_db['id'],{"smsg:usia": msg_db['usia'],"smsg:lokasi":msg_db['lokasi'],"smsg:produk":msg_db['produk']})
			except:
				print str_db
		elif msg.error().code() != KafkaError._PARTITION_EOF:
			print(msg.error())
		
	consumer.close()
	
if __name__ == "__main__":
	main()


