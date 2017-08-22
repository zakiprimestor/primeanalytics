from mapr_streams_python import Consumer, KafkaError
from sys import argv
import multiprocessing
from pychbase import Connection,Table
from time import sleep
#from ast import literal_eval

'''
def mp_worker(msg):
		try:
				except:
			print 'Insert to maprdb failed. '
'''
def main():
	#Consumer init
	consumer = Consumer({'group.id': 'maprdb-1','default.topic.config': {'auto.offset.reset': 'earliest'}})
	consumer.subscribe(['/zaki-stream:'+argv[1]])
	#consumer.seek(0,2)
	#connection to db
	conn = Connection()
	#conn.create_table('mstohive1',{'smsg':{}})
	table = conn.table('mstohive1')
	while True:
		msg_db = {}
		str_db = ''
		msg = consumer.poll(timeout=2.0)
		if msg is None: 
			continue
		if not msg.error():
			#msg_db = literal_eval(str(msg.value()))
			str_db = msg.value()
			str_db_1 = str_db.replace('{','').replace('}','').split(",")
			for i in str_db_1:
				k,v = i.split(':')
				msg_db[k.replace('"',"").strip()] =  v.replace('"',"").strip()
			#table.put(str(msg_db['id']),{"smsg:usia": str(msg_db['usia']),"smsg:lokasi":msg_db['lokasi'],"smsg:produk":msg_db['produk']})
			print 'Received message: %s' % (str_db, )
			print 'Dictionary Created %s' % (str(msg_db),)
			table.put(msg_db['id'],{"smsg:usia": msg_db['usia'],"smsg:lokasi":msg_db['lokasi'],"smsg:produk":msg_db['produk']})
			#print 'Record Dengan ID %s Terbentuk di table' % (msg_db['id'], )
			#sleep(0.1)
		elif msg.error().code() != KafkaError._PARTITION_EOF:
			print(msg.error())
		
	consumer.close()
	
if __name__ == "__main__":
	main()


