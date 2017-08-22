from kafka import KafkaClient, KafkaConsumer
from sys import argv
'''
kafka  =  KafkaClient("127.0.0.1:6667")
consumer = SimpleConsumer(kafka, "my-group", argv[1])
consumer.max_buffer_size=0
consumer.seek(0,2)

for msg in consumer:
	print "OFFSET: "+str(msg[0])+"\t MSG: "+str(msg[1][3])
'''
consumer = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='earliest')
consumer.subscribe([argv[1]])
#consumer.seek(0,2)

for message in consumer:
	print message