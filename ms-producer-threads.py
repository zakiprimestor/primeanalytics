#import
from mapr_streams_python import Producer
import threading, logging, time

#threadLimiter = threading.BoundedSemaphore(250)

class MyProducer(threading.Thread):
	def __init__(self,line,topic):
		super(MyProducer, self).__init__()
		self.topic = topic
		self.line = line
	def run(self):
		#threadLimiter.acquire()
		#try:
		print self.line
		producer.produce(self.topic,self.line)
		#finally:
		#	threadLimiter.release()

#variable declare
def follow(thefile):
	thefile.seek(0,2)
	while True:
		line =  thefile.readline()
		while True:
			line =  thefile.readline()
			if not line:
				time.sleep(0.01)
				continue
			yield line
def main(line,topic):
	MyProducer(line,topic).start()
	
if __name__ == "__main__":
	producer =  Producer({'streams.producer.default.stream': '/zaki-stream'})
	topic = "tokotopic"
	json_f = open("json_toko.json","r")
	loglines= follow(json_f)
	for line in loglines:
		#logic
		main(line,topic)
	#closing_routines
	producer.flush()
	json_f.close()
	
