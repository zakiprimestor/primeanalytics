from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import *
from pyspark.sql import *
from pychbase import Connection,Table

appName = "Streaming using spark streaming example"
config = SparkConf().setAppName(appName)  


props = []
props.append(("spark.rememberDuration","10"))
props.append(("spark.batchDuration","10"))
props.append(("spark.eventLog.enabled","true"))
props.append(("spark.streaming.timeout","30"))
props.append(("spark.ui.enabled","true"))

config = config.setAll(props)

sc = SparkContext(conf=config)
ssc = StreamingContext(sc, int(config.get("spark.batchDuration")))

def runApplication(ssc, config):
	ssc.start()
	if config.get("spark.streaming.timeout") == '':
		ssc.awaitTermination()
	else:
		stopped = awaitTerminationOrTimeout(int(conifg.get("spark.streaming.timeout")))
	if not stopped:
		print "Stopping streaming context after timeout..."
		ssc.stop(True)
		print "Streaming context stopped."

print
print( "APPNAME:" + config.get( "spark.app.name" ))
print( "APPID:" +   sc.applicationId)
print( "VERSION:" + sc.version)
print

hbase_table = "spark-streaming-table-1"
conn = Connection()
table = conn.table(hbase_table)

topic = ["/zaki-stream:sparkqu"]
k_params = {"key.deserializer" : "org.apache.kafka.common.serialization.StringDeserializer","value.deserializer" : "org.apache.kafka.common.serialization.StringDeserializer","session.timeout.ms" : "45","group.id" : "Kafka_MapR-Streams_to_HBase"}


def SaveToHBase(rdd):
	print "=====Pull From Stream====="
	if not rdd.empty():
		print "=Some Records..=="
		for line in rdd.collect():
			table.put(line.id,{'smsg:lokasi':line.lokasi,'smsg:usia':line.usia,'smsg:produk':line.produk})
def ConvertToDict(str_msg):
	str_d = str_msg.replace('{','').replace('}','').split(",")
	msg_d = {}
        for i in str_d:
        	k,v = i.split(':')
        	msg_d[k.replace('"',"").strip()] =  v.replace('"',"").strip()
	return msg_d
def printMsg(x):
	print str(x)

kds = KafkaUtils.createDirectStream(ssc,topic,k_params,fromOffsets=None)

parsed = kds.filter(lambda x: printMsg(x))
parsed = kds.filter(lambda x: x != None and len(x) > 0)
parsed = parsed.map(lambda x: x[1])
parsed = parsed.map(lambda rec: ConvertToDict(str(rec)))
#parsed = parsed.filter(lambda x: x != None and len(x) == 4)
parsed = parsed.map(lambda data: Row(id=getValue(str,data['id']),lokasi=getValue(str,data['lokasi']),usia=getValue(str,data['usia']),produk=getValue(str,data['produk'])))

parsed.foreachRDD(SaveToHBase)

# Start App

runApplication(ssc, config)

print '======='
print "SUCCESS"
print '=======' 
