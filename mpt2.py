import time, os
from mapr_streams_python import Producer

#Set the filename and open the file
filename = 'json_toko2.json'
file = open(filename,'r')
producer =  Producer({'streams.producer.default.stream': '/zaki-stream'})
topic = "tokotopic2"

#Find the size of the file and move to the end
st_results = os.stat(filename)
st_size = st_results[6]
file.seek(st_size)

while 1:
    where = file.tell()
    line = file.readline()
    if not line:
        time.sleep(1)
        file.seek(where)
    else:
        print line, # already has newline
	producer.produce(topic,line)