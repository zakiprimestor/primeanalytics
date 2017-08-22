#import
from mapr_streams_python import Producer
from random import randint
from time import sleep          
#variable declare
res_product = ['pampers','tooth_paste','detergen','cloth','soda','milk','cheese','cookies','rice','tools']
res_location = ['jakarta','depok','tangerang','bogor','bekasi']
json_f = open('json_toko.json','w+')
#logic

for i in range(1,5000001):
	usia = randint(25,65)
	lokasi = res_location[randint(0,len(res_location)-1)]
	produk = res_product[randint(0,len(res_product)-1)]
	
	json_f.write('{"id": "' + str(i) + '","usia": "' + str(usia) + '","lokasi" : "' + lokasi + '","produk" : "' + produk + '"}')
	json_f.write('\n')
	#mapr stream producer embedded on script log.
	producer =  Producer({'streams.producer.default.stream': '/zaki-stream'})
        topic = "tokotopic"
	#send message to broker
	producer.produce(topic,'{"id": "' + str(i) + '","usia": "' + str(usia) + '","lokasi" : "' + lokasi + '","produk" : "' + produk + '"}')
	#send to terminal
	print '{"id": "' + str(i) + '","usia": "' + str(usia) + '","lokasi" : "' + lokasi + '","produk" : "' + produk + '"}\n'
#closing routines
json_f.close()

