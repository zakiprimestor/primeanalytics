#import
from random import randint
#variable declare
res_product = ['pampers','tooth_paste','detergen','cloth','soda','milk','cheese','cookies','rice','tools']
res_location = ['jakarta','depok','tangerang','bogor','bekasi']
json_f = open('json_toko2.json','w+')
#logic

for i in range(0,5000000):
	usia = randint(25,65)
	lokasi = res_location[randint(0,len(res_location)-1)]
	produk = res_product[randint(0,len(res_product)-1)]
	
	json_f.write('{"id": "' + str(i) + '","usia": "' + str(usia) + '","lokasi" : "' + lokasi + '","produk" : "' + produk + '"}')
	json_f.write('\n')
	
	print '{"id": "' + str(i) + '","usia": "' + str(usia) + '","lokasi" : "' + lokasi + '","produk" : "' + produk + '"}\n'

#closing routines
json_f.close()

