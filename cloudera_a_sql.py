# -*- coding: utf-8 -*-
"""
Created on Wed Jan 19 14:20:49 2022

@author: CONSULTOR
"""

from pyspark import SparkContext, SparkConf, HiveContext
import subprocess
import os
#from subprocess import call
from pyspark.sql import DataFrameWriter
import time
import sys
reload(sys)
sys.setdefaultencoding('utf8')

# create Spark context with Spark configuration
conf = SparkConf().setAppName("Data Frame Join")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

'''

query = sqlContext.sql("select nombre, produc, ntile(4) over (order by produc is null asc) as clasificacion from vinos.vinos")
inicio = time.time()
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))



query = sqlContext.sql("select nomb, descri, cal, coleg, avg(cal) over(partition by nomb) as promedio, if(avg(cal) over(partition by nomb) > 8.5, coleg*0.5, coleg) as coleg_desceunto from escuela.a join escuela.am on am.cta = a.cta join escuela.m on m.cve = am.cve join escuela.colegiaturas on colegiaturas.depto = m.depto")
inicio = time.time()
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))



year = sqlContext.sql('select avg(durac) from musica.grab').head()[0]
query = sqlContext.sql("select split(obra, ' ')[0] as nombre_obra, split(interprete, ' ')[0] as nombre_interprete, durac as duracion from musica.grab where durac > " + str(year))
inicio = time.time()
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))



inicio = time.time()
query = sqlContext.sql("select nomb, descri, cal, avg(cal) over(partition by nomb order by cal rows between 1 preceding and 2 following) from escuela.a join escuela.am on a.cta = am.cta join escuela.m on m.cve = am.cve")
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))



archivo = '/user/luis/consultasql_1'
subprocess.call(['hadoop', 'fs', '-mkdir', archivo])

inicio = time.time()
comando = "beeline -n luis -u jdbc:hive2://localhost:10000 -e \"INSERT OVERWRITE DIRECTORY '/user/luis/consultasql_1' select * from escuela.administrativos where sueldo > 3000 and nomb like 'M%'\""
exc = os.system(comando)
comando = " hdfs dfs -cat /user/luis/consultasql_1/000000_0"
exc = os.system(comando)
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

''' 

#direc = '/user/luis/consultasql_2'
#subprocess.call(['hadoop', 'fs', '-mkdir', direc])

inicio = time.time()
query = sqlContext.sql("select * from musica.aut where nacion = 'MEXICO' and nombre REGEXP \"^[A-D]\"")
query.show()
#query.write.save('/user/luis/consultasql_2/', format='parquet', mode='append')
#df1 = sqlContext.createDataFrame(query)
query.write.csv("/user/luis/consultasql_2/result.csv", mode='append')
#subprocess.call(['hadoop', 'fs', '-get', '/home/luis/consultas/result_sql_1'])
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

'''

inicio = time.time()
query = sqlContext.sql("create database if not exists particiones_estaticas_luis")
query = sqlContext.sql("create table if not exists particiones_estaticas_luis.disco_particion_estatica(cat string, a_grab int, precio float, tipo string) partitioned by (decada string) row format  delimited fields terminated by',' stored as textfile")
query = sqlContext.sql("insert into particiones_estaticas_luis.disco_particion_estatica partition(decada = '90s') select * from musica.disco where a_grab between 1989 and 2000")
query = sqlContext.sql("insert into particiones_estaticas_luis.disco_particion_estatica partition(decada = '80s') select * from musica.disco where a_grab between 1979 and 1990")
query = sqlContext.sql("insert into particiones_estaticas_luis.disco_particion_estatica partition(decada = '70s') select * from musica.disco where a_grab between 1969 and 1980")
query = sqlContext.sql("show partitions particiones_estaticas_luis.disco_particion_estatica")
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))



inicio = time.time()
query = sqlContext.sql("create database if not exists particiones_dinamicas_luis")
query = sqlContext.sql("create table if not exists particiones_dinamicas_luis.disco_particion_dinamica(cat string, a_grab int, precio float) partitioned by (tipo string) row format delimited fields terminated by ',' stored as textfile")
query = sqlContext.sql("SET hive.exec.dynamic.partition.mode=nonstrinct")
query = sqlContext.sql("insert into particiones_dinamicas_luis.disco_particion_dinamica partition (tipo) select * from musica.disco")
query = sqlContext.sql("show partitions particiones_dinamicas_luis.disco_particion_dinamica")
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

'''