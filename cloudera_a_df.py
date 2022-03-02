# -*- coding: utf-8 -*-
"""
Created on Tue Jan 18 20:45:02 2022

@author: CONSULTOR
"""

from pyspark import SparkContext, SparkConf, HiveContext
import datetime
import csv
from pyspark.sql.functions import count, desc , col, max , struct, year, lit, ntile, avg, udf, when, split
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
import time
import sys
import subprocess
reload(sys)
sys.setdefaultencoding('utf8')

conf = SparkConf().setAppName("Data Frame Join")
sc = SparkContext.getOrCreate(conf=conf)
sc.setLogLevel("ERROR")
sqlContext = HiveContext(sc)

vinos = sqlContext.table("vinos.vinos")

a = sqlContext.table("escuela.a")
m = sqlContext.table("escuela.m")
am = sqlContext.table("escuela.am")
colegiaturas = sqlContext.table("escuela.colegiaturas")

grab = sqlContext.table("musica.grab")
disco = sqlContext.table("musica.disco")

archivo = '/user/luis/consultadf_1'
subprocess.call(['hadoop', 'fs', '-mkdir', archivo])

'''

inicio = time.time()
vinos = vinos.select('nombre', 'produc').orderBy(desc('produc'))
w =  Window.partitionBy()
query = vinos.select('nombre', 'produc', ntile(4).over(w).alias("clasificacion"))
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))



inicio = time.time()
query = a.join(am, am.cta == a.cta).join(m, m.cve == am.cve).join(colegiaturas, colegiaturas.depto == m.depto).select('nomb', 'descri', 'cal', 'coleg')
w = Window.partitionBy(a.nomb)
query = query.withColumn('promedio', avg(am.cal).over(w))
query = query.withColumn("descuento", when(col('promedio') > 8.5, (col('coleg') * 0.5)).otherwise(col('coleg')))
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))



inicio = time.time()
split_col = split(grab['obra'], ' ')
query = grab.withColumn('nombre_obra', split_col.getItem(0))
split_col = split(grab['interprete'], ' ')
year = grab.agg(avg('durac')).head()[0]
query = query.withColumn('nombre_interprete', split_col.getItem(0)).select('nombre_obra', 'nombre_interprete', 'durac')
query = query.filter(query.durac > year)
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

'''

inicio = time.time()
query = a.join(am, am.cta == a.cta).join(m, m.cve == am.cve).select('nomb', 'descri', 'cal').orderBy(desc('nomb'))
w = Window.partitionBy(a.nomb).orderBy(am.cal).rowsBetween(-1, 2)
query = query.withColumn('promedio_movil', avg(am.cal).over(w))
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

'''

#query = sqlContext.sql("insert into particiones_estaticas_luis.disco_particion_estatica partition(decada = '90s') select * from musica.disco where a_grab between 1989 and 2000")
#query = sqlContext.sql("insert into particiones_estaticas_luis.disco_particion_estatica partition(decada = '80s') select * from musica.disco where a_grab between 1979 and 1990")
#query = sqlContext.sql("insert into particiones_estaticas_luis.disco_particion_estatica partition(decada = '70s') select * from musica.disco where a_grab between 1969 and 1980")
#query = sqlContext.sql("show partitions particiones_estaticas_luis.disco_particion_estatica")

inicio = time.time()
query = sqlContext.sql("create database if not exists particiones_estaticas_luis2")
query = sqlContext.sql("create table if not exists particiones_estaticas_luis2.disco_particion_estatica(cat string, a_grab int, precio float, tipo string) partitioned by (decada string) row format  delimited fields terminated by',' stored as textfile")
disco_part_est = sqlContext.table("particiones_estaticas_luis2.disco_particion_estatica")
query1 = disco.filter((disco.a_grab > 1989) & (disco.a_grab < 2000))
query2 = disco.filter((disco.a_grab > 1979) & (disco.a_grab < 1990))
query3 = disco.filter((disco.a_grab > 1969) & (disco.a_grab < 1980))

query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))



inicio = time.time()
query = sqlContext.sql("create database if not exists particiones_dinamicas_luis2")
query = sqlContext.sql("create table if not exists particiones_dinamicas_luis2.disco_particion_dinamica(cat string, a_grab int, precio float) partitioned by (tipo string) row format delimited fields terminated by ',' stored as textfile")
query = sqlContext.sql("SET hive.exec.dynamic.partition.mode=nonstrinct")
#query = sqlContext.sql("insert into particiones_dinamicas_luis2.disco_particion_dinamica partition (tipo) select * from musica.disco")
#query = sqlContext.sql("show partitions particiones_dinamicas_luis.disco_particion_dinamica")
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

'''