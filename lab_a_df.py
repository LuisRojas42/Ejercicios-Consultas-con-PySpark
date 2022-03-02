# -*- coding: utf-8 -*-
"""
Created on Mon Jan 17 15:32:58 2022

@author: CONSULTOR
"""

from pyspark import SparkContext, SparkConf, HiveContext
import datetime
import csv
from pyspark.sql.functions import count, desc , col, max , struct, year, lit
import time
import sys
reload(sys)
sys.setdefaultencoding('utf8')

conf = SparkConf().setAppName("Data Frame Join")
sc = SparkContext.getOrCreate(conf=conf)
sc.setLogLevel("ERROR")
sqlContext = HiveContext(sc)

vinos = sqlContext.table("vinos.vinos")
vinos = vinos.withColumnRenamed('nombre', 'nombre_vino')
vinedo = sqlContext.table("vinos.vinedo")

a = sqlContext.table("escuela.a")
a = a.withColumnRenamed('depto', 'depto_a')
a = a.withColumnRenamed('cta', 'cta_a')
m = sqlContext.table("escuela.m")
m = m.withColumnRenamed('cve', 'cve_m')
am = sqlContext.table("escuela.am")

aut = sqlContext.table("musica.aut")
obra = sqlContext.table("musica.obra")

'''
#data = vinos.withColumn("id", col("vinedo")).join(vinedo.withColumn("id", col("nombre")), on="id").withColumn('vinos.produc', col('vinos.produc') * 12).filter((vinos.tipo == 'TINTO' & vinos.produc > 10000 & vinedo.superf > 10000)).select("vinos.nombre", "vinedo.nombre", "vinedo.superf", "vinos.produc")
inicio = time.time()
data = vinedo.join(vinos, vinedo.nombre == vinos.vinedo)
query = data.select('nombre', 'nombre_vino', 'superf', 'produc', 'tipo').withColumn('produc', (col('produc') * 12))
query = query.filter((query.tipo == 'TINTO') & (query.produc > 10000) & (query.superf > 10000))
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

inicio = time.time()
query = a.join(m, a.depto_a == m.depto).join(am, ((a.cta_a == am.cta) & (m.cve_m == am.cve))).select('depto', 'nomb', 'descri', 'cal').orderBy(['depto', 'nomb'])
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

inicio = time.time()
query = aut.select(col('nacion').substr(1, 2), col('nacion').substr(1, 4), 'nacion')
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

inicio = time.time()
query = vinos.select('nombre_vino', 'uva', 'tipo', 'produc').withColumn('produc', (col('produc') / 12))
query = query.filter((col('uva').like('C%N%N%'))).orderBy('produc')
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

inicio = time.time()
query = aut.select(year('f_nac').alias('year'), 'genero').withColumn("evento", lit('NACIO UN NACIONALISTA'))
query = query.filter(query.genero == 'NACIONALISTA').select('year', 'evento')
query_2 = aut.join(obra, ((aut.nombre == obra.autor) & (aut.genero == 'NACIONALISTA')))
query_2 = query_2.select(col('a_crea').alias('year')).withColumn("evento", lit('SE ESCRIBIO UNA OBRA NACIONALISTA'))
query_3 = query.unionAll(query_2)
query_3.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))
'''

inicio = time.time()
year = aut.filter(aut.nombre == 'RACHMANINOFF').select(year('f_nac')).head()[0]
query = obra.filter(obra.a_crea > year)
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))
