# -*- coding: utf-8 -*-
"""
Created on Mon Jan 17 13:45:04 2022

@author: CONSULTOR
"""

from pyspark import SparkContext, SparkConf, HiveContext
import time
import sys
reload(sys)
sys.setdefaultencoding('utf8')

# create Spark context with Spark configuration
conf = SparkConf().setAppName("Data Frame Join")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

print("LAB 1 ejercicios 15 y 16")
query = sqlContext.sql("select N.NOMBRE, V.NOMBRE, SUPERF, (PRODUC * 12)  from vinos.vinedo V, vinos.vinos N where V.SUPERF > 10000 and N.tipo = 'TINTO' and PRODUC > 10000 and N.vinedo = V.nombre")
inicio = time.time()
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

query = sqlContext.sql("select a.depto, nomb, descri, cal from a join m on a.depto = m.depto join am on am.cta = a.cta where m.cve = am.cve order by a.depto, nomb;")
inicio = time.time()
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

print("LAB 2 ejercicios 8 y 9")
query = sqlContext.sql("select distinct substring(NACION, 1, 2), substring(NACION, 1, 4), NACION from musica.aut")
inicio = time.time()
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

query = sqlContext.sql("select nombre, uva, tipo, cast(produc/12 as decimal (6, 2)) from vinos.vinos where uva like 'C%' and uva like '%N%N%' order by 5 asc")
inicio = time.time()
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

print("LAB 3 ejercicios 4 y 5")
query = sqlContext.sql("select year(f_nac) as year, 'NACIO UN MUSICO NACIONALISTA' from musica.aut where genero = 'NACIONALISTA' union all select a_crea as year, 'SE ESCRIBRIO UNA OBRA NACIONALISTA' from musica.obra, musica.aut where aut.nombre = obra.autor and aut.genero = 'NACIONALISTA' ")
inicio = time.time()
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

query = sqlContext.sql("select * from musica.obra, musica.aut where a_crea > year(f_nac) and aut.nombre = 'RACHMANINOFF'")
inicio = time.time()
query.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))