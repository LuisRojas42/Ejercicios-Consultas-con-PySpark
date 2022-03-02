# -*- coding: utf-8 -*-
"""
Created on Wed Jan 12 12:38:30 2022

@author: CONSULTOR

 ../spark2/bin/spark-submit csv_a_sql.py 2>prg.out

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc , col, max, struct
import time
import sys
reload(sys)
sys.setdefaultencoding('utf8')

spark = SparkSession.builder.appName('spark_app').getOrCreate()

listening_df = spark.read.format("com.databricks.spark.csv").option('inferSchema', True).option('header', True).load("../listenings.csv")
genre_df = spark.read.format('com.databricks.spark.csv').option('inferSchema', True).option('header', True).load("../genre.csv")

listening_df.registerTempTable('listenings')
genre_df.registerTempTable('genre')
'''
inicio = time.time()
spark.sql("select * from listenings").show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

inicio = time.time()
spark.sql("select * from genre").show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

inicio = time.time()
spark.sql("select artist, track from listenings").show()
final = time.time()
print("Tiempo de impresion Q0: ", (final-inicio))

inicio = time.time()
spark.sql("select * from listenings where artist = 'Rihanna'").show()
final = time.time()
print("Tiempo de impresion Q1: ", (final-inicio))

inicio = time.time()
spark.sql("select user_id, count(user_id) as count from listenings where artist == 'Rihanna' group by user_id order by count desc limit 10").show()
final = time.time()
print("Tiempo de impresion Q2: ", (final-inicio))

inicio = time.time()
spark.sql("select artist, track, count(*) as count from listenings group by artist, track order by count desc limit 10").show()
final = time.time()
print("Tiempo de impresion Q3: ", (final-inicio))

inicio = time.time()
spark.sql("select artist, track, count(*) as count from listenings where artist == 'Rihanna' group by artist, track order by count desc limit 10").show()
final = time.time()
print("Tiempo de impresion Q4: ", (final-inicio))

inicio = time.time()
spark.sql("select artist, album, count(*) as count from listenings group by artist, album order by count desc limit 10").show()
final = time.time()
print("Tiempo de impresion Q5: ", (final-inicio))

inicio = time.time()
spark.sql("select * from listenings l inner join genre g on l.artist = g.artist").show()
final = time.time()
print("Tiempo de impresion data: ", (final-inicio))

inicio = time.time()
spark.sql("select user_id, count(*) as count from listenings l inner join genre g on l.artist = g.artist where g.genre == 'pop' group by l.user_id order by count desc limit 10").show()
final = time.time()
print("Tiempo de impresion Q6: ", (final-inicio))

inicio = time.time()
spark.sql("select genre, count(*) as count from listenings l inner join genre g on l.artist = g.artist group by g.genre order by count desc limit 10").show()
final = time.time()
print("Tiempo de impresion Q7: ", (final-inicio))
'''
#inicio = time.time()
#q8_1 = data.select('user_id', 'genre').groupby('user_id', 'genre').agg(count('*').alias('count')).orderBy('user_id')
# select user_id, genre, count(*) as count from listenings l inner join genre g on l.artist = g.artist group by usr_id, genre order by user_id
#q8_1 = spark.sql("select user_id, genre, count(*) as count from listenings l inner join genre g on l.artist = g.artist group by l.user_id, g.genre order by user_id").show()
#final = time.time()
#print("Tiempo de impresion Q8_1: ", (final-inicio))

#q8_2 = q8_1.groupby('user_id').agg(max(struct(col('count'), col('genre'))).alias('max')).select(col('user_id'), col('max'))
# select max(count) as max, user_id from (select user_id, genre, count(*) as count from listenings l inner join genre g on l.artist = g.artist group by l.user_id, g.genre order by user_id) group by user_id
inicio = time.time()
spark.sql("select max(count) as max, user_id from (select user_id, genre, count(*) as count from listenings l inner join genre g on l.artist = g.artist group by l.user_id, g.genre order by user_id) group by user_id").show()
final = time.time()
print("Tiempo de impresion Q8_2: ", (final-inicio))
'''
inicio = time.time()
spark.sql("select genre, count(genre) as count from genre where genre in ('pop', 'rock', 'metal', 'hip hop') group by genre").show()
final = time.time()
print("Tiempo de impresion Q9: ", (final-inicio))
'''