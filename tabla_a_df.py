# -*- coding: utf-8 -*-
"""
Created on Thu Jan 13 13:30:58 2022

@author: CONSULTOR
"""

from pyspark import SparkContext, SparkConf, HiveContext
import csv
from pyspark.sql.functions import count, desc , col, max , struct
import time
import sys
reload(sys)
sys.setdefaultencoding('utf8')

conf = SparkConf().setAppName("Data Frame Join")
sc = SparkContext.getOrCreate(conf=conf)
sc.setLogLevel("ERROR")
sqlContext = HiveContext(sc)

listening_df = sqlContext.table("listeningsr")
inicio = time.time()
listening_df.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

genre_df = sqlContext.table("genre")
inicio = time.time()
genre_df.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

listening_df = listening_df.drop('date')
listening_df = listening_df.na.drop()
'''
q0 = listening_df.select('artist', 'track')
inicio = time.time()
q0.show()
final = time.time()
print("Tiempo de impresion Q0: ", (final-inicio))

q1 = listening_df.select('*').filter(listening_df.artist == 'Rihanna')
inicio = time.time()
q1.show()
final = time.time()
print("Tiempo de impresion Q1: ", (final-inicio))

q2 = listening_df.select('user_id', 'artist').filter(listening_df.artist == 'Rihanna').groupby('user_id').agg(count('user_id').alias('count')).orderBy(desc('count')).limit(10)
inicio = time.time()
q2.select('user_id').show()
final = time.time()
print("Tiempo de impresion Q2: ", (final-inicio))

q3 = listening_df.select('artist', 'track').groupby('artist', 'track').agg(count('*').alias('count')).orderBy(desc('count')).limit(10)
inicio = time.time()
q3.show()
final = time.time()
print("Tiempo de impresion Q3: ", (final-inicio))

q4 = listening_df.select('artist', 'track').filter(listening_df.artist == 'Rihanna').groupby('artist', 'track').agg(count('*').alias('count')).orderBy(desc('count')).limit(10)
inicio = time.time()
q4.show()
final = time.time()
print("Tiempo de impresion Q4: ", (final-inicio))

q5 = listening_df.select('artist', 'album').groupby('artist', 'album').agg(count('*').alias('count')).orderBy(desc('count')).limit(10)
inicio = time.time()
q5.show()
final = time.time()
print("Tiempo de impresion Q5: ", (final-inicio))
'''
data = listening_df.join(genre_df, how = 'inner', on = ['artist'])
inicio = time.time()
data.show()
final = time.time()
print("Tiempo de impresion data: ", (final-inicio))
'''
q6 = data.select('user_id', 'genre').filter(data.genre == 'pop').groupby('user_id').agg(count('*').alias('count')).orderBy(desc('count')).limit(10)
inicio = time.time()
q6.select('user_id').show()
final = time.time()
print("Tiempo de impresion Q6: ", (final-inicio))

q7 = data.select('genre').groupby('genre').agg(count('*').alias('count')).orderBy(desc('count')).limit(10)
inicio = time.time()
q7.show()
final = time.time()
print("Tiempo de impresion Q7: ", (final-inicio))

q8_1 = data.select('user_id', 'genre').groupby('user_id', 'genre').agg(count('*').alias('count')).orderBy('user_id')
#inicio = time.time()
#q8_1.show()
#final = time.time()
#print("Tiempo de impresion Q8_1: ", (final-inicio))

q8_2 = q8_1.groupby('user_id').agg(max(struct(col('count'), col('genre'))).alias('max')).select(col('user_id'), col('max'))
inicio = time.time()
q8_2.show()
final = time.time()
print("Tiempo de impresion Q8_2: ", (final-inicio))

q9 = genre_df.select('genre').filter((col('genre')=='pop') | (col('genre') == 'rock') | (col('genre') == 'metal') | (col('genre') == 'hip hop')).groupby('genre').agg(count('genre').alias('count'))
inicio = time.time()
q9.show()
final = time.time()
print("Tiempo de impresion Q9: ", (final-inicio))
'''