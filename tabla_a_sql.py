# -*- coding: utf-8 -*-
"""
Created on Tue Jan 11 13:59:16 2022

@author: CONSULTOR

spark-submit tabla_a_sql.py 2>prg.out

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

'''
listening_df = sqlContext.sql("select * from listenings")
inicio = time.time()
listening_df.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

genre_df = sqlContext.sql("select * from genre")
inicio = time.time()
genre_df.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

q0 = sqlContext.sql("select artist, track from listenings")
inicio = time.time()
q0.show()
final = time.time()
print("Tiempo de impresion Q0: ", (final-inicio))

q1 = sqlContext.sql("select * from listenings where artist = 'Rihanna'")
inicio = time.time()
q1.show()
final = time.time()
print("Tiempo de impresion Q1: ", (final-inicio))

#q2 = listening_df.select('user_id').filter(listening_df.artist == 'Rihanna').groupby('user_id').agg(count('user_id').alias('count')).orderBy(desc('count')).limit(10)
q2 = sqlContext.sql("select user_id, count(user_id) as count from listenings where artist == 'Rihanna' group by user_id order by count desc limit 10")
inicio = time.time()
q2.show()
final = time.time()
print("Tiempo de impresion Q2: ", (final-inicio))

#q3 = listening_df.select('artist', 'track').groupby('artist', 'track').agg(count('*').alias('count')).orderBy(desc('count')).limit(10)
q3 = sqlContext.sql("select artist, track, count(*) as count from listenings group by artist, track order by count desc limit 10")
inicio = time.time()
q3.show()
final = time.time()
print("Tiempo de impresion Q3: ", (final-inicio))

#q4 = listening_df.select('artist', 'track').filter(listening_df.artist == 'Rihanna').groupby('artist', 'track').agg(count('*').alias('count')).orderBy(desc('count')).limit(10)
q4 = sqlContext.sql("select artist, track, count(*) as count from listenings where artist == 'Rihanna' group by artist, track order by count desc limit 10")
inicio = time.time()
q4.show()
final = time.time()
print("Tiempo de impresion Q4: ", (final-inicio))

#q5 = listening_df.select('artist', 'album').groupby('artist', 'album').agg(count('*').alias('count')).orderBy(desc('count')).limit(10)
q5 = sqlContext.sql("select artist, album, count(*) as count from listenings group by artist, album order by count desc limit 10")
inicio = time.time()
q5.show()
final = time.time()
print("Tiempo de impresion Q5: ", (final-inicio))

'''
#data = listening_df.join(genre_df, how = 'inner', on = ['artist'])
data = sqlContext.sql("select * from listenings l inner join genre g on l.artist = g.artist")
inicio = time.time()
data.show()
final = time.time()
print("Tiempo de impresion data: ", (final-inicio))
'''
#q6 = data.select('user_id').filter(data.genre == 'pop').groupby('user_id').agg(count('*').alias('count')).orderBy(desc('count')).limit(10)
q6 = sqlContext.sql("select user_id, count(*) as count from listenings l inner join genre g on l.artist = g.artist where g.genre == 'pop' group by l.user_id order by count desc limit 10")
inicio = time.time()
q6.show()
final = time.time()
print("Tiempo de impresion Q6: ", (final-inicio))

#q7 = data.select('genre').groupby('genre').agg(count('*').alias('count')).orderBy(desc('count')).limit(10)
q7 = sqlContext.sql("select genre, count(*) as count from listenings l inner join genre g on l.artist = g.artist group by g.genre order by count desc limit 10")
inicio = time.time()
q7.show()
final = time.time()
print("Tiempo de impresion Q7: ", (final-inicio))

#q8_1 = data.select('user_id', 'genre').groupby('user_id', 'genre').agg(count('*').alias('count')).orderBy('user_id')
q8_1 = sqlContext.sql("select user_id, genre, count(*) as count from listenings l inner join genre g on l.artist = g.artist group by l.user_id, g.genre order by count desc limit 10")
inicio = time.time()
q8_1.show()
final = time.time()
print("Tiempo de impresion Q8_1: ", (final-inicio))
'''
#q8_2 = q8_1.groupby('user_id').agg(max(struct(col('count'), col('genre'))).alias('max')).select(col('user_id'), col('max'))
q8_2 = sqlContext.sql("select user_id, max(count) as max, genre from (select user_id, genre, count(*) as count from listenings l inner join genre g on l.artist = g.artist group by l.user_id, g.genre order by user_id) group by user_id")
inicio = time.time()
q8_2.show()
final = time.time()
print("Tiempo de impresion Q8_2: ", (final-inicio))
'''
#q9 = genre_df.select('genre').filter((col('genre')=='pop') | (col('genre') == 'rock') | (col('genre') == 'metal') | (col('genre') == 'hip hop')).groupby('genre').agg(count('genre').alias('count'))
q9 = sqlContext.sql("select genre, count(genre) as count from genre where genre in ('pop', 'rock', 'metal', 'hip hop') group by genre")
inicio = time.time()
q9.show()
final = time.time()
print("Tiempo de impresion Q9: ", (final-inicio))
'''