# -*- coding: utf-8 -*-
"""
Created on Thu Jan 13 16:09:29 2022

@author: CONSULTOR
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc , col, max, struct
import time

spark = SparkSession.builder.appName('spark_app').getOrCreate()

features_df = spark.read.format("com.databricks.spark.csv").option('inferSchema', True).option('header', True).load("features.csv")
labels_df = spark.read.format('com.databricks.spark.csv').option('inferSchema', True).option('header', True).load("labels.csv")

features_df.registerTempTable('features')
labels_df.registerTempTable('labels')

inicio = time.time()
spark.sql("select * from features").show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

inicio = time.time()
spark.sql("select * from labels").show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

inicio = time.time()
spark.sql("select * from labels join features on labels.id = features.id").show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

inicio = time.time()
spark.sql("select count(*) from labels join features on labels.id = features.id group by features.recorded_by").show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

inicio = time.time()
spark.sql("select count(*) as count from labels join features on labels.id = features.id group by features.water_quality order by count desc").show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))

#inicio = time.time()
#spark.sql("select sum(amount_tsh) from labels join features on labels.id = features.id group by features.status_group").show()
#final = time.time()
#print("Tiempo de impresion: ", (final-inicio))

inicio = time.time()
spark.sql("select population, count(*) as count from labels join features on labels.id = features.id group by features.population order by population asc").show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))  

inicio = time.time()
spark.sql("select if(isnull(case when population < 1 then null else population end as population), ) , count(*) as count from labels join features on labels.id = features.id group by features.population order by population asc").show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))  