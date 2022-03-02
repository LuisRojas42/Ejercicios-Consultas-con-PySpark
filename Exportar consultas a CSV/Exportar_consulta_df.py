# -*- coding: utf-8 -*-
"""
Created on Tue Feb  1 16:27:51 2022

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

listening_df = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", True).option("header", True).load("export.csv")
inicio = time.time()
listening_df.show()
final = time.time()
print("Tiempo de impresion: ", (final-inicio))