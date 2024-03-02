# -*- coding: utf-8 -*-
'''
@author: naga
'''

import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

spark = SparkSession.builder.appName('Examples.com').getOrCreate()
rdd = spark.sparkContext.parallelize(data)

dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()

dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()

dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
dfFromRDD2.printSchema()

dfFromData2 = spark.createDataFrame(data).toDF(*columns)
dfFromData2.printSchema()     

rowData = map(lambda x: Row(*x), data) 
dfFromData3 = spark.createDataFrame(rowData,columns)
dfFromData3.printSchema()