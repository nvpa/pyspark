# -*- coding: utf-8 -*-
"""
@author: naga
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Examples.com').getOrCreate()

print(spark)
rdd=spark.sparkContext.parallelize([1,2,3,4,56])
print("RDD count :"+str(rdd.count()))

rdd = spark.sparkContext.emptyRDD
print(rdd)
rdd2 = spark.sparkContext.parallelize([])
print(rdd2)
