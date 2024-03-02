# -*- coding: utf-8 -*-
"""
@author: naga
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("Examples.com") \
    .getOrCreate()

data = [
    ("James",None,"M"),
    ("Anna","NY","F"),
    ("Julia",None,None)
  ]

columns = ["name","state","gender"]
df = spark.createDataFrame(data,columns)
df.show()

df.filter("state is NULL").show()
df.filter(df.state.isNull()).show()
df.filter(col("state").isNull()).show() 

df.na.drop("state").show()