# -*- coding: utf-8 -*-
"""
@author: naga
"""
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

spark = SparkSession.builder.appName('Examples.com').getOrCreate()

print(spark)

