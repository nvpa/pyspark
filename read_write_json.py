from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

df =spark.read.json("./zipcodes.json")

df.show()