"""
@author: naga
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col
from pyspark.sql.types import IntegerType, StructField, StringType, DoubleType, BooleanType, StructType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

df = spark.read.json("./zipcodes.json")
df.show()
# if we use custom schema
schema = StructType([
    StructField("RecordNumber", IntegerType(), True),
    StructField("Zipcode", IntegerType(), True),
    StructField("ZipCodeType", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("LocationType", StringType(), True),
    StructField("Lat", DoubleType(), True),
    StructField("Long", DoubleType(), True),
    StructField("Xaxis", IntegerType(), True),
    StructField("Yaxis", DoubleType(), True),
    StructField("Zaxis", DoubleType(), True),
    StructField("WorldRegion", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("LocationText", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Decommisioned", BooleanType(), True),
    StructField("TaxReturnsFiled", StringType(), True),
    StructField("EstimatedPopulation", IntegerType(), True),
    StructField("TotalWages", IntegerType(), True),
    StructField("Notes", StringType(), True)
])

df = spark.read.format("json").schema(schema).load('./zipcodes.json')
# df.show()

df.write.mode('overwrite').json('zipcode_json')

# we can use multiline
df = spark.read.option('multiline', True).json('./multiline_zipcodes.json')
df.printSchema()
df.show()
