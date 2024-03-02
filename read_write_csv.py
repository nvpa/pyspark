import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

df = spark.read.options(header='true', inferSchema='true', delimiter=',').csv('./zipcodes.csv')
# df = df.sort(df.State)
# df = df.groupBy('RecordNumber').count()
df.write.mode('overwrite').options(header='true').csv('./csv_write.csv')
df.show(truncate=False)



