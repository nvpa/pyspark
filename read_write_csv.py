import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col

# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# df = spark.read.options(header='true', inferSchema='true', delimiter=',').csv('./zipcodes.csv')
# # df = df.sort(df.State)
# # df = df.groupBy('RecordNumber').count()
# df.write.mode('overwrite').options(header='true').csv('./csv_write.csv')
# df.show(truncate=False)

#
# data = [("James ", "", "Smith", "36636", "M", 3000),
#         ("Michael ", "Rose", "", "40288", "M", 4000),
#         ("Robert ", "", "Williams", "42114", "M", 4000),
#         ("Maria ", "Anne", "Jones", "39192", "F", 4000),
#         ("Jen", "Mary", "Brown", "", "F", -1)]
# columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
# df = spark.createDataFrame(data, columns)
# df.write.mode("overwrite").parquet("./people.parquet")
# parDF1 = spark.read.parquet("./people.parquet")
# parDF1.createOrReplaceTempView("parquetTable")
# parDF1.printSchema()
# parDF1.show(truncate=False)
#
# parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")
# parkSQL.show(truncate=False)
#
# spark.sql("CREATE TEMPORARY VIEW PERSON USING parquet OPTIONS (path \"./people.parquet\")")
# spark.sql("SELECT * FROM PERSON").show()
#
# df.write.partitionBy("gender", "salary").mode("overwrite").parquet("./people2.parquet")
#
# parDF2 = spark.read.parquet("./people2.parquet/gender=M")
# parDF2.show(truncate=False)
#
# spark.sql("CREATE TEMPORARY VIEW PERSON2 USING parquet OPTIONS (path \"./people2.parquet/gender=F\")")
# spark.sql("SELECT * FROM PERSON2").show()

from os.path import abspath
# from pyspark.sql import SparkSession

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

# Create spark session with hive enabled
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

columns = ["id", "name", "age", "gender"]

# Create DataFrame
data = [(1, "James", 30, "M"), (2, "Ann", 40, "F"),
        (3, "Jeff", 41, "M"), (4, "Jennifer", 20, "F")]
sampleDF = spark.sparkContext.parallelize(data).toDF(columns)

# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS emp")

# Create Hive Internal table
sampleDF.write.mode('overwrite') \
    .saveAsTable("emp.employee")

# Spark read Hive table
df = spark.read.table("emp.employee")
df.show()
