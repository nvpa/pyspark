"""
@author: naga
"""


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("James ", "", "Smith", "36636", "M", 3000),
        ("Michael ", "Rose", "", "40288", "M", 4000),
        ("Robert ", "", "Williams", "42114", "M", 4000),
        ("Maria ", "Anne", "Jones", "39192", "F", 4000),
        ("Jen", "Mary", "Brown", "", "F", -1)]
columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data, columns)
df.write.mode("overwrite").parquet("./people.parquet")
parDF1 = spark.read.parquet("./people.parquet")
parDF1.createOrReplaceTempView("parquetTable")
parDF1.printSchema()
parDF1.show(truncate=False)

parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")
parkSQL.show(truncate=False)

spark.sql("CREATE TEMPORARY VIEW PERSON USING parquet OPTIONS (path \"./people.parquet\")")
spark.sql("SELECT * FROM PERSON").show()

df.write.partitionBy("gender", "salary").mode("overwrite").parquet("./people2.parquet")

parDF2 = spark.read.parquet("./people2.parquet/gender=M")
parDF2.show(truncate=False)

spark.sql("CREATE TEMPORARY VIEW PERSON2 USING parquet OPTIONS (path \"./people2.parquet/gender=F\")")
spark.sql("SELECT * FROM PERSON2").show()
