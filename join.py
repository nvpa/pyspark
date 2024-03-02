
import pyspark
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
emp = [(1, "Smith", -1, "2018", "10", "M", 3000),
       (2, "Rose", 1, "2010", "20", "M", 4000),
       (3, "Williams", 1, "2010", "10", "M", 1000),
       (4, "Jones", 2, "2005", "10", "F", 2000),
       (5, "Brown", 2, "2010", "40", "", -1),
       (6, "Brown", 2, "2010", "50", "", -1)
       ]
empColumns = ["emp_id", "name", "superior_emp_id", "year_joined", \
              "emp_dept_id", "gender", "salary"]

empDF = spark.createDataFrame(data=emp, schema=empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance", 10),
        ("Marketing", 20),
        ("Sales", 30),
        ("IT", 40)
        ]
deptColumns = ["dept_name", "dept_id"]
deptDF = spark.createDataFrame(data=dept, schema=deptColumns)
# deptDF.printSchema()
# deptDF.show(truncate=False)
joinDf = empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, how='full')
# outerDf= empDF.join(deptDF,empDF.emp_dept_id == deptDF.dept_id, how= 'outer')
# outerDf = empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, how='rightouter')
# outerDf= empDF.join(deptDF,empDF.emp_dept_id == deptDF.dept_id, how= 'semi')
outerDf = empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, how='leftanti')
outerDf.show(truncate=False)
joinDf.show(truncate=False)



from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, avg, udf

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

df = spark.read.options(header='true', inferSchema='true').csv("./small_zipcode.csv")
columns = ["Seqno", "Name"]
data = [("1", "john jones"),
        ("2", "tracey smith"),
        ("3", "amy sanders"),
        (0, "jones"),
        ("5", " smith"),
        ("6", 0)
        ]

df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()




import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
# Create spark session
data = [("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"), ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
        ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"), ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico")]

columns = ["Product", "Amount", "Country"]
df = spark.createDataFrame(data=data, schema=columns)
df = df.groupBy('Product').pivot('Country').sum('amount')
# df.printSchema()
df.show(truncate=False)

df = df.na.fill('unknown', ['type'])
df.show(truncate=False)

def convertUdf(str):
    get = ''
    arr = str.split(' ')
    for i in arr:
        get = get + i[0:1].upper() + i[1:len(i)] + ' '
    return get


convert = udf(lambda z: convertUdf(z), StringType())

df = df.select(col('Seqno'), convert(col('name')).alias('name'))


df.show(truncate=False)
