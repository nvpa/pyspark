"""
@author: naga
"""


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
simpleData = [("James", "Sales", "NY", 90000, 34, 10000),
              ("Michael", "Sales", "NY", 86000, 56, 20000),
              ("Robert", "Sales", "CA", 81000, 30, 23000),
              ("Maria", "Finance", "CA", 90000, 24, 23000),
              ("Raman", "Finance", "CA", 99000, 40, 24000),
              ("Scott", "Finance", "NY", 83000, 36, 19000),
              ("Jen", "Finance", "NY", 79000, 53, 15000),
              ("Jeff", "Marketing", "CA", 80000, 25, 18000),
              ("Kumar", "Marketing", "NY", 91000, 50, 21000)
              ]
columns = ["employee_name", "department", "state", "salary", "age", "bonus"]

df = spark.createDataFrame(data=simpleData, schema=columns)
df = df.groupBy('state').agg(avg('salary').alias('avg_salary')).filter(col('avg_salary') >=87500)

# df.printSchema()
df.show(truncate=False)






df.createOrReplaceTempView("EMP")
df = spark.sql("select employee_name,department,state,salary,age,bonus from EMP ORDER BY department asc").show(
    truncate=False)

data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) ,\
("Saif", "Sales", 4200) \
  ]

# Create DataFrame
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
df = df.drop_duplicates(['employee_name'])
df.show(truncate=False)

data = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('languages', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)
df = df.withColumn("gender", when(col("gender") == 'M', 'Male').otherwise('Female'))
# df.printSchema()
df.show(truncate=False)
schema=StructType([
    StructField('firstname',StringType(), True),
StructField('lastname',StringType(), True),
StructField('middlename',StringType(), True),
])

data= [('nama','ummadi','sole'),('kasim','shaik','chalo')]

df = spark.createDataFrame(data, schema =schema)
# df= df.sort(df.firstname.asc())
df= df.filter(df.firstname.contains('ka'))
print(df.show())

data = [('James', 'hero', "FA"), ('cameron', 'list', 'ZA'), ('hello', 'sam', 'SA')]
states = {'FA': 'failure', 'ZA': 'zerocross', 'SA': 'southafrica'}
broadcastVariables = spark.sparkContext.broadcast(states)
schema = ["name", "caption", "state"]
df = spark.createDataFrame(data, schema=schema)

print(df.show())


def is_convert(value):
    return broadcastVariables.value[value]


df = df.rdd.map(lambda x: (x[0], x[1], is_convert(x[2]))).toDF(schema)
filteredDf = df.where((df['state'].isin(broadcastVariables.value)))
print()

data = [1, 2, 3, 4, 4, 5, 4, 48]
df = spark.sparkContext.parallelize(data)
accum = spark.sparkContext.accumulator(0)
# def run(x):
#     global accum
#     accum += x
#     return accum
# df.foreach(run)
# df = df.filter(lambda x: x)
# print(df.collect())
df.foreach(lambda x: accum.count(x))

print(accum.value)

print(str(df.getNumPartitions()))
df = df.repartition(4)
print(str(df.getNumPartitions()))
print(df.collect())
df1 = spark.sparkContext.textFile("./test.txt")
print(str(df1.getNumPartitions()))
# print(df1.saveAsTextFile("./partition.txt"))

df2 = df1.flatMap(lambda x: x.split(' '))
print(df2.collect())
df3 = df2.map(lambda x: (x, 1))
df4 = df1.map(lambda x: (x[0], x[1])).sortByKey()
df5 = df3.reduceByKey(lambda x, b: x + b)
df6 = df5.filter(lambda x: 'o' in x[0])
print(df6.collect())
