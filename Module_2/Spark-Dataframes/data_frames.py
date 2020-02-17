import Arrays as Arrays
from pyspark.sql import SparkSession
from operator import add
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql import HiveContext
import numpy as np
import pandas as pd
import os
os.environ["SPARK_HOME"] = "D:\\spark-2.4.3-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"]="D:\\winutils"


## 1. Import the dataset and create data framesdirectly on import.
spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

df = spark.read.csv("C:\\Users\\Kruthika\\PycharmProjects\\M2_ICP_3\\survey.csv",header=True);
df.show()
sc = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .master("local[*]")\
    .config("spark.sql.warehouse.dir", "something") \
    .enableHiveSupport() \
    .getOrCreate()

#2.Remove duplicates
df.dropDuplicates()
print(df.count())

#3.UnionAll
df1 = df.limit(10)
df2 = df.limit(10)
unionDf = df1.unionAll(df2)
unionDf.orderBy('Country').coalesce(1).write.format('csv').save("output", header='true')

#4GroupBy
print(df.groupBy("treatment"))

## part-2 1. Join operation
joined_df = df1.join(df2, df1.Country == df2.Country)

# Aggregate functions
df.groupby('treatment').agg({'Age': 'mean'}).show()

## part-2 2. 13th row
df13=df.take(13)
print(df13[-1])
def getdata():
    return pd.read_csv("C:\\Users\\Kruthika\\PycharmProjects\\M2_ICP_3\\survey.csv",sep=',')
dfd=spark.udf.register('GATE_TIME',getdata())
dfd.registerTempTable("survey")
df.createOrReplaceTempView("survey")
#df.registerTempTable("survey")


#bonus 2.
sqlDF = spark.sql("SELECT max('Age') FROM survey")
sqlDF.show()

sqlDF = spark.sql("SELECT avg('Age') FROM survey")
sqlDF.show()



