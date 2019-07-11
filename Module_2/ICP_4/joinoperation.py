import sys
import os
from pyspark.sql import SQLContext
os.environ["SPARK_HOME"] = "D:\\spark-2.4.3-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"]="D:\\winutils"
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import pandas  as pd
# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
#stream1 = pd.read_csv("lorem.txt", sep=" ", header=None)
sqlContext = SQLContext(sc)
lines = ssc.socketTextStream("localhost", 23)
words = lines.flatMap(lambda line: line.split(" "))
s1=words.window(20)
s2=words.window(30)
joined=s1.join(s2)
joined.pprint()
ssc.start()  # Start the computation
ssc.awaitTermination()