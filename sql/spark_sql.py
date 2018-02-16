from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as func

spark=SparkSession.builder.appName('SparkSQL').getOrCreate()

def initmap(line):
    fields=line.split(',')
    return Row (ID=int(fields[0]), Name=str(fields[1]), Age=int(fields[2]), NumFriends=int(fields[3]) )


infile=spark.sparkContext.textFile("../fakefriends.csv")
inrdd=infile.map(initmap)

"""Create and register schemaRDD"""
schemaRDD=spark.createDataFrame(inrdd).cache()
schemaRDD.createOrReplaceTempView("Friends")


query1=spark.sql("SELECT Age, SUM(NumFriends) as total from Friends WHERE GROUP BY Age ORDER BY total DESC")
for rw in query1.take(10):
  print(rw)

schemaRDD.groupBy("Age").agg(func.sum("NumFriends").alias("Total")).orderBy("Total",ascending=0).show(10)
"""" Using Sql functions"""


spark.stop()

"""
Query output

Row(Age=40, total=4264)                                                         
Row(Age=26, total=4115)
Row(Age=45, total=4024)
Row(Age=33, total=3904)
Row(Age=55, total=3842)
Row(Age=52, total=3747)
Row(Age=54, total=3615)
Row(Age=67, total=3434)
Row(Age=44, total=3386)
Row(Age=64, total=3376)

Dataframe Function output

+---+-----+                                                                     
|Age|Total|
+---+-----+
| 40| 4264|
| 26| 4115|
| 45| 4024|
| 33| 3904|
| 55| 3842|
| 52| 3747|
| 54| 3615|
| 67| 3434|
| 44| 3386|
| 64| 3376|
+---+-----+


"""