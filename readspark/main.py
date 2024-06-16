import findspark
findspark.init()
from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

df = spark.read.option("header","true") \
    .option("delimiter",",") \
    .option("inferSchema","true") \
    .csv("C:\\Users\\INFX014597\\Documents\\testme\\Old.csv")


print(df.printSchema())