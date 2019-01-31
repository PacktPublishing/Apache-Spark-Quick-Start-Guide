from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.Builder().appName('Example').getOrCreate()

sales_df = spark.read \
     .option("inferSchema", "true") \
     .option("header", "true") \
     .csv("sales.csv")

result = sales_df.groupBy("COUNTRY_CODE")\
                 .sum("AMOUNT")\
                 .orderBy(desc("sum(AMOUNT)"))

result.show()