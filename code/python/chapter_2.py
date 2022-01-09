# The Spark Session
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print(spark)

# DataFrame
myRange = spark.range(1000).toDF("number")

# Transformation - example of narrow transformation
divisBy2 = myRange.where("number % 2 = 0")

# Action
divisBy2.count()

# An End-to-End Example
# Load data from CSV file
flightData2015 = spark \
  .read \
  .option("inferSchema","true") \
  .option("header","true") \
  .csv("../../data/flight-data/csv/2015-summary.csv")

# Display 3 records
flightData2015.take(3)

# Explain plan of sort operation
flightData2015.sort("count").explain()

# Change number or partitins from 200 to 5
spark.conf.set("spark.sql.shuffle.partitions","5")
flightData2015.sort("count").take(2)

flightData2015.sort("count").explain()

# Data Frames and SQL
# Craate temp view on DataFrame for SQL queries
flightData2015.createOrReplaceTempView("flight_data_2015")

# SQL query on DataFrame
sqlWay = spark.sql("""
  select dest_country_name, count(1)
  from flight_data_2015
  group by dest_country_name
""")

# The same operation as above SQL but with DataFrame API
dataFrameWay = flightData2015 \
  .groupBy("dest_country_name") \
  .count()

# Explain both plans to see they are identical
sqlWay.explain()
dataFrameWay.explain()

# Example of another function
spark.sql("select max(count) from flight_data_2015").take(1)

from pyspark.sql.functions import max
flightData2015.select(max("count")).take(1)

maxSql = spark.sql("""
  select dest_country_name, sum(count) as destination_total
  from flight_data_2015
  group by dest_country_name
  order by sum(count) desc
  limit 5
""")
maxSql.show()

from pyspark.sql.functions import desc
maxDf = flightData2015 \
  .groupBy("dest_country_name") \
  .sum("count") \
  .withColumnRenamed("sum(count)","destination_total") \
  .sort(desc("destination_total")) \
  .limit(5)
maxDf.show()

# Explain execution plan
maxSql.explain()
maxDf.explain()