from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("pyspark.sql.shuffle.partitions","5")

# Static DataFrame version
staticDataFrame = spark.read\
  .format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .load("data/retail-data/by-day/*.csv")

staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema

from pyspark.sql.functions import window, col

staticDataFrame\
  .selectExpr(
    "CustomerId",
    "(UnitPrice*Quantity) as total_cost",
    "InvoiceDate")\
  .groupBy(
    col("CustomerId"),window(col("InvoiceDate"),"1 day"))\
  .sum("total_cost")\
  .show(5)

# Streaming DataFrame version
streamingDataFrame = spark.readStream\
  .schema(staticSchema)\
  .option("maxFilesPerTrigger",1)\
  .format("csv")\
  .option("header","true")\
  .load("data/retail-data/by-day/*.csv")

streamingDataFrame.isStreaming

from pyspark.sql.functions import window, col

purchaseyCustomerPerHour = streamingDataFrame\
  .selectExpr(
    "CustomerId",
    "(UnitPrice*Quantity) as total_cost",
    "InvoiceDate")\
  .groupBy(
    col("CustomerId"),window(col("InvoiceDate"),"1 day"))\
  .sum("total_cost")

# purchaseyCustomerPerHour.writeStream\
#   .format("memory")\
#   .queryName("customer_purchases")\
#   .outputMode("complete")\
#   .start()

# spark.sql("""
#   SELECT *
#   FROM customer_purchases
#   ORDER BY `sum(total_cost)` DESC
# """)\
# .show(5)

purchaseyCustomerPerHour.writeStream\
  .format("console")\
  .queryName("customer_purchases_2")\
  .outputMode("complete")\
  .start()
