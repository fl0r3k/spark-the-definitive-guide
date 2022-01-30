from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("pyspark.sql.shuffle.partitions","5")


df = spark.read\
  .option("header","true")\
  .option("inferSchema","true")\
  .csv("data/retail-data/all/*.csv")\
  .coalesce(5)

df.cache()
df.createOrReplaceTempView("dfTable")

df.count() == 541909 # Caching trick


# Aggregation Funcitons
## count
from pyspark.sql.functions import count
df.select(count("StockCode")).show()

## countDistinct
from pyspark.sql.functions import countDistinct
df.select(countDistinct("StockCode")).show()

## approx_count_distinct
from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("StockCode",0.1)).show()

## first and last
from pyspark.sql.functions import first, last
df.select(first("StockCode"),last("StockCode")).show()

## min and max
from pyspark.sql.functions import min, max
df.select(min("Quantity"),max("Quantity")).show()

## sum
from pyspark.sql.functions import sum
df.select(sum("Quantity")).show()

## sumDistinct
from pyspark.sql.functions import sumDistinct
df.select(sumDistinct("Quantity")).show()

## avg
from pyspark.sql.functions import sum, count, avg, expr
df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))\
  .selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases")\
  .show()

## Variance and Standard Deviation
from pyspark.sql.functions import var_pop, stddev_pop
from pyspark.sql.functions import var_samp, stddev_samp
df.select(
    var_pop("Quantity"),
    stddev_pop("Quantity"),
    var_samp("Quantity"),
    stddev_samp("Quantity"))\
  .show()

## skewness and kurtosis
from pyspark.sql.functions import skewness, kurtosis
df.select(
    skewness("Quantity"),
    kurtosis("Quantity"))\
  .show()

## Covariance adn Correlatoin
from pyspark.sql.functions import corr, covar_pop, covar_samp
df.select(
    corr("InvoiceNo","Quantity"),
    covar_samp("InvoiceNo","Quantity"),
    covar_pop("InvoiceNo","Quantity"))\
  .show()

## Aggregating to Complex Types
from pyspark.sql.functions import collect_set, collect_list
df.agg(
    collect_set("Country"),
    collect_list("Country"))\
  .show()


# Grouping
df.groupBy("InvoiceNo","CustomerId").count().show()

## Grouping with Expressions
from pyspark.sql.functions import count
df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)"))\
  .show()

## Grouping with Maps
df.groupBy("InvoiceNo").agg(
    expr("avg(Quantity)"),
    expr("stddev_pop(Quantity)"))\
  .show()

# Window Functions
from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date",to_date(col("InvoiceDate"),"MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")

from pyspark.sql.window import Window
from pyspark.sql.functions import desc
windowSpec = Window\
  .partitionBy("CustomerId","date")\
  .orderBy(desc("Quantity"))\
  .rowsBetween(Window.unboundedPreceding,Window.currentRow)

from pyspark.sql.functions import max
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

from pyspark.sql.functions import dense_rank, rank
purchaseDensRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)

from pyspark.sql.functions import col
dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
  .select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    purchaseRank.alias("quantityRank"),
    purchaseDensRank.alias("quantityDenseRank"),
    maxPurchaseQuantity.alias("maxPurchaseQuantity"))\
  .show()

# Grouping Sets
dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNotNull")

## Rollups
rolledUpDF = dfNoNull.rollup("Date","Country")\
  .agg(sum("Quantity"))\
  .selectExpr("Date","Country","`sum(Quantity)` as total_quantity")\
  .orderBy("Date")
rolledUpDF.show()

rolledUpDF.where("Country IS NULL").show()
rolledUpDF.where("Date IS NULL").show()

## Cube
from pyspark.sql.functions import sum
dfNoNull.cube("Date","Country")\
  .agg(sum(col("Quantity")))\
  .select("Date","Country","sum(Quantity)")\
  .orderBy("Date")\
  .show()

## Grouping Metadata
from pyspark.sql.functions import grouping_id, sum, expr
dfNoNull.cube("CustomerId","StockCode")\
  .agg(
    grouping_id(),
    sum("Quantity"))\
  .orderBy(expr("grouping_id()").desc())\
  .show()

## Pivot
pivoted = dfWithDate.groupBy("Date").pivot("Country").sum()
pivoted.printSchema()
pivoted.where("Date > '2011-12-05'")\
  .select("Date","`USA_sum(CAST(Quantity AS BIGINT))`")\
  .show()

# User-Definde Aggregation Functions
# only Scala or Java