from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("pyspark.sql.shuffle.partitions","5")


# Creating DataFrame
## from raw source
df = spark.read\
  .format("json")\
  .load("data/flight-data/json/2015-summary.json")

df.createOrReplaceGlobalTempView("dfTable")

## on the fly
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
myManualSchema = StructType([
  StructField("some",StringType(),True),
  StructField("col",StringType(),True),
  StructField("names",LongType(),False)
])
myRow = Row("Hello",None,1)
myDf = spark.createDataFrame([myRow],myManualSchema)
myDf.show()


# select and selectExpr
df.select("DEST_COUNTRY_NAME").show(2)
df.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").show(2)

from pyspark.sql.functions import expr, col, column
df.select(
    expr("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"))\
  .show(2)

df.select(
    expr("DEST_COUNTRY_NAME destination"))\
  .show(2)

df.select(
    expr("DEST_COUNTRY_NAME destination").alias("DEST_COUNTRY_NAME"))\
  .show(2)

df.selectExpr(
    "DEST_COUNTRY_NAME as destination",
    "DEST_COUNTRY_NAME")\
  .show(2)

df.selectExpr(
    "*",
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
  .show(2)

df.selectExpr(
    "avg(count)",
    "count(distinct(DEST_COUNTRY_NAME))")\
  .show(2)


# Converting to Spark Types (Literals)
from pyspark.sql.functions import lit
df.select(expr("*"),lit(1).alias("One")).show(2)


# Adding Columns
df.withColumn("numberOne",lit(1)).show(2)

df.withColumn(
    "withinCountry",
    expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME"))\
  .show(2)

df.withColumn(
    "withinCountry",
    expr("DEST_COUNTRY_NAME"))\
  .columns


# Renaming Columns
df.withColumnRenamed("DEST_COUNTRY_NAME","dest")\
  .columns


# Reserved Characters and Keywords
dfWithLongColName = df.withColumn(
  "This Long Column Name",
  expr("ORIGIN_COUNTRY_NAME"))

dfWithLongColName.selectExpr(
    "`This Long Column Name`",
    "`This Long Column Name` as `new col`")\
  .show(2)

dfWithLongColName.createOrReplaceTempView("dfTableLong")

dfWithLongColName.select(expr("`This Long Column Name`")).columns


# Case Sensitivity
spark.conf.set("spark.sql.caseSensitivity",True)
spark.conf.set("spark.sql.caseSensitivity",False)


# Removing Columns
dfWithLongColName.drop("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME")


# Changing a Column's Type (cast)
df.withColumn("count2",col("count").cast("long"))


# Filtering Rows
df.filter(col("count") < 2).show(2)

df.where(col("count") < 2)\
  .where(col("ORIGIN_COUNTRY_NAME") != "Croatia")\
  .show(2)


# Getting Unique Row
df.select("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME")\
  .distinct()\
  .count()

df.select("ORIGIN_COUNTRY_NAME")\
  .distinct()\
  .count()


# Random Samples
seed = 5
withReplacement = False
fraction = 0.5
df.sample(withReplacement, fraction, seed).count()


# Random Split
dataFrames = df.randomSplit([0.25,0.75],seed)
dataFrames[0].count() > dataFrames[1].count()


# Concatenating and Appending Rows (Union)
from pyspark.sql import Row
schema = df.schema
newRows = [
  Row("New Country", "Other Country", 5),
  Row("New Country 2", "Other Country 3", 1)
]
parallelizeRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizeRows,schema)

df.union(newDF)\
  .where("count = 1")\
  .where(col("ORIGIN_COUNTRY_NAME") != "United States")\
  .show()


# Sorting Rows
df.sort("count").show(5)
df.orderBy("count","DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"),col("DEST_COUNTRY_NAME")).show(5)

df.orderBy(col("count").desc()).show(2)
df.orderBy(col("count").desc(),col("DEST_COUNTRY_NAME").asc()).show(2)

spark.read\
  .format("json")\
  .load("data/flight-data/json/2015-summary.json")\
  .sortWithinPartitions("count")


# Limit
df.limit(5).show()
df.orderBy(col("count").desc()).limit(6).show()


# Repartition and Coalesce
df.rdd.getNumPartitions()

## Repartition
df.repartition(5)

df.repartition(col("DEST_COUNTRY_NAME"))

df.repartition(5,col("DEST_COUNTRY_NAME"))

## Coalesce
df.repartition(5,col("DEST_COUNTRY_NAME")).coalesce(2)


# Collecting Rows to the Driver
collectDF = df.limit(10)
collectDF.take(5) # take top 5
collectDF.show() # print nicely
collectDF.show(5,False)
collectDF.collect() # take all
