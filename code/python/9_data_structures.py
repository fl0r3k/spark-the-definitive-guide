from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("pyspark.sql.shuffle.partitions","5")

# The Structure of the Data Source API
## Read API Structure

# DataFrameReader.format(...).option(...).schema(...).load()

# spark.read.format("csv")\
#   .option("mode","FAILFAST")\
#   .option("inferSchema","true")\
#   .option("path","path/to/file(s)")\
#   .schema(someSchema)\
#   .load()

## Write API Structure

# DataFrameWriter.format(...).option(...).partitionBy(...).bucketBy(...).sortBy(...).save()

# dataframe.write.format("csv")\
#   .option("mode","OVERWRITE")\
#   .option("dateFormat","yyyy-MM-dd")\
#   .option("path","path/to/file(s)")\
#   .save()

# CSV Files
## Reading CSV Files
spark.read.format("csv")\
  .option("header","true")\
  .option("mode","FAILFAST")\
  .option("inferSchema","true")\
  .load("some/path/to/file.csv")


from pyspark.sql.types import StructType, StructField, StringType, LongType

### valid schema
myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False)
])

spark.read.format("csv")\
  .option("header","true")\
  .option("mode","FAILFAST")\
  .schema(myManualSchema)\
  .load("data/flight-data/csv/2010-summary.csv")\
  .show(5)

### fail due to invalid schema
myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", LongType(), True),
  StructField("ORIGIN_COUNTRY_NAME", LongType(), True),
  StructField("count", LongType(), False)
])

spark.read.format("csv")\
  .option("header","true")\
  .option("mode","FAILFAST")\
  .schema(myManualSchema)\
  .load("data/flight-data/csv/2010-summary.csv")\
  .show(5)


## Writing CSV Files
csvFile = spark.read.format("csv")\
  .option("header","true")\
  .option("mode","FAILFAST")\
  .option("inferSchema","true")\
  .load("data/flight-data/csv/2010-summary.csv")

csvFile.write.format("csv")\
  .mode("overwrite")\
  .option("sep","\t")\
  .save("../../tmp/my-tsv-file.tsv")



# JSON Files
## Reading JSON Files
spark.read.format("json")\
  .option("mode","PERMISSIVE")\
  .option("inferSchema","true")\
  .load("data/flight-data/json/2010-summary.json")\
  .show(5)

## Writting JSON Files
csvFile.write.format("json")\
  .mode("overwrite")\
  .save("../../tmp/my-json-file.json")

# Parquet Files

## Reading Parquet Files
spark.read.format("parquet")\
  .load("data/flight-data/parquet/2010-summary.parquet")\
  .show(5)

## Writing Parquet Files
csvFile.write.format("parquet")\
  .mode("overwrite")\
  .save("../../tmp/my-parquet-file.parquet")


# ORC Files

## Reading ORC Files
spark.read.format("orc")\
  .load("data/flight-data/orc/2010-summary.orc")\
  .show(5)

## Writing ORC Files
csvFile.write.format("orc")\
  .mode("overwrite")\
  .save("../../tmp/my-orc-file.orc")



# SQL Databases
driver = "org.sqlite.JDBC"
path = "data/flight-data/jdbc/my-sqlite.db"
url = "jdbc:sqlite:" + path
tablename = "flight_info"

dbDataFrame = spark.read.format("jdbc")\
  .option("url",url)\
  .option("dbtable",tablename)\
  .option("driver",driver)\
  .load()

# TO BE CONTINUED ...


# Text Files

## Reading ORC Files
spark.read.text("data/flight-data/csv/2010-summary.csv")\
  .selectExpr("split(value,',') as rows")\
  .show()

## Writing ORC Files
csvFile.select("DEST_COUNTRY_NAME")\
  .write.text("../../tmp/simple-text-file.txt")

csvFile.limit(10).select("DEST_COUNTRY_NAME","count")\
  .write.partitionBy("count")\
  .text("../../tmp/five-csv-files2py.txt")



# Advanced I/O Concepts

## Writing Data in Parallel
csvFile.repartition(5)\
  .write.format("csv")\
  .save("../../tmp/multiple.csv")


## Partitioning
csvFile.limit(10)\
  .write.format("parquet").mode("overwrite")\
  .partitionBy("DEST_COUNTRY_NAME")\
  .save("../../tmp/partitioned-files.parquet")


## Bucketing
numberBuckets = 10
columnToBucketBy = "count"
csvFile.limit(10)\
  .write.format("parquet").mode("overwrite")\
  .bucketBy(numberBuckets,columnToBucketBy)\
  .saveAsTable("bucketedFiles")