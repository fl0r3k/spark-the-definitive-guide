from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("pyspark.sql.shuffle.partitions","5")

# Automatic schema on read
df = spark.read\
  .format("json")\
  .load("data/flight-data/json/2015-summary.json")

df.printSchema()

df.schema

# Defining schema manually with metadata
from pyspark.sql.types import StructField, StructType, StringType, LongType
myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME",StringType(),True),
  StructField("ORIGIN_COUNTRY_NAME",StringType(),True),
  StructField("count",LongType(),False,metadata={"hello":"world"})
])
df = spark.read\
  .format("json")\
  .schema(myManualSchema)\
  .load("data/flight-data/json/2015-summary.json")

