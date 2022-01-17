from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("pyspark.sql.shuffle.partitions","5")

# Column object using col and column
from pyspark.sql.functions import col, column
col("someColumnName")
column("someColumnName")

# Column object using expr and col
from pyspark.sql.functions import expr
expr("someCol")
col("someCol")

# Examples of expressions
expr("someCol - 5")
col("someCol") - 5
expr("someCol") - 5

expr("(((someCol + 5)*200) - 6) < otherCol")

# Getting all columns of dataframe
spark.read\
  .format("json")\
  .load("data/flight-data/json/2015-summary.json")\
  .columns