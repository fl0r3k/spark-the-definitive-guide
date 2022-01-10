from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("pyspark.sql.shuffle.partitions","5")

from pyspark.sql import Row
spark.sparkContext.parallelize([Row(1),Row(2),Row(3)]).toDF()