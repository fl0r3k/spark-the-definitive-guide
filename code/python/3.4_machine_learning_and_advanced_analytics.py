from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("pyspark.sql.shuffle.partitions","5")

# Load data
staticDataFrame = spark.read\
  .format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .load("data/retail-data/by-day/*.csv")

staticDataFrame.printSchema()

# Data preprocessing
from pyspark.sql.functions import date_format, col
preppedDataFrame = staticDataFrame\
  .na.fill(0)\
  .withColumn(
    "day_of_week",
    date_format(col("InvoiceDate"),"EEEE"))\
  .coalesce(5)

# Split to train and test
trainDataFrame = preppedDataFrame.where("InvoiceDate < '2011-07-01'")
testDataFrame  = preppedDataFrame.where("InvoiceDate >= '2011-07-01'")

# Display train and test data counts
trainDataFrame.count()
testDataFrame.count()

# Transform day of week to corresponding numerical values
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer()\
  .setInputCol("day_of_week")\
  .setOutputCol("day_of_week_index")

# Trasform day of week numerical values to boolean columns
from pyspark.ml.feature import OneHotEncoder
encoder = OneHotEncoder()\
  .setInputCol("day_of_week_index")\
  .setOutputCol("day_of_week_encoded")

# Preparing input vector of features for ML model
from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler()\
  .setInputCols([
    "UnitPrice",
    "Quantity",
    "day_of_week_encoded"
  ])\
  .setOutputCol("features")

# Pack above tree steps into a pipeline
from pyspark.ml import Pipeline
transformationPipeline = Pipeline()\
  .setStages([indexer,encoder,vectorAssembler])

# Fitting pipeline using train data
fittedPipeline = transformationPipeline.fit(trainDataFrame)
# Transforming train data using fitted pipeline
transformedTraining = fittedPipeline.transform(trainDataFrame)

# Caching transformed training data
transformedTraining.cache()

# Defining KMeans model
from pyspark.ml.clustering import KMeans
kmeans = KMeans()\
  .setK(20)\
  .setSeed(1)

# Fitting KMeans model
kmModel = kmeans.fit(transformedTraining)


# Using fitted pipeline on test data
transformedTest = fittedPipeline.transform(testDataFrame)
# Using fitted KMeans model on test data
kmModel.computeCost(transformedTest)