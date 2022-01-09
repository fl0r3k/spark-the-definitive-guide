# on Linux
cd $SPARK_HOME
./bin/spark-submit \
  --master local \
  ./examples/src/main/python/pi.py 10