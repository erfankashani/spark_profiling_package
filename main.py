from profiling.context_manager.spark_context_manager import SparkContextManager

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('TestSparkSession').getOrCreate()
spark_manager = SparkContextManager(spark=spark)
spark_context = spark_manager.get_spark_context()
print(spark_context.getConf().get('spark.app.name'))

# this is tetsjj