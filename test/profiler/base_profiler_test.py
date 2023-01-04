from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('TestSparkSession').getOrCreate()

# sample spark dataframe
spark_df = spark.createDataFrame([('alice', 1), ('adam', 10)],['name','age'])
spark_df.show()

def test_base_profiler():
    from profiling.profiler.base_profiler import BaseProfiler
    base_profiler = BaseProfiler(spark=spark, dataframe=spark_df)
    profiling_df = base_profiler.df
    assert profiling_df != None