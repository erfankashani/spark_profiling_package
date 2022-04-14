import pytest
# pytestmark = pytest.mark.usefixtures("spark_context")
from profiling.context_manager.spark_context_manager import SparkContextManager


# define spark session for the test 
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('TestSparkSession').getOrCreate()


def test_spark_context_manager():
    spark_context = SparkContextManager(spark=None)
    assert spark_context != None


def test_spark_context_with_no_args():
    spark_manager = SparkContextManager(spark=None)
    spark_context = spark_manager.get_spark_context()
    assert spark_context.getConf().get('spark.app.name') == "DefaultSparkSession"


def test_spark_context_with_args():
    spark_manager = SparkContextManager(spark=spark)
    spark_context = spark_manager.get_spark_context()
    assert spark_context.getConf().get('spark.app.name') == "TestSparkSession"