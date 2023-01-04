from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

class BaseProfiler():
    def __init__(self, spark: SparkSession, dataframe: DataFrame):
        self.spark = spark
        self.df = dataframe
        self.profiling_result = None

    
    def process(self):
        return self.profiling_result