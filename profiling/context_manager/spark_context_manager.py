from pyspark.sql import SparkSession

class SparkContextManager:
    "deals with spark abstraction and passes the spark info into other packages"
    
    _spark_queue = []

    def __init__(self, spark: SparkSession=None):
        if len(self._spark_queue) > 0:
            self.get_spark_session().stop()
            self._spark_queue.pop()
        self._spark_queue.append(self._assign_spark_session(spark))
    
    def _assign_spark_session(self, spark: SparkSession=None):
        """returns either default or existing spark session"""
        if spark == None:
            spark = SparkSession.builder.appName('DefaultSparkSession').getOrCreate()
        return spark
    
    def get_spark_session(self):
        """returns the last spark session in the queue"""
        return self._spark_queue[-1]
    
    def get_spark_context(self):
        spark = self.get_spark_session()
        return spark.sparkContext

    def get_spark_queue(self):
        return self._spark_queue