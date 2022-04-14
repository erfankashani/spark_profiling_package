from profiling.context_manager.spark_context_manager import SparkContextManager
from profiling.importer.importer import Importer

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ProfilingSparkSession').getOrCreate()
spark_manager = SparkContextManager(spark=spark)
spark_context = spark_manager.get_spark_context()
print(spark_context.getConf().get('spark.app.name'))


profiling_input="data_paper_sample_10k.csv"
filepath='./data/'+profiling_input

importer = Importer(SparkSession=spark)
profiling_df = importer.import_csv_table(file_path=filepath, header='True', inferschema='True')
profiling_df.show()
# profiling_result = profile_table(profiling_df)