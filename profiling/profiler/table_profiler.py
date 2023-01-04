from profiling.profiler.base_profiler import BaseProfiler
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, DateType

class TableProfiler(BaseProfiler):
    def __init__(self, spark: SparkSession, dataframe: DataFrame):
        super().__init__(spark, dataframe)
        # self.profiling_result = None
    
    def get_column_names(self):
        return self.df.columns
    
    def get_column_types(self):
        return self.df.dtypes

    def get_column_numeric_count(self):
        return len(self.get_column_numeric_names())
    
    def get_column_numeric_names(self):
        return [col[0] for col in self.get_column_types() if col[1] in ['int', 'float', 'long', 'double', 'decimal', 'bigint']]

    def get_column_categorical_count(self):
        return len(self.get_column_categorical_names())
    
    def get_column_categorical_names(self):
        return [col[0] for col in self.get_column_types() if col[1] in ['string', 'boolean']]
    
    def get_column_date_count(self):
        return len(self.get_column_date_names())
    
    def get_column_date_names(self):
        return [col[0] for col in self.get_column_types() if col[1].startswith('date')]

    def get_record_count(self):
        return self.df.count()

    def get_column_count(self):
        return len(self.get_column_names())

    def process(self):
        table_profiler_schema = StructType([
            StructField('record_count', IntegerType(), True),
            StructField('column_count', IntegerType(), True),
            StructField('numeric_column_count', IntegerType(), True),
            StructField('categorical_column_count', IntegerType(), True),
            StructField('date_column_count', IntegerType(), True),
        ])

        table_profiled_df = [
            (
                self.get_record_count(),
                self.get_column_count(),
                self.get_column_numeric_count(),
                self.get_column_categorical_count(),
                self.get_column_date_count()
            )
        ]
        self.profiling_result = self.spark.createDataFrame(data=table_profiled_df, schema=table_profiler_schema)
        return self.profiling_result