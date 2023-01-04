from profiling.profiler.base_profiler import BaseProfiler
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, DateType
from pyspark.sql.functions import col,countDistinct

class ColumnProfiler(BaseProfiler):
    def __init__(self, spark: SparkSession, dataframe: DataFrame, column_name: str):
        self.column_name = column_name
        super().__init__(spark, dataframe)

    def get_column_name(self):
        return self.column_name

    def get_column_type(self):
        return self.df.dtypes[self.column_name]

    def get_total_count(self):
        return self.df.count()

    def get_unique_count(self):
        return self.df.select(self.column_name).distinct() \
                      .count()
    
    def get_null_count(self):
        return self.df.where(col(self.column_name).isNull()).count()

    def get_null_proportion(self):
        return self.get_null_count() / self.get_total_count()
    
    def get_top_five_values(self):
        return [self.df.select(self.column_name) \
                       .distinct() \
                       .orderBy(self.column_name, ascending=False) \
                       .limit(5) \
                       .collect()
                ]
    
    

    
