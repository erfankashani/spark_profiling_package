from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession

class Importer():
    def __init__(self, SparkSession: SparkSession):
        self.spark_session = SparkSession
    
    def import_hive_table(self, table_path: str) -> DataFrame:
        pass
    
    def import_hive_partitioned_table(self, table_path: str) -> DataFrame:
        pass

    def import_delta_table(self, table_path) -> DataFrame:
        pass

    def import_csv_table(self, file_path: str, header: str='True', inferschema: str='True') -> DataFrame:
        
        try:
            df = self.spark_session.read \
                                   .format('csv') \
                                   .options(header=header,inferschema=inferschema) \
                                   .load(file_path)
        except AnalysisException:
            df = None
            print(f"the path does not exist: {file_path}")
        
        return df