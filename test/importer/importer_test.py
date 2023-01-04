from profiling.importer.importer import Importer
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('TestSparkSession').getOrCreate()

def test_importer():
    importer = Importer(SparkSession=spark)
    assert importer != None

def test_import_hive_table_right_path():
    pass

def test_import_hive_table_wrong_path():
    pass

def test_import_hive_partitioned_table_right_path():
    pass

def test_import_hive_partitioned_table_wrong_path():
    pass

def test_import_delta_table_right_path():
    pass

def test_import_delta_table_wrong_path():
    pass

# def test_import_csv_table_with_right_path():
#     importer = Importer(SparkSession=spark)
#     file_path = './data/data_paper_sample_10k.csv'
#     df = importer.import_csv_table(file_path=file_path, header='True', inferschema='True')
#     assert df != None

def test_import_csv_table_with_wrong_path():
    importer = Importer(SparkSession=spark)
    file_path = 'wrong_path.csv'
    df = importer.import_csv_table(file_path=file_path, header='True', inferschema='True')
    assert df == None
