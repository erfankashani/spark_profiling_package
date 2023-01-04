from profiling.profiler.column_profiler import ColumnProfiler
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType, DateType, LongType



def test_column_profiler():
    spark = SparkSession.builder.appName('TestSparkSession').getOrCreate()
    sc = spark.sparkContext
    
    input_data = [(1,500,200.0, 'Tesla',True,datetime.strptime('2019-12-01','%Y-%m-%d')),
                  (2,0,200.0, 'Microsoft',True,datetime.strptime('2019-12-02','%Y-%m-%d')),
                  (3,0,0.0, 'Google',False,datetime.strptime('2019-12-03','%Y-%m-%d')),
                  (4,1000,0.0, 'Apple',True,datetime.strptime('2019-12-04','%Y-%m-%d')),
                  (5,None,34.0, 'Costco',True,datetime.strptime('2019-12-05','%Y-%m-%d')),
                  (6,None,None, 'Walmart',False,datetime.strptime('2019-12-06','%Y-%m-%d')),
                  (7,34,None, 'Target',True,datetime.strptime('2019-12-07','%Y-%m-%d')),
                  (8,10,20.0, 'Home Depot',True,datetime.strptime('2019-12-08','%Y-%m-%d'))
                  ]

    expected_data = [(8, 6, 3, 2, 1)]

    input_schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('revenue', LongType(), True),
        StructField('budget', FloatType(), True),
        StructField('company_name', StringType(), True),
        StructField('is_active', BooleanType(), True),
        StructField('report_date', DateType(), True),
    ])
    expected_schema = StructType([
            StructField('record_count', IntegerType(), True),
            StructField('column_count', IntegerType(), True),
            StructField('numeric_column_count', IntegerType(), True),
            StructField('categorical_column_count', IntegerType(), True),
            StructField('date_column_count', IntegerType(), True),
        ])

    input_df = spark.createDataFrame(data=spark.sparkContext.parallelize(input_data), schema=input_schema)
    
    expected_df = spark.createDataFrame(data=spark.sparkContext.parallelize(expected_data), schema=expected_schema)
    
    column_profiling_input = input_df.select('revenue')
    column_profiler = ColumnProfiler(spark, column_profiling_input, 'revenue')
    
    assert column_profiler.get_null_count() == 2
    assert column_profiler.get_unique_count() == 6
    assert column_profiler.get_total_count() == 8
    # pytest.assrt(output_df.collect() == expected_df.collect())
    # assert output_df.schema == expected_df.schema
    # assert output_df.collect() == expected_df.collect()