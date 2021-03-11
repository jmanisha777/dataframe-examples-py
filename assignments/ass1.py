from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os.path
import yaml

if _name_ == '_main_':
    os.environ['PYSPARK_SUBMIT_ARGS'] = ('--packages "org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell')

    # Creating Spark Session
    spark = SparkSession \
                .builder \
                .appName('Exercise')\
                .master('local[*]')\
                .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(_file_))
    app_config_path = os.path.abspath(current_dir+'/../'+"application.yml")
    app_secret_path = os.path.abspath(current_dir+'/../'+".secrets")

    config = open(app_config_path)
    app_config = yaml.load(config, Loader=yaml.FullLoader)
    secrets = open(app_secret_path)
    app_secret = yaml.load(secrets, Loader=yaml.FullLoader)

    data = [Row("A1", 123, '2019-01-01 10:30:00', 'request_ride'),
               Row("A2", 234, '2019-01-01 11:00:00', 'request_ride'),
               Row("A1", 123, '2019-01-01 11:10:00', 'payment'),
               Row("A3", 456, '2019-01-01 12:00:00', 'request_ride'),
               Row("A4", 567, '2019-01-01 12:10:00', 'cancel_ride'),
               Row("A3", 456, '2019-01-01 12:10:00', 'payment'),
               Row("A2", 234, '2019-01-02 12:00:00', 'payment'),
               Row("A4", 567, '2019-02-01 12:00:00', 'request_ride'),
               Row("A1", 999, '2019-01-01 10:30:00', 'request_ride'),
               Row("A1", 999, '2019-02-01 12:30:00', 'payment')]
    schema = StructType().add("passenger_id", StringType(),False).add("ride_id", IntegerType() ,True)\
            .add("action_at", StringType(), False).add("action_type", StringType(), False)
                 #StructField("passenger_id", StringType(),False),\
                 #StructField("ride_id", IntegerType() ,True),\
                 #StructField("action_at", DateType(), False),\
                 #StructField("action_type", StringType(), False))


    data_df = spark.createDataFrame(data, schema)
    print("Printing DataFrame " )
    data_df.show(8)
    data_df.select(to_timestamp('action_at').alias('Time')).show()
    data_df.groupBy("ride_id").pivot("action_type").agg(first(to_timestamp("action_at"))).show()

    ## spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" assignments/ass1.py