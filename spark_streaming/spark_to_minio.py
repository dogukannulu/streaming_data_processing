"""
This script gets the streaming data from Kafka topic, then writes it to MinIO
"""

import sys
import warnings
import traceback
import logging
from pyspark import SparkConf, SparkContext

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')# One can see the logs to understand possible errors better. Log levels were determined due to method importance.

warnings.filterwarnings('ignore')
checkpointDir = "file:///tmp/streaming/minio_streaming" # Historical data is kept here. Can be deleted after each run for development purposes.


def create_spark_session():
    """
    Creates the Spark Session with suitable configs.
    """
    from pyspark.sql import SparkSession
    try:
        # Spark session is established with kafka jar. Suitable versions can be found in Maven repository.
        spark = (SparkSession.builder
                 .appName("Streaming Kafka Example")
                 .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")
                 .getOrCreate())
        logging.info('Spark session successfully created')
    except Exception as e:
        traceback.print_exc(file=sys.stderr) # To see traceback of the error.
        logging.error(f"Couldn't create the spark session due to exception: {e}")

    return spark


def read_minio_credentials():
    """
    Gets the MinIO config files from local with configparser.
    """
    import configparser

    config = configparser.RawConfigParser()
    try:
        config.read('/home/train/.aws/credentials')
        config.sections()
        accessKeyId = config.get('minio', 'aws_access_key_id')
        secretAccessKey = config.get('minio', 'aws_secret_access_key')
        logging.info('MinIO credentials is obtained correctly')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"MinIO credentials couldn't be obtained due to exception: {e}")

    return accessKeyId, secretAccessKey


def load_minio_config(spark_context: SparkContext):
    """
    Establishes the necessary configurations to access to MinIO.
    """
    accessKeyId, secretAccessKey = read_minio_credentials()
    try:
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", accessKeyId)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secretAccessKey)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://127.0.0.1:9000")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        logging.info('MinIO configuration is created successfully')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"MinIO config could not be created successfully due to exception: {e}")


def create_initial_dataframe(spark_session):
    """
    Reads the streaming data and creates the initial dataframe accordingly.
    """
    try:
        # Gets the streaming data from topic office_input.
        df = (spark_session
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "office_input")
            .load())
        logging.info("Initial dataframe created successfully")
    except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")

    return df


def create_final_dataframe(df, spark_session):
    """
    Modifies the initial dataframe, and creates the final dataframe.
    """
    from pyspark.sql.types import IntegerType, FloatType, StringType
    from pyspark.sql import functions as F
    df2 = df.selectExpr("CAST(value AS STRING)") # Get only the value part of the topic message.

    df3 = df2.withColumn("ts_min_bignt", F.split(F.col("value"), ",")[0].cast(IntegerType())) \
        .withColumn("co2", F.split(F.col("value"), ",")[1].cast(FloatType())) \
        .withColumn("humidity", F.split(F.col("value"), ",")[2].cast(FloatType())) \
        .withColumn("light", F.split(F.col("value"), ",")[3].cast(FloatType())) \
        .withColumn("pir", F.split(F.col("value"), ",")[4].cast(FloatType())) \
        .withColumn("temperature", F.split(F.col("value"), ",")[5].cast(FloatType())) \
        .withColumn("room", F.split(F.col("value"), ",")[6].cast(StringType())) \
        .withColumn("event_ts_min", F.split(F.col("value"), ",")[7].cast(StringType())) \
        .drop(F.col("value")) # Define data types of all columns.

    df3.createOrReplaceTempView("df3")

    # Below adds the if_movement column. This column shows the situation of the movement depending on the pir column.
    df4 = spark_session.sql("""
    select
      event_ts_min,
      co2,
      humidity,
      light,
      temperature,
      room,
      pir,
      case
        when pir > 0 then 'movement'
        else 'no_movement'
      end as if_movement
    from df3

    """)
    logging.info("Final dataframe created successfully")
    return df4


def start_streaming(df):
    """
    Starts the streaming to index office_input in elasticsearch.
    """
    logging.info("Streaming is being started...")
    stream_query = (df.writeStream
                        .format("csv")
                        .outputMode("append")
                        .option('header', 'true')
                        .option("checkpointLocation", checkpointDir)
                        .option("path", 's3a://my_bucket/office_input') # The bucket my_bucket can be created via UI
                        .start())

    return stream_query.awaitTermination()


if __name__ == '__main__':
    spark = create_spark_session()
    load_minio_config(spark.sparkContext)
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df, spark)
    start_streaming(df_final)
