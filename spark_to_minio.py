import sys
import warnings
import traceback
import findspark
import logging
from pyspark import SparkConf, SparkContext

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

warnings.filterwarnings('ignore')
#findspark.init("/opt/manual/spark")
checkpointDir = "file:///tmp/streaming/minio_streaming"



def create_spark_session():
    from pyspark.sql import SparkSession
    try:
        spark = (SparkSession.builder
                 .appName("Streaming Kafka Example")
                 .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")
                 .getOrCreate())
        logging.info('Spark session successfully created')
        print(spark.version)
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session due to exception: {e}")

    return spark


def read_minio_credentials():
    import configparser

    config = configparser.RawConfigParser()
    try:
        config.read('/home/train/.aws/credentials')
        config.sections()
        accessKeyId = config.get('minio', 'aws_access_key_id')
        secretAccessKey = config.get('minio', 'aws_secret_access_key')
        logging.info('MinIO credentials is established correctly')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"MinIO connection was not successful due to exception: {e}")

    return accessKeyId, secretAccessKey


def load_minio_config(spark_context: SparkContext):
    accessKeyId, secretAccessKey = read_minio_credentials()
    try:
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", accessKeyId)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secretAccessKey)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://127.0.0.1:9000")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        logging.info('MinIO configuration is set successfully')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"MinIO config could not be created successfully due to exception: {e}")


def create_initial_dataframe(spark_session):
    try:
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
    from pyspark.sql.types import IntegerType, FloatType, StringType
    from pyspark.sql import functions as F
    df2 = df.selectExpr("CAST(value AS STRING)")

    df3 = df2.withColumn("ts_min_bignt", F.split(F.col("value"), ",")[0].cast(IntegerType())) \
        .withColumn("co2", F.split(F.col("value"), ",")[1].cast(FloatType())) \
        .withColumn("humidity", F.split(F.col("value"), ",")[2].cast(FloatType())) \
        .withColumn("light", F.split(F.col("value"), ",")[3].cast(FloatType())) \
        .withColumn("pir", F.split(F.col("value"), ",")[4].cast(FloatType())) \
        .withColumn("temperature", F.split(F.col("value"), ",")[5].cast(FloatType())) \
        .withColumn("room", F.split(F.col("value"), ",")[6].cast(StringType())) \
        .withColumn("event_ts_min", F.split(F.col("value"), ",")[7].cast(StringType())) \
        .drop(F.col("value"))

    df3.createOrReplaceTempView("df3")

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
    logging.info("Streaming is being started...")
    stream_query = (df.writeStream
                        .format("csv")
                        .outputMode("append")
                        .option('header', 'true')
                        .option("checkpointLocation", checkpointDir)
                        .option("path", 's3a://dataops/office_input')
                        .start())

    return stream_query.awaitTermination()


if __name__ == '__main__':
    spark = create_spark_session()
    load_minio_config(spark.sparkContext)
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df, spark)
    start_streaming(df_final)