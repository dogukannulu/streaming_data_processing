import sys
import warnings
import traceback
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

warnings.filterwarnings('ignore')
checkpointDir = "file:///tmp/streaming/kafka_office_input"
office_input_index = {
    "settings": {
        "index": {
            "analysis": {
                "analyzer": {
                    "custom_analyzer":
                        {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase", "custom_edge_ngram", "asciifolding"
                            ]
                        }
                },
                "filter": {
                    "custom_edge_ngram": {
                        "type": "edge_ngram",
                        "min_gram": 2,
                        "max_gram": 10
                    }
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "event_ts_min": {"type": "date",
            "format": "yyyy-MM-d hh:mm:ss||yyyy-MM-dd hh:mm:ss||yyyy-MM-dd HH:mm:ss||yyyy-MM-d HH:mm:ss",
            "ignore_malformed": "true"
      },
            "co2": {"type": "float"},
            "humidity": {"type": "float"},
            "light": {"type": "float"},
            "temperature": {"type": "float"},
            "room": {"type": "keyword"},
            "pir": {"type": "float"},
            "if_movement": {"type": "keyword"}

        }
    }
}


def create_spark_session():
    from pyspark.sql import SparkSession
    try:
        spark = (SparkSession.builder
                 .appName("Streaming Kafka Example")
                 .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.12.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")
                 .config("spark.driver.memory", "2048m")
                 .config("spark.sql.shuffle.partitions", 4)
                 .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                 .getOrCreate())
        logging.info('Spark session successfully created')
    except Exception:
        traceback.print_exc(file=sys.stderr)
        logging.error("Couldn't create the spark session")

    return spark


#SparkConf().getAll()
def create_initial_dataframe(spark_session):
    try:
        df = spark_session \
          .readStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "localhost:9092") \
          .option("subscribe", "office_input") \
          .load()
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


def create_elasticsearch_connection():
    from elasticsearch import Elasticsearch
    try:
        es = Elasticsearch("http://localhost:9200")
        logging.info(f"Connection {es} created succesfully")
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error("Couldn't create the final dataframe")

    return es


def check_if_index_exists(es):

    if es.indices.exists(index="office_input"):
        print("Index office_input already exists")
        logging.info("Index office_input already exists")
    else:
        es.indices.create(index="office_input", body=office_input_index)
        print("Index office_input created")
        logging.info("Index office_input created")


def start_streaming(df, es):
    logging.info("Streaming is being started...")
    my_query = (df.writeStream
                   .format("org.elasticsearch.spark.sql")
                   .outputMode("append")
                   .option("es.nodes", "localhost")
                   .option("es.port", "9200")
                   .option("es.resource", "office_input/_doc")
                   .option("checkpointLocation", checkpointDir)
                   .start())

    return my_query.awaitTermination()


if __name__ == '__main__':
    spark = create_spark_session()
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df, spark)
    es = create_elasticsearch_connection()
    check_if_index_exists(es)
    start_streaming(df_final, es)