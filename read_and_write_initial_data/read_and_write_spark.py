"""
This script gets the zip folder and creates one dataframe from all csv files. Then, saves it to the local.
"""

import os
import findspark
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

findspark.init("/opt/manual/spark") # This is where local spark is installed
spark = SparkSession.builder \
        .appName("Spark Read Write") \
        .master("local[2]") \
        .getOrCreate()


def create_separate_dataframes() -> dict:
    """
    Creates a dictionary that includes room numbers as keys and dataframes per room as values
    """
    dataframes = {} # This dict saves property dataframe per room as values.
    directory = '/home/train/datasets/KETI'
    dataframes_room = {} # This dict saves dataframes per room.
    columns = ['co2', 'humidity', 'light', 'pir', 'temperature']
    count2 = 0
    for filename in os.listdir(directory): # loop through the folders under KETI
        new_directory = directory + '/' + filename # e.g. /home/train/datasets/KETI/656
        count = 0
        for new_files in os.listdir(new_directory): # loop through the files under each room folder
            f = os.path.join(new_directory, new_files) # e.g. /home/train/datasets/KETI/656/co2.csv
            f_hdfs = f.replace('home', 'user') # e.g. /user/train/datasets/KETI/656/co2.csv
            my_path = filename + '_' + new_files.split('.')[0] # e.g. 656_co2
            dataframes[my_path] = spark.read.csv(f'{f_hdfs}')

            dataframes[my_path] = dataframes[my_path].toDF('ts_min_bignt', columns[count])  # Sample key: 656_co2. Dataframe columns: ts_min_bignt, co2
            count += 1
            count2 += 1

        dataframes[f'{filename}_co2'].createOrReplaceTempView('df_co2')
        dataframes[f'{filename}_humidity'].createOrReplaceTempView('df_humidity')
        dataframes[f'{filename}_light'].createOrReplaceTempView('df_light')
        dataframes[f'{filename}_pir'].createOrReplaceTempView('df_pir')
        dataframes[f'{filename}_temperature'].createOrReplaceTempView('df_temperature')

        # Below sql joins on ts_min_bignt and creates the dataframe per room
        dataframes_room[filename] = spark.sql('''
        select
          df_co2.*,
          df_humidity.humidity,
          df_light.light,
          df_pir.pir,
          df_temperature.temperature        
        from df_co2
        inner join df_humidity
          on df_co2.ts_min_bignt = df_humidity.ts_min_bignt
        inner join df_light
          on df_humidity.ts_min_bignt = df_light.ts_min_bignt
        inner join df_pir
          on df_light.ts_min_bignt = df_pir.ts_min_bignt
        inner join df_temperature
          on df_pir.ts_min_bignt = df_temperature.ts_min_bignt      
        ''')

        dataframes_room[filename] = dataframes_room[filename].withColumn("room", F.lit(filename))
    return dataframes_room


def unionAll(dfs):
    """
    Merges multiple dataframes vertically.
    """
    return reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)


def create_main_dataframe(separate_dataframes:dict):
    """
    Merges all per-room dataframes vertically. Creates final dataframe.
    """
    dataframes_to_concat = []
    for i in separate_dataframes.values():
        dataframes_to_concat.append(i)

    df = reduce(DataFrame.unionAll, dataframes_to_concat)
    df = df.sort(F.col("ts_min_bignt")) # All data is sorted according to ts_min_bignt. We want it to stream according to timestamp.

    df = df.dropna()

    df_main = df.withColumn("event_ts_min", F.from_unixtime(F.col("ts_min_bignt")).cast(DateType()))
    df_main = df_main.withColumn("event_ts_min", F.date_format(F.col("event_ts_min"), "yyyy-MM-dd HH:mm:ss")) # Create datetime column

    return df_main


def write_main_dataframe(df):
    """
    Writes the final dataframe to the local.
    """
    df = df.toPandas()
    df.to_csv("/home/train/data-generator/input/sensors.csv")


if __name__ == "__main__":
    room_dataframes = create_separate_dataframes()
    main_df = create_main_dataframe(room_dataframes)
    write_main_dataframe(main_df)
