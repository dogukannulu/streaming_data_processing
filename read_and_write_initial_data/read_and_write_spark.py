import os
import findspark
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

findspark.init("/opt/manual/spark")
spark = SparkSession.builder \
        .appName("Spark Read Write") \
        .master("local[2]") \
        .getOrCreate()


def create_separate_dataframes():
    dataframes = {}
    directory = '/home/train/datasets/KETI'
    dataframes_room = {}
    columns = ['co2', 'humidity', 'light', 'pir', 'temperature']
    count2 = 0
    for filename in os.listdir(directory):
        new_directory = directory + '/' + filename
        count = 0
        for new_files in os.listdir(new_directory):
            f = os.path.join(new_directory, new_files)
            f_hdfs = f.replace('home', 'user')
            my_path = filename + '_' + new_files.split('.')[0]
            dataframes[my_path] = spark.read.csv(f'{f_hdfs}')

            dataframes[my_path] = dataframes[my_path].toDF('ts_min_bignt', columns[count])
            count += 1
            count2 += 1

        dataframes[f'{filename}_co2'].createOrReplaceTempView('df_co2')
        dataframes[f'{filename}_humidity'].createOrReplaceTempView('df_humidity')
        dataframes[f'{filename}_light'].createOrReplaceTempView('df_light')
        dataframes[f'{filename}_pir'].createOrReplaceTempView('df_pir')
        dataframes[f'{filename}_temperature'].createOrReplaceTempView('df_temperature')

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


get_dataframes_room = create_separate_dataframes()


def unionAll(dfs):
    return reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)


def create_main_dataframe(separate_dataframes=get_dataframes_room):
    dataframes_to_concat = []
    for i in separate_dataframes.values():
        dataframes_to_concat.append(i)

    df = reduce(DataFrame.unionAll, dataframes_to_concat)
    df = df.sort(F.col("ts_min_bignt"))

    df = df.dropna()

    df_main = df.withColumn("event_ts_min", F.from_unixtime(F.col("ts_min_bignt")).cast(DateType()))
    df_main = df_main.withColumn("event_ts_min", F.date_format(F.col("event_ts_min"), "yyyy-MM-dd HH:mm:ss"))

    print(df_main.show(10))
    print(df_main.count())
    return df_main


main_dataframe = create_main_dataframe(get_dataframes_room)


def write_main_dataframe(df=main_dataframe):
    df = df.toPandas()
    df.to_csv("/home/train/data-generator/input/sensors.csv")


if __name__ == "__main__":
    room_dataframes = create_separate_dataframes()
    main_df = create_main_dataframe(room_dataframes)
    write_main_dataframe(main_df)