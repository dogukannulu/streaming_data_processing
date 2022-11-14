"""
This script gets the zip folder and creates one dataframe from all csv files. Then, saves it to the local.
"""

import os
import pandas as pd
from functools import reduce

directory = '/home/train/datasets/KETI'
dataframes = {}
dataframes_room = {}
columns = ['co2', 'humidity', 'light', 'pir', 'temperature']


def create_separate_dataframes() -> dict:
    """
    Creates a dictionary that includes room numbers as keys and dataframes per room as values
    """
    count2 = 0
    for filename in os.listdir(directory):
        new_directory = directory + '/' + filename
        count = 0
        for new_files in os.listdir(new_directory):
            f = os.path.join(new_directory, new_files)
            my_path = new_directory.split('/')[-1] + '_' + new_files.split('.')[0] # e.g. 656_co2
            dataframes[my_path] = pd.read_csv(f, names=['ts_min_bignt', columns[count]])  # Sample key: 656_co2. Dataframe columns: ts_min_bignt, co2
            count += 1
            count2 += 1

        # Below creates dataframe per room
        dataframes_room[filename] = reduce(lambda left, right:
                         pd.merge(left, right, on='ts_min_bignt', how='inner'),
                         [dataframes[f'{filename}_co2'], dataframes[f'{filename}_humidity'], \
                          dataframes[f'{filename}_light'], dataframes[f'{filename}_pir'],\
                          dataframes[f'{filename}_temperature']])
        dataframes_room[filename]['room'] = filename # adds room number as column.

    return dataframes_room


def create_main_dataframe(separate_dataframes:dict):
    """
    Concats all per-room dataframes vertically. Creates final dataframe.
    """
    dataframes_to_concat = []

    for i in separate_dataframes.values():
        dataframes_to_concat.append(i)

    df = pd.concat(dataframes_to_concat, ignore_index=True)
    df = df.sort_values('ts_min_bignt') # All data is sorted according to ts_min_bignt. We want it to stream according to timestamp.
   
    df.dropna(inplace=True)
    df["event_ts_min"] = pd.to_datetime(df["ts_min_bignt"], unit='s') # Create datetime column
    return df


def write_main_dataframe(df):
    """
    Writes the final dataframe to the local.
    """
    df.to_csv('/home/train/data-generator/input/sensors.csv', index=False)


if __name__ == '__main__':
    all_dataframes = create_separate_dataframes()
    main_dataframe = create_main_dataframe(all_dataframes)
    write_main_dataframe(main_dataframe)
