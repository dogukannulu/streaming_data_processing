import os
import pandas as pd
from functools import reduce

directory = '/home/train/datasets/KETI'
dataframes = {}
dataframes_room = {}
columns = ['co2', 'humidity', 'light', 'pir', 'temperature']


def create_separate_dataframes():
    count2 = 0
    for filename in os.listdir(directory):
        new_directory = directory + '/' + filename
        count = 0
        for new_files in os.listdir(new_directory):
            f = os.path.join(new_directory, new_files)
        # checking if it is a file
            my_path = new_directory.split('/')[-1] + '_' + new_files.split('.')[0]
            dataframes[my_path] = pd.read_csv(f, names=['ts_min_bignt', columns[count]])
            count += 1
            count2 += 1

        dataframes_room[filename] = reduce(lambda left, right:     # Merge three pandas DataFrames
                         pd.merge(left, right, on='ts_min_bignt', how='inner'),
                         [dataframes[f'{filename}_co2'], dataframes[f'{filename}_humidity'], \
                          dataframes[f'{filename}_light'], dataframes[f'{filename}_pir'],\
                          dataframes[f'{filename}_temperature']])
        dataframes_room[filename]['room'] = filename

    return dataframes_room


get_dataframes_room = create_separate_dataframes()


def create_main_dataframe(separate_dataframes=get_dataframes_room):

    dataframes_to_concat = []

    for i in separate_dataframes.values():
        dataframes_to_concat.append(i)

    df = pd.concat(dataframes_to_concat, ignore_index=True)
    df = df.sort_values('ts_min_bignt')
   
    df.dropna(inplace=True)
    df["event_ts_min"] = pd.to_datetime(df["ts_min_bignt"], unit='s')
    return df


main_dataframe = create_main_dataframe(get_dataframes_room)


def write_main_dataframe(df = main_dataframe):
    df.to_csv('/home/train/data-generator/input/sensors.csv', index=False)


if __name__ == '__main__':
    all_dataframes = create_separate_dataframes()
    main_dataframe = create_main_dataframe(all_dataframes)
    print(main_dataframe.head(10))
    write_main_dataframe(main_dataframe)