#!/usr/bin/env bash

set -e
source /home/train/data-generator/datagen/bin/activate
python /home/train/data-generator/dataframe_to_kafka.py -i /home/train/data-generator/input/sensors.csv -t office_input -rst 2
