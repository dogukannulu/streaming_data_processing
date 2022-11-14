from airflow import DAG
from datetime import datetime, timedelta

from kafka_admin_client.kafka_admin_client import create_new_topic

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

start_date = datetime(2022, 10, 19, 12, 20)

default_args = {
    'owner': 'train',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('streaming_data_processing_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    download_data = BashOperator(task_id='download_data',
                                 bash_command='wget -O /home/train/datasets/sensors.zip https://github.com/dogukannulu/datasets/raw/master/sensors_instrumented_in_an_office_building_dataset.zip',
                                 retries=1, retry_delay=timedelta(seconds=15))

    unzip_file = BashOperator(task_id='unzip_file',
                              bash_command='unzip /home/train/datasets/sensors_instrumented_in_an_office_building_dataset.zip -d /home/train/datasets/',
                              retries=2, retry_delay=timedelta(seconds=15))

    remove_readme = BashOperator(task_id='remove_readme',
                                 bash_command='rm /home/train/datasets/KETI/README.txt')

    put_data_to_hdfs = BashOperator(task_id='put_data_to_hdfs',
                                    bash_command='/opt/manual/hadoop-3.1.2/bin/hdfs dfs -put ~/datasets/KETI/ /user/train/datasets/',
                                    retries=2, retry_delay=timedelta(seconds=15),
                                    execution_timeout=timedelta(minutes=10))

    run_spark = BashOperator(task_id='run_spark',
                             bash_command='source ~/venvspark/bin/activate &&'
                                          'python3 /home/train/PycharmProjects/read_and_write_initial_data/read_and_write_spark.py',
                             retries=2, retry_delay=timedelta(seconds=15),
                             execution_timeout=timedelta(minutes=20))

    copy_to_datasets = BashOperator(task_id='copy_to_datasets',
                                    bash_command='cp /home/train/data-generator/input/sensors.csv /home/train/datasets')
    
    create_new_topic = BranchPythonOperator(task_id='create_new_topic', python_callable=create_new_topic)

    topic_created = DummyOperator(task_id="topic_created",)

    topic_exists = DummyOperator(task_id="topic_already_exists")

    run_data_generator = BashOperator(task_id="run_data_generator",
                                      bash_command="/home/train/venvairflow/dags/dataframe_to_kafka.sh ",
                                      execution_timeout=timedelta(minutes=15),
                                      trigger_rule='none_failed_or_skipped')

    spark_write_to_es = BashOperator(task_id="spark_write_to_es",
                                     bash_command="/home/train/venvairflow/dags/spark_to_es.sh ",
                                     execution_timeout=timedelta(minutes=15),
                                     trigger_rule='none_failed_or_skipped')

    spark_write_to_minio = BashOperator(task_id="spark_write_to_minio",
                                     bash_command="/home/train/venvairflow/dags/spark_to_minio.sh ",
                                     execution_timeout=timedelta(minutes=15),
                                     trigger_rule='none_failed_or_skipped')

    download_data >> unzip_file >> remove_readme >> put_data_to_hdfs >> run_spark >> copy_to_datasets >> create_new_topic >> [topic_created, topic_exists]
    [topic_created, topic_exists] >> run_data_generator
    [topic_created, topic_exists] >> spark_write_to_es
    [topic_created, topic_exists] >> spark_write_to_minio
