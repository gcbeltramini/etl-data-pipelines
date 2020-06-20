from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# Example in https://airflow.apache.org/docs/stable/tutorial.html
default_args = {
    'owner': 'etl-data-pipelines',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

AWS_CREDENTIALS = 'aws_credentials'

dag = DAG('etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_key='log_data/',  # 'log_data/{execution_date.year}/{execution_date.month}/'
    table='public.staging_events',
    json_path='s3://udacity-dend/log_json_path.json',
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_key='song_data/',  # part of the data: 'song_data/A/A/A'
    table='public.staging_songs',
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='public.songplays',
    query=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='public.users',
    query=SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='public.songs',
    query=SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='public.artists',
    query=SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='public.time',
    query=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

(start_operator
 >> [stage_events_to_redshift, stage_songs_to_redshift]
 >> load_songplays_table
 >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
 >> run_quality_checks
 >> end_operator)
