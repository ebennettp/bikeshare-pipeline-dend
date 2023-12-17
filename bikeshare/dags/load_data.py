"""
Extract and copy data to Redshift,
transform tables into a star schema,
check data integrity with custom queries
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from bikeshare.plugins.helpers.sql_queries import SqlQueries
from bikeshare.plugins.operators.data_check import DataQualityOperator
from bikeshare.plugins.operators.load_table import LoadTableOperator
from bikeshare.plugins.operators.s3_to_redshift import S3ToRedshift
from bikeshare.plugins.operators.weather_to_redshift import WeatherToRedshift

default_args = {
    "owner": "shizu",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="load_data",
    start_date=datetime(2020, 4, 15),  # April 15, 2020
    end_date=datetime.today() - timedelta(days=31),  # a month ago
    default_args=default_args,
    schedule=timedelta(days=30),
    description="Transform and load data into Redshift",
)

# operetors
start_operator = EmptyOperator(task_id="begin_execution", dag=dag)

stage_trips = S3ToRedshift(
    task_id="stage_trips_table",
    dag=dag,
    conn_id="redshift",
    aws_credentials="aws_credentials",
    table="staging_trips",
    s3_bucket="",  # s3 bucket name
    s3_key="{year}{month}-capitalbikeshare-tripdata.csv",
)

stage_weather = WeatherToRedshift(
    task_id="stage_weather_table", dag=dag, conn_id="redshift", table="staging_weather"
)

joint_operator = EmptyOperator(task_id="joint", dag=dag)

load_trips_table = LoadTableOperator(
    task_id="load_trips_table",
    dag=dag,
    conn_id="redshift",
    table="f_trips",
    sql=SqlQueries.trips_table_insert,
    truncate=True,
)

load_weather_table = LoadTableOperator(
    task_id="load_weather_table",
    dag=dag,
    conn_id="redshift",
    table="d_weather",
    sql=SqlQueries.weather_table_insert,
)

load_stations_table = LoadTableOperator(
    task_id="load_stations_table",
    dag=dag,
    conn_id="redshift",
    table="d_stations",
    sql=SqlQueries.stations_table_insert,
)

load_time_table = LoadTableOperator(
    task_id="load_time_table",
    dag=dag,
    conn_id="redshift",
    table="d_time",
    sql=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id="data_quality_check",
    dag=dag,
    conn_id="redshift",
    cases=[
        {
            "sql": "SELECT COUNT(ride_id) FROM f_trips WHERE ride_id IS NULL",
            "answer": 0,
        },
        {
            "sql": "SELECT COUNT(start_time) FROM f_trips WHERE start_time IS NULL",
            "answer": 0,
        },
    ],
)

end_operator = EmptyOperator(task_id="Stop_execution", dag=dag)

# Dependencies configuration
start_operator >> [stage_trips, stage_weather]

[stage_trips, stage_weather] >> joint_operator

joint_operator >> [
    load_trips_table,
    load_weather_table,
    load_stations_table,
    load_time_table,
]

[
    load_trips_table,
    load_weather_table,
    load_stations_table,
    load_time_table,
] >> run_quality_checks

run_quality_checks >> end_operator
