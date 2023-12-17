"""
Run queries to DROP existing tables and CREATE new ones
on the Redshift cluster connected to Airflow connection
"""

from datetime import datetime, timedelta

from airflow import DAG

from bikeshare.plugins.helpers.sql_queries import SqlQueries
from bikeshare.plugins.operators.create_table import CreateTableOperator

default_args = {
    "owner": "shizu",
    "start_date": datetime.utcnow(),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "create_tables", default_args=default_args, description="Create tables in Redshift"
)

create_trips_stage_table = CreateTableOperator(
    task_id="create_trips_stage_table",
    dag=dag,
    conn_id="redshift",
    table="staging_trips",
    sql=SqlQueries.trips_stage_table_create,
)

create_weather_stage_table = CreateTableOperator(
    task_id="create_weather_stage_table",
    dag=dag,
    conn_id="redshift",
    table="staging_weather",
    sql=SqlQueries.weather_stage_table_create,
)

create_trips_table = CreateTableOperator(
    task_id="create_trips_fact_table",
    dag=dag,
    conn_id="redshift",
    table="f_trips",
    sql=SqlQueries.trips_table_create,
)

create_time_table = CreateTableOperator(
    task_id="create_time_table",
    dag=dag,
    table="d_time",
    conn_id="redshift",
    sql=SqlQueries.time_table_create,
)

create_stations_table = CreateTableOperator(
    task_id="create_stations_table",
    dag=dag,
    conn_id="redshift",
    table="d_stations",
    sql=SqlQueries.stations_table_create,
)

create_weather_table = CreateTableOperator(
    task_id="create_weather_table",
    dag=dag,
    conn_id="redshift",
    table="d_weather",
    sql=SqlQueries.weather_table_create,
)

# tasks
create_trips_stage_table
create_weather_stage_table
create_trips_table
create_stations_table
create_time_table
create_weather_table
