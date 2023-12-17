from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from meteostat import Hourly, Stations


class WeatherToRedshift(BaseOperator):
    """
    Fetch hourly weather data between a time interval
    collected from a weather station in Washington D.C,
    stage resulting DataFrame into a Redshift table
    """

    def __init__(self, conn_id="", table="", *args, **kwargs):
        super(WeatherToRedshift, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.table = table

    def execute(self, context: dict):
        # Get nearest weather station
        stations = Stations().nearby(38.9072, -77.0369)  # Washington, D.C. Coordinates
        station = stations.fetch(1)

        # fetch pandas DataFrame
        df = Hourly(
            station,
            datetime.fromtimestamp(context["data_interval_start"].timestamp()),
            datetime.fromtimestamp(context["data_interval_end"].timestamp()),
        ).fetch()

        df.reset_index(0, inplace=True)  # transform time index to a column

        rds_hook = RedshiftSQLHook(self.conn_id)
        conn = rds_hook.get_sqlalchemy_engine()

        # write dataset to cluster
        df.to_sql(self.table, conn, index=False, if_exists="replace", method="multi")
