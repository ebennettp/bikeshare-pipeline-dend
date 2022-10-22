"""
SQL queries used by other modules to run on the cluster
"""

class SqlQueries:

	trips_stage_table_create = ("""
		CREATE TABLE IF NOT EXISTS public.staging_trips (
			ride_id varchar,
			rideable_type varchar,
			started_at timestamp,
			ended_at timestamp,
			start_station_name varchar,
			start_station_id int,
			end_station_name varchar,
			end_station_id int,
			start_lat float,
			start_lng float,
			end_lat float,
			end_lng float,
			member_casual varchar);
		""")

	weather_stage_table_create = ("""
		CREATE TABLE IF NOT EXISTS public.staging_weather (
			time timestamp,
			temp float,
			dwpt float,
			rhum float,
			prcp float,
			snow float,
			wdir float,
			wspd float,
			wpgt float,
			pres float,
			tsun float,
			coco float);
		""")

	trips_table_create = ("""
		CREATE TABLE IF NOT EXISTS public.f_trips(
			ride_id 			varchar PRIMARY KEY,
			rideable_type 		varchar,
			start_time 			timestamp NOT NULL,
			station_id 			int NOT NULL,
			member_casual 		varchar);
	""")

	weather_table_create = ("""
		CREATE TABLE IF NOT EXISTS public.d_weather (
			time 			timestamp PRIMARY KEY,
			temp 			float,
			humidity 		float,
			precipitation 	float,
			snow 			float,
			wind_direction 	float,
			wind_speed 		float,
			peak_wind_gust 	float,
			air_pressure 	float,
			sunshine 		float,
			condition_code 	float);
		""")

	stations_table_create = ("""
		CREATE TABLE IF NOT EXISTS public.d_stations(
			station_id			varchar PRIMARY KEY,
			station_lat			decimal(8,6) NOT NULL,
			station_lon			decimal(9,6) NOT NULL,
			station_name		varchar);
	""")

	time_table_create = ("""
	    CREATE TABLE IF NOT EXISTS public.d_time(
	    	start_time 	timestamp PRIMARY KEY,
	    	hour 		int4,
	    	day 		int4,
	    	month 		varchar,
	    	year 		int4);
	    """)
	
	trips_table_insert = ("""
		SELECT DISTINCT
			ride_id,
			rideable_type,
			started_at AS start_time,
			start_station_id AS station_id,
			member_casual
		FROM staging_trips
		WHERE 
			ride_id IS NOT NULL AND
			start_time IS NOT NULL AND
			station_id IS NOT NULL
	""")
	
	weather_table_insert = ("""
		SELECT DISTINCT
			"time",
			temp,
			rhum AS humidity,
			prcp AS precipitation,
			snow,
			wdir AS wind_direction,
			wspd AS wind_speed,
			pres AS air_pressure,
			tsun AS sunshine,
			coco AS condition_code
		FROM staging_weather
	""")
	
	stations_table_insert = ("""
		SELECT DISTINCT
			start_station_id AS station_id,
			start_lat AS station_lat,
			start_lng AS station_lon,
			end_station_name AS station_name
		FROM staging_trips
		WHERE station_id IS NOT NULL
	""")
	
	time_table_insert = ("""
	    SELECT started_at AS start_time, extract(hour from start_time), extract(day from start_time), 
	           extract(month from start_time), extract(year from start_time)
	    FROM staging_trips
	""")
