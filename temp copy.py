import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
	"latitude": 30.0626,
	"longitude": 31.2497,
	"start_date": "1940-01-01",
	"end_date": "2025-04-23",
	"daily": ["weather_code", "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum", "et0_fao_evapotranspiration", "precipitation_sum", "rain_sum", "snowfall_sum", "precipitation_hours", "dew_point_2m_mean", "cloud_cover_mean", "relative_humidity_2m_mean", "sunshine_duration", "daylight_duration", "sunset", "sunrise"],
	"hourly": "temperature_2m",
	"timezone": "auto"
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation {response.Elevation()} m asl")
print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

# Process hourly data. The order of variables needs to be the same as requested.
hourly = response.Hourly()
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)}

hourly_data["temperature_2m"] = hourly_temperature_2m

hourly_dataframe = pd.DataFrame(data = hourly_data)
print(hourly_dataframe)

# Process daily data. The order of variables needs to be the same as requested.
daily = response.Daily()
daily_weather_code = daily.Variables(0).ValuesAsNumpy()
daily_temperature_2m_mean = daily.Variables(1).ValuesAsNumpy()
daily_temperature_2m_max = daily.Variables(2).ValuesAsNumpy()
daily_temperature_2m_min = daily.Variables(3).ValuesAsNumpy()
daily_wind_speed_10m_max = daily.Variables(4).ValuesAsNumpy()
daily_wind_gusts_10m_max = daily.Variables(5).ValuesAsNumpy()
daily_wind_direction_10m_dominant = daily.Variables(6).ValuesAsNumpy()
daily_shortwave_radiation_sum = daily.Variables(7).ValuesAsNumpy()
daily_et0_fao_evapotranspiration = daily.Variables(8).ValuesAsNumpy()
daily_precipitation_sum = daily.Variables(9).ValuesAsNumpy()
daily_rain_sum = daily.Variables(10).ValuesAsNumpy()
daily_snowfall_sum = daily.Variables(11).ValuesAsNumpy()
daily_precipitation_hours = daily.Variables(12).ValuesAsNumpy()
daily_dew_point_2m_mean = daily.Variables(13).ValuesAsNumpy()
daily_cloud_cover_mean = daily.Variables(14).ValuesAsNumpy()
daily_relative_humidity_2m_mean = daily.Variables(15).ValuesAsNumpy()
daily_sunshine_duration = daily.Variables(16).ValuesAsNumpy()
daily_daylight_duration = daily.Variables(17).ValuesAsNumpy()
daily_sunset = daily.Variables(18).ValuesInt64AsNumpy()
daily_sunrise = daily.Variables(19).ValuesInt64AsNumpy()

daily_data = {"date": pd.date_range(
	start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
	end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = daily.Interval()),
	inclusive = "left"
)}

daily_data["weather_code"] = daily_weather_code
daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
daily_data["temperature_2m_max"] = daily_temperature_2m_max
daily_data["temperature_2m_min"] = daily_temperature_2m_min
daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
daily_data["wind_gusts_10m_max"] = daily_wind_gusts_10m_max
daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant
daily_data["shortwave_radiation_sum"] = daily_shortwave_radiation_sum
daily_data["et0_fao_evapotranspiration"] = daily_et0_fao_evapotranspiration
daily_data["precipitation_sum"] = daily_precipitation_sum
daily_data["rain_sum"] = daily_rain_sum
daily_data["snowfall_sum"] = daily_snowfall_sum
daily_data["precipitation_hours"] = daily_precipitation_hours
daily_data["dew_point_2m_mean"] = daily_dew_point_2m_mean
daily_data["cloud_cover_mean"] = daily_cloud_cover_mean
daily_data["relative_humidity_2m_mean"] = daily_relative_humidity_2m_mean
daily_data["sunshine_duration"] = daily_sunshine_duration
daily_data["daylight_duration"] = daily_daylight_duration
daily_data["sunset"] = daily_sunset
daily_data["sunrise"] = daily_sunrise

daily_dataframe = pd.DataFrame(data = daily_data)
print(daily_dataframe)

daily_dataframe.to_csv("data.csv")