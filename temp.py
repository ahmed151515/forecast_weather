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
	"latitude": [60.1087, 24.6877, 30.0626, 1.2897, 19.0728, 64.8378, 47.9077],
	"longitude": [-113.6426, 46.7219, 31.2497, 103.8501, 72.8826, -147.7164, 106.8832],
	"start_date": "1940-01-01",
	"end_date": "2025-04-23",
	"daily": ["weather_code", "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum", "et0_fao_evapotranspiration"],
	"hourly": "temperature_2m",
	"timezone": "auto"
}
responses = openmeteo.weather_api(url, params=params)
data = []
# Process first location. Add a for-loop for multiple locations or weather models
for response in responses:
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

	daily_dataframe = pd.DataFrame(data = daily_data)
	print(daily_dataframe)
	data.append(daily_dataframe)













final_dataframe = pd.concat(data, ignore_index=True)

# نحفظه في ملف CSV جديد
final_dataframe.to_csv("new.csv", index=False)

print("Saved all data into 'new.csv' successfully.")
