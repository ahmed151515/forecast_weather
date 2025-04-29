import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time

geolocator = Nominatim(user_agent="weather_location_name")

def reverse_geocode(lat, lon, retries=3):
	for i in range(retries):
		try:
			location = geolocator.reverse((lat, lon), timeout=10)
			if location and location.address:
				return location.address.split(",")[0]  # تأخذ فقط اسم المكان الأول
		except GeocoderTimedOut:
			time.sleep(1)  # انتظار بسيط ثم إعادة المحاولة
	return f"{lat},{lon}"  # fallback

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
	"latitude": [1.3521, -1.2921, 33.4484, 47.8864,30.0626,37.9838, 51.5074, 55.7558, 64.8378, 71.2906, -90,30.0626],
	"longitude": [103.8198, 36.8219, -112.074, 106.9057,31.2497
              , 23.7275, -0.1278, 37.6173, -147.7164, -156.7886, 0],
	"start_date": "1980-01-01",
	"end_date": "2025-04-23",
	"daily": ["weather_code", "temperature_2m_mean", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum", "et0_fao_evapotranspiration", "rain_sum", "precipitation_sum", "snowfall_sum", "precipitation_hours", "cloud_cover_mean", "relative_humidity_2m_mean", "dew_point_2m_mean", "temperature_2m_min", "temperature_2m_max"],
	"timezone": ["auto", "auto", "auto", "auto", "auto", "auto", "auto", "auto", "auto", "auto","auto"]
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
data = []
for response in responses:
	print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
	print(f"Elevation {response.Elevation()} m asl")
	print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
	print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

	# Process daily data. The order of variables needs to be the same as requested.
	daily = response.Daily()
	name = reverse_geocode(response.Latitude(),response.Longitude())
	daily_weather_code = daily.Variables(0).ValuesAsNumpy()
	daily_temperature_2m_mean = daily.Variables(1).ValuesAsNumpy()
	daily_wind_speed_10m_max = daily.Variables(2).ValuesAsNumpy()
	daily_wind_gusts_10m_max = daily.Variables(3).ValuesAsNumpy()
	daily_wind_direction_10m_dominant = daily.Variables(4).ValuesAsNumpy()
	daily_shortwave_radiation_sum = daily.Variables(5).ValuesAsNumpy()
	daily_et0_fao_evapotranspiration = daily.Variables(6).ValuesAsNumpy()
	daily_rain_sum = daily.Variables(7).ValuesAsNumpy()
	daily_precipitation_sum = daily.Variables(8).ValuesAsNumpy()
	daily_snowfall_sum = daily.Variables(9).ValuesAsNumpy()
	daily_precipitation_hours = daily.Variables(10).ValuesAsNumpy()
	daily_cloud_cover_mean = daily.Variables(11).ValuesAsNumpy()
	daily_relative_humidity_2m_mean = daily.Variables(12).ValuesAsNumpy()
	daily_dew_point_2m_mean = daily.Variables(13).ValuesAsNumpy()
	daily_temperature_2m_min = daily.Variables(14).ValuesAsNumpy()
	daily_temperature_2m_max = daily.Variables(15).ValuesAsNumpy()

	daily_data = {"date": pd.date_range(
		start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
		end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
		freq = pd.Timedelta(seconds = daily.Interval()),
		inclusive = "left"
	)}

	daily_data["weather_code"] = daily_weather_code
	daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
	daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
	daily_data["wind_gusts_10m_max"] = daily_wind_gusts_10m_max
	daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant
	daily_data["shortwave_radiation_sum"] = daily_shortwave_radiation_sum
	daily_data["et0_fao_evapotranspiration"] = daily_et0_fao_evapotranspiration
	daily_data["rain_sum"] = daily_rain_sum
	daily_data["precipitation_sum"] = daily_precipitation_sum
	daily_data["snowfall_sum"] = daily_snowfall_sum
	daily_data["precipitation_hours"] = daily_precipitation_hours
	daily_data["cloud_cover_mean"] = daily_cloud_cover_mean
	daily_data["relative_humidity_2m_mean"] = daily_relative_humidity_2m_mean
	daily_data["dew_point_2m_mean"] = daily_dew_point_2m_mean
	daily_data["temperature_2m_min"] = daily_temperature_2m_min
	daily_data["temperature_2m_max"] = daily_temperature_2m_max
	daily_data["location"] = name

	daily_dataframe = pd.DataFrame(data = daily_data)
	print(daily_dataframe)
	data.append(daily_dataframe)

combined = pd.concat(data, ignore_index=True)
combined.to_csv("balanced_climate_samples.csv", index=False)
print("Saved balanced_climate_samples.csv with full feature set.")