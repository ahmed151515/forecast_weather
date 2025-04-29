select * from new;

 -- delete first row it is null
begin transaction
delete from openmeteo_cairo_1940_2025 where id=0
commit
rollback


-- null check
SELECT *
FROM new
WHERE 
    date IS NULL
    OR weather_code IS NULL
    OR temperature_2m_mean IS NULL
    OR temperature_2m_max IS NULL
    OR temperature_2m_min IS NULL
    OR wind_speed_10m_max IS NULL
    OR wind_gusts_10m_max IS NULL
    OR wind_direction_10m_dominant IS NULL
    OR shortwave_radiation_sum IS NULL
    OR et0_fao_evapotranspiration IS NULL;

-- delete all null
begin transaction
delete
FROM new
WHERE 
    date IS NULL
    OR weather_code IS NULL
    OR temperature_2m_mean IS NULL
    OR temperature_2m_max IS NULL
    OR temperature_2m_min IS NULL
    OR wind_speed_10m_max IS NULL
    OR wind_gusts_10m_max IS NULL
    OR wind_direction_10m_dominant IS NULL
    OR shortwave_radiation_sum IS NULL
    OR et0_fao_evapotranspiration IS NULL;
commit
rollback
-- add weather_status 
alter table new add weather_status varchar(30)

-- fill weather_status 

UPDATE new
SET weather_status = CASE
    WHEN weather_code = 0 THEN 'Clear sky'
    WHEN weather_code = 1 THEN 'Mainly clear'
    WHEN weather_code = 2 THEN 'Partly cloudy'
    WHEN weather_code = 3 THEN 'Overcast'
    WHEN weather_code IN (45, 48) THEN 'Fog'
    WHEN weather_code IN (51, 53, 55) THEN 'Drizzle'
    WHEN weather_code IN (56, 57) THEN 'Freezing drizzle'
    WHEN weather_code IN (61, 63, 65) THEN 'Rain'
    WHEN weather_code IN (66, 67) THEN 'Freezing rain'
    WHEN weather_code IN (71, 73, 75) THEN 'Snow fall'
    WHEN weather_code = 77 THEN 'Snow grains'
    WHEN weather_code IN (80, 81, 82) THEN 'Rain showers'
    WHEN weather_code IN (85, 86) THEN 'Snow showers'
    WHEN weather_code = 95 THEN 'Thunderstorm'
    WHEN weather_code IN (96, 99) THEN 'Thunderstorm with hail'
    ELSE 'Unknown'
END;

select *, LEAD(weather_status)
into final_data from new;


select count(weather_status) as count ,weather_status from new group by  weather_status;
select count(weather_status)  from new ;
begin transaction
delete from new where weather_status in ('Mainly clear','Snow fall','Partly cloudy') 
commit
rollback