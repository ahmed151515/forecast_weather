select * from openmeteo_cairo_1940_2025;

 -- delete first row it is null
begin transaction
delete from openmeteo_cairo_1940_2025 where id=0
commit
rollback


-- null check
SELECT *
FROM openmeteo_cairo_1940_2025
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

-- create final data with next_day_weather_code target
select *, LEAD(weather_code) over (order by date) as next_day_weather_code
into final_data from openmeteo_cairo_1940_2025 


select * from final_data
-- delete it next_day_weather_code is null 
begin transaction
delete  from final_data where next_day_weather_code is null 
commit
rollback

