select * from new;



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
    OR et0_fao_evapotranspiration IS NULL ;

-- delete all null
begin transaction
delete
FROM final_new
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
    WHEN weather_code BETWEEN 1 AND 3 THEN 'Cloudy'
    WHEN weather_code BETWEEN 40 AND 49 THEN 'Fog'
    WHEN weather_code BETWEEN 50 AND 59 THEN 'Drizzle'
    WHEN weather_code BETWEEN 60 AND 69 THEN 'Rain'
    WHEN weather_code BETWEEN 70 AND 79 THEN 'Snow'
    WHEN weather_code BETWEEN 80 AND 99 THEN 'Storm'
    ELSE 'Unknown'
END;

select
    id,
    MONTH(date) AS month,
    DAY(date) AS day,  
    date,
    weather_code,
    temperature_2m_mean,
    temperature_2m_max,
    temperature_2m_min,
    wind_speed_10m_max,
    wind_gusts_10m_max,
    wind_direction_10m_dominant,
    shortwave_radiation_sum,
    et0_fao_evapotranspiration,
	weather_status,
    LEAD(weather_status) OVER (ORDER BY id) AS next_day_weather_status
into final_new from new;

-- delete date 2025-4-23
begin transaction
delete from final_new where format(date,'yyyy-M-d') = '2025-4-23'
commit
select date from final_new where format(date,'yyyy-M-d') = '2025-4-23'
select format(date,'yyyy-M-d') from final_new --where format(date,'yyyy-M-d') = '1940-1-1'
select count(weather_status) as count ,weather_status from new group by  weather_status;
select count(weather_status)  from new ;
