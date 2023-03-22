WITH import_weather_api AS (
    SELECT * FROM {{source('weather','weather_api')}}
),

type_casts AS (
    SELECT
        CAST(last_data_update AS TIMESTAMP) AS read_date,
        CAST(region AS STRING) AS region,
        CAST(country AS STRING) AS country,
        CAST(weather_condition AS STRING) AS weather_condition,
        TRIM(CAST(icon AS STRING),'/') AS icon,
        CAST(latitude AS NUMERIC) AS latitude,
        CAST(longitude AS NUMERIC) AS longitude,
        CAST(temp_c AS NUMERIC) AS temp_c,
        CAST(humidity AS NUMERIC) AS humidity,
        CAST(wind_kph AS NUMERIC) AS wind_kph,
        CAST(pressure_mb AS INTEGER) AS pressure_mb,
        CAST(uv_val AS INTEGER) AS uv_val,
        CAST(upload_date AS TIMESTAMP) AS upload_date
    FROM
        import_weather_api
)

SELECT * FROM type_casts

