with import_weather AS (
    SELECT * FROM {{ref('stg_weather_api__weather')}}
),

import_air_quality AS (
    SELECT * FROM {{ref('stg_weather_api__air_quality')}}
),

join_tables AS (
    SELECT
        iw.read_date AS read_date,
        iw.city AS city,
        iw.region AS region,
        iw.country AS country,
        iw.weather_condition AS weather_condition,
        iw.icon AS weather_icon,
        CONCAT(iw.latitude,',',iw.longitude) AS lat_long,
        iw.temp_c AS temp_c,
        iw.humidity AS humidity,
        iw.wind_kph AS wind_kph,
        iw.pressure_mb AS pressure_mb,
        iw.uv_val AS uv_val,
        ia.co AS co,
        ia.no2 AS no2,
        ia.o3 AS o3,
        ia.so2 AS so2,
        ia.pm2_5 AS pm2_5,
        ia.pm10 AS pm10,
        ia.us_epa_index
    FROM
        import_weather iw
        LEFT JOIN import_air_quality ia
            ON iw.read_date = ia.read_date
            AND iw.city = ia.city
)

SELECT * FROM join_tables