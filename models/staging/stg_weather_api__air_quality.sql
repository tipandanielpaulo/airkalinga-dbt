WITH import_weather_api AS (
    SELECT * FROM {{source('weather','air_quality_api')}}
),

type_casts AS (
    SELECT
        CAST(read_date AS TIMESTAMP) AS read_date,
        CAST(region AS STRING) AS region,
        CAST(country AS STRING) AS country,
        CAST(latitude AS NUMERIC) AS latitude,
        CAST(longitude AS NUMERIC) AS longitude,
        CAST(co AS NUMERIC) AS co,
        CAST(no2 AS NUMERIC) AS no2,
        CAST(o3 AS NUMERIC) AS o3,
        CAST(so2 AS INTEGER) AS so2,
        CAST(pm2_5 AS INTEGER) AS pm2_5,
        CAST(pm10 AS INTEGER) AS pm10,
        CAST(us_epa_index AS INTEGER) AS us_epa_index,
        CAST(upload_date AS TIMESTAMP) AS upload_date
    FROM
        import_weather_api
    WHERE 
        1=1
    qualify ROW_NUMBER() OVER (PARTITION BY id ORDER BY upload_date DESC) = 1
)


SELECT * FROM type_casts