with import_air_quality AS (
    SELECT * FROM {{ref('stg_weather_api__air_quality')}}
),

filter_tables AS (
    SELECT
        read_date,
        city,
        pm2_5
    FROM
        import_air_quality
)

SELECT * FROM filter_tables