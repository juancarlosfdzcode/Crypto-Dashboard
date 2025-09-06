
WITH source_data AS (
    SELECT 
        datetime,
        volume,
        market_cap,
        price,
        timestamp,
        coin,
        price * volume as turnover,
        market_cap / price as circulating_supply_approx
    FROM {{ source('crypto_database', 'crypto_data') }}
),

cleaned_data AS (
    SELECT 
        datetime,
        timestamp,
        coin,
        price,
        volume,
        market_cap,
        turnover,
        circulating_supply_approx,
        
        CASE WHEN price <= 0 THEN NULL ELSE price END AS clean_price,
        CASE WHEN volume < 0 THEN 0 ELSE volume END AS clean_volume,
        CASE WHEN market_cap <= 0 THEN NULL ELSE market_cap END AS clean_market_cap,
        
        DATE(datetime) AS price_date,
        extract(hour from datetime) AS price_hour,
        extract(dayofweek from datetime) AS day_of_week,
        extract(week from datetime) AS week_of_year,
        extract(month from datetime) AS month,
        extract(year from datetime) AS year
        
    FROM source_data
    WHERE coin IN ('Aave', 'Chainlink', 'Cronos')
      AND datetime IS NOT NULL
      AND price IS NOT NULL
)

SELECT * FROM cleaned_data