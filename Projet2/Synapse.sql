CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Aymen2003';

-- CREATING A SCOPE
CREATE DATABASE SCOPED CREDENTIAL storage_credential
WITH IDENTITY = 'Managed Identity';

-- DEFINING DATA SOURCE
CREATE EXTERNAL DATA SOURCE gold_data_source
WITH (
    TYPE = HADOOP,
    LOCATION = 'storage_account_path',
    CREDENTIAL = storage_credential
);


--DEFINE FORMAT
CREATE EXTERNAL FILE FORMAT ParquetFileFormat
WITH (
    FORMAT_TYPE = PARQUET
);


-- CREATE TABLES
-- VEHICLE DIMENSION
CREATE EXTERNAL TABLE dbo.dim_vehicle (
    vehicle_id VARCHAR(50),
    vin VARCHAR(17),           -- typical VINs are 17 characters
    make VARCHAR(50),
    model VARCHAR(50),
    year INT,
    battery_kwh INT,
    odo_km FLOAT               -- use FLOAT instead of DOUBLE (T-SQL does not support DOUBLE)
)
WITH (
    LOCATION = 'gold/dim_vehicle/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);

select * from dbo.dim_vehicle;




-- Station DIMENSION
CREATE EXTERNAL TABLE dbo.dim_station (
    station_id       VARCHAR(50),
    station_name     VARCHAR(100),
    station_power_kw INT,
    station_lat      FLOAT,
    station_lon      FLOAT
)
WITH (
    LOCATION = 'gold/dim_station/',      -- folder inside gold container
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);

-- Test
SELECT * FROM dbo.dim_station;








DROP EXTERNAL TABLE dbo.dim_driver;

CREATE EXTERNAL TABLE dbo.dim_driver (
    station_id       VARCHAR(50),
    station_name     VARCHAR(100),
    effective_from   DATETIME2,
    surrogate_key    BIGINT,
    effective_to     DATETIME2,
    is_current       INT      
)
WITH (
    LOCATION = 'gold/dim_driver_parquet/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);


-- Test
SELECT * FROM dbo.dim_driver;




DROP EXTERNAL TABLE dbo.dim_time;
CREATE EXTERNAL TABLE dbo.dim_time (
    timestamp     DATETIME2,
    date          DATE,
    hour          INT,
    minute        INT,
    second        INT,
    day_of_week   VARCHAR(20),
    month         INT,
    year          INT,
    is_weekend    INT,
    surrogate_key BIGINT
)
WITH (
    LOCATION = 'dim_time_parquet_fixed/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);


SELECT * FROM dbo.dim_time;




CREATE EXTERNAL TABLE dbo.fact_charging (
    fact_id             BIGINT,
    vehicle_id          VARCHAR(50),
    station_id          VARCHAR(50),
    driver_sk           BIGINT,
    time_sk             BIGINT,
    timestamp           DATETIME2,
    event_date          DATE,
    speed_kmh           FLOAT,
    soc                 INT,
    power_kw            FLOAT,
    distance_delta_km   FLOAT,
    odo_km              FLOAT,
    is_charging         INT,
    event_ingestion_time DATETIME2
)
WITH (
    LOCATION = 'gold/fact_parquet/',   
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);

select * from dbo.fact_charging;



ALTER TABLE dbo.fact_charging
ADD CONSTRAINT FK_fact_vehicle
FOREIGN KEY (vehicle_id)
REFERENCES dbo.dim_vehicule(vehicle_id);