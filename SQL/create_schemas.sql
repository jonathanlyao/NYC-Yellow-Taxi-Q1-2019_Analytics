CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

-- Alter schema owner to user
ALTER SCHEMA raw  OWNER TO nyc_taxi_user;
ALTER SCHEMA stg  OWNER TO nyc_taxi_user;
ALTER SCHEMA core OWNER TO nyc_taxi_user;
ALTER SCHEMA mart OWNER TO nyc_taxi_user;

-- Grant to user
GRANT USAGE,CREATE ON SCHEMA raw TO nyc_taxi_user;
GRANT USAGE,CREATE ON SCHEMA stg TO nyc_taxi_user;
GRANT USAGE,CREATE ON SCHEMA core TO nyc_taxi_user;
GRANT USAGE,CREATE ON SCHEMA mart TO nyc_taxi_user;

-- Alter role
ALTER ROLE nyc_taxi_user
SET search_path= raw, stg, core, mart, public;

-- Check if the role has been successfully created
SELECT rolname FROM pg_roles WHERE rolname='nyc_taxi_user';

-- Check if all the schema has been successfully created
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('raw','stg','core','mart')
ORDER BY schema_name;