CREATE OR REPLACE DATABASE SNOWPIPE;

-- create integration object that contains the access information
CREATE OR REPLACE STORAGE INTEGRATION azure_snowpipe_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID =  '*******************************************'
  STORAGE_ALLOWED_LOCATIONS = ( 'azure://snowflakeaccountazure.blob.core.windows.net/snowpipe');

  
  
-- Describe integration object to provide access
DESC STORAGE integration azure_snowpipe_integration;

---- Create file format & stage objects ----

-- create file format
create database snowpipe

create or replace file format snowpipe.public.fileformat_azure
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

-- create stage object
create or replace stage snowpipe.public.stage_azure
    STORAGE_INTEGRATION = azure_snowpipe_integration
    URL = 'azure://snowflakeaccountazure.blob.core.windows.net/snowpipe'
    FILE_FORMAT = fileformat_azure;
    

-- list files
LIST @snowpipe.public.stage_azure;


CREATE OR REPLACE NOTIFICATION INTEGRATION snowpipe_event
  ENABLED = true
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://snowflakeaccountazure.queue.core.windows.net/snowpipequeue'
  AZURE_TENANT_ID = '*******************************************';
  
  
  -- Register Integration
  
  DESC notification integration snowpipe_event;


--query file
SELECT 
$1,
$2,
$3,
$4,
$5,
$6,
$7,
$8,
$9,
$10,
$11,
$12,
$13,
$14,
$15,
$16,
$17,
$18,
$19,
$20
FROM @snowpipe.public.stage_azure;


-- create destination table
create or replace table snowpipe.public.happiness (
    country_name varchar,
    regional_indicator varchar,
    ladder_score number(4,3),
    standard_error number(4,3),
    upperwhisker number(4,3),
    lowerwhisker number(4,3),
    logged_gdp number(5,3),
    social_support number(4,3),
    healthy_life_expectancy number(5,3),
    freedom_to_make_life_choices number(4,3),
    generosity number(4,3),
    perceptions_of_corruption number(4,3),
    ladder_score_in_dystopia number(4,3),
    explained_by_log_gpd_per_capita number(4,3),
    explained_by_social_support number(4,3),
    explained_by_healthy_life_expectancy number(4,3),
    explained_by_freedom_to_make_life_choices number(4,3),
    explained_by_generosity number(4,3),
    explained_by_perceptions_of_corruption number(4,3),
    dystopia_residual number (4,3));
    


COPY INTO HAPPINESS
FROM @snowpipe.public.stage_azure;

SELECT * FROM snowpipe.public.happiness;

TRUNCATE TABLE snowpipe.public.happiness;

CREATE OR REPLACE FILE FORMAT SNOWPIPE.PUBLIC.FF_HAPPINESS_CSV
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;  -- optional (see note below)


CREATE OR REPLACE PIPE SNOWPIPE.PUBLIC.AZURE_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = 'SNOWPIPE_EVENT'
AS
COPY INTO SNOWPIPE.PUBLIC.HAPPINESS
FROM @SNOWPIPE.PUBLIC.STAGE_AZURE
FILE_FORMAT = (FORMAT_NAME = SNOWPIPE.PUBLIC.FF_HAPPINESS_CSV);

ALTER PIPE SNOWPIPE.PUBLIC.AZURE_PIPE REFRESH;

  SELECT SYSTEM$PIPE_STATUS( 'AZURE_PIPE' );

  select * from snowpipe.public.happiness


-----------end of snowpipe test------------
 