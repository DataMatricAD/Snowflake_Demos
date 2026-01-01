-- create file format
create or replace file format MANAGE_DB.public.fileformat_gcp
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

-- create stage object
create or replace stage MANAGE_DB.public.stage_gcp
    STORAGE_INTEGRATION = gcp_integration
    URL = 'gcs://snowflakebucket2025'
    FILE_FORMAT = MANAGE_DB.public.fileformat_gcp;

LIST @MANAGE_DB.public.stage_gcp;



---- Query files & Load data ----

--query files
SELECT 
$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
$12,$13,$14,$15,$16,$17,$18,$19,$20
FROM @MANAGE_DB.public.stage_gcp;


create or replace table MANAGE_DB.public.happiness_gcs (
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
    
    
COPY INTO MANAGE_DB.public.happiness_gcs
FROM @MANAGE_DB.public.stage_gcp;

SELECT * FROM MANAGE_DB.public.happiness_gcs;
