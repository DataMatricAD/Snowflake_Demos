-- create integration object that contains the access information
CREATE STORAGE INTEGRATION gcp_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflakebucket2025/csv', 'gcs://snowflakebucketjson2025/json');

  
-- Describe integration object to provide access
DESC STORAGE integration gcp_integration;
