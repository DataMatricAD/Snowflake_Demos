
USE DATABASE RETAILDB;
-- create integration object that contains the access information
CREATE OR REPLACE STORAGE INTEGRATION azure_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = '*******-*****-****-****-**********'
  STORAGE_ALLOWED_LOCATIONS = ('azure://snowflakeaccountazure.blob.core.windows.net/csv', 'azure://snowflakeaccountazure.blob.core.windows.net/json');

  
-- Describe integration object to provide access
DESC STORAGE integration azure_integration;