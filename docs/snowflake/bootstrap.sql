-- =============================================================================
-- WAGA Snowflake Bootstrap
-- =============================================================================
-- Run this script as ACCOUNTADMIN in the Snowflake Web Console.
-- It creates all infrastructure needed for the WAGA pipeline.
--
-- Prerequisites:
--   1. Generate a key-pair (see Key-Pair Setup section below)
--   2. Have the public key content ready to paste
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Role
-- -----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS WAGA_TRANSFORM
    COMMENT = 'Service role for the WAGA pipeline (Dagster + dbt + dlt)';

-- -----------------------------------------------------------------------------
-- 2. Warehouse
-- -----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS WAGA_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Compute warehouse for WAGA pipeline';

-- -----------------------------------------------------------------------------
-- 3. Database + Schemas
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS WAGA
    COMMENT = 'Weather Adjusted Generation Analytics';

CREATE SCHEMA IF NOT EXISTS WAGA.RAW
    COMMENT = 'dlt landing zone (merge disposition)';

CREATE SCHEMA IF NOT EXISTS WAGA.STAGING
    COMMENT = 'dbt staging views';

CREATE SCHEMA IF NOT EXISTS WAGA.MARTS
    COMMENT = 'dbt contracted mart tables';

CREATE SCHEMA IF NOT EXISTS WAGA.ANALYTICS
    COMMENT = 'Polars analytics outputs';

-- -----------------------------------------------------------------------------
-- 4. Service Account User
-- -----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS WAGA_PIPELINE
    DEFAULT_ROLE = WAGA_TRANSFORM
    DEFAULT_WAREHOUSE = WAGA_WH
    COMMENT = 'Service account for WAGA pipeline (key-pair auth, no password)';

-- Paste your public key here after generating it (see below):
-- ALTER USER WAGA_PIPELINE SET RSA_PUBLIC_KEY = '<paste-public-key-content-here>';

-- -----------------------------------------------------------------------------
-- 5. Grants
-- -----------------------------------------------------------------------------
GRANT USAGE ON WAREHOUSE WAGA_WH TO ROLE WAGA_TRANSFORM;

GRANT USAGE ON DATABASE WAGA TO ROLE WAGA_TRANSFORM;
GRANT CREATE SCHEMA ON DATABASE WAGA TO ROLE WAGA_TRANSFORM;

GRANT USAGE ON ALL SCHEMAS IN DATABASE WAGA TO ROLE WAGA_TRANSFORM;
GRANT CREATE TABLE ON ALL SCHEMAS IN DATABASE WAGA TO ROLE WAGA_TRANSFORM;
GRANT CREATE VIEW ON ALL SCHEMAS IN DATABASE WAGA TO ROLE WAGA_TRANSFORM;
GRANT SELECT ON ALL TABLES IN DATABASE WAGA TO ROLE WAGA_TRANSFORM;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE WAGA TO ROLE WAGA_TRANSFORM;

-- Future grants so new tables/views inherit permissions
GRANT SELECT ON FUTURE TABLES IN DATABASE WAGA TO ROLE WAGA_TRANSFORM;
GRANT INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE WAGA TO ROLE WAGA_TRANSFORM;
GRANT SELECT ON FUTURE VIEWS IN DATABASE WAGA TO ROLE WAGA_TRANSFORM;

GRANT ROLE WAGA_TRANSFORM TO USER WAGA_PIPELINE;

-- =============================================================================
-- Key-Pair Setup (run locally, NOT in Snowflake)
-- =============================================================================
--
-- 1. Generate RSA private key (PKCS#8, no passphrase):
--
--    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM \
--      -out ~/.snowflake/waga_rsa_key.p8 -nocrypt
--
-- 2. Extract the public key:
--
--    openssl rsa -in ~/.snowflake/waga_rsa_key.p8 \
--      -pubout -out ~/.snowflake/waga_rsa_key.pub
--
-- 3. Base64-encode the private key for env vars:
--
--    base64 -i ~/.snowflake/waga_rsa_key.p8 | tr -d '\n'
--
--    -> This value goes into WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64
--
-- 4. Set the public key on the Snowflake user:
--
--    Copy the content between -----BEGIN PUBLIC KEY----- and
--    -----END PUBLIC KEY----- (without the markers), paste as a single
--    line, and run:
--
--    ALTER USER WAGA_PIPELINE SET RSA_PUBLIC_KEY = '<single-line-content>';
--
-- 5. Verify the key works:
--
--    DESC USER WAGA_PIPELINE;
--    -- RSA_PUBLIC_KEY_FP should show a fingerprint
-- =============================================================================
