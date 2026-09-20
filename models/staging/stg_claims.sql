{{
    config(
        materialized = 'table',
        tags = ['staging', 'claims']
    )
}}

-- ============================================================
-- MODEL: STG_CLAIMS
-- PURPOSE:
--   Transform RAW.CLAIMS into a clean, standardized staging
--   table before the data is consumed by MART dimensions/facts.
--
-- GRAIN:
--   One row per CLAIM_ID.
--
-- KEY TRANSFORMATIONS:
--   1. Standardize text values using TRIM / UPPER
--   2. Convert blank strings to NULL
--   3. Preserve existing Snowflake DATE / NUMBER / TIMESTAMP types
--   4. Generate a deterministic record hash
--   5. Deduplicate CLAIM_ID using the latest available record
--   6. Preserve ingestion / source metadata for lineage
--
-- IMPORTANT:
--   RAW columns are already typed as DATE, NUMBER and TIMESTAMP_NTZ.
--   Therefore we use explicit casts (::TYPE) instead of TRY_TO_*
--   functions. TRY_TO_* functions are primarily useful when the
--   incoming value is a string or otherwise potentially malformed.
-- ============================================================


WITH source_data AS (

    SELECT

        -- ========================================================
        -- BUSINESS / PRIMARY KEY
        -- ========================================================

        CLAIM_ID::NUMBER(38,0) AS CLAIM_ID,

        NULLIF(
            TRIM(CLAIM_NUMBER),
            ''
        ) AS CLAIM_NUMBER,


        -- ========================================================
        -- FOREIGN KEYS
        -- ========================================================

        POLICY_ID::NUMBER(38,0) AS POLICY_ID,

        MEMBER_ID::NUMBER(38,0) AS MEMBER_ID,

        PROVIDER_ID::NUMBER(38,0) AS PROVIDER_ID,


        -- ========================================================
        -- CLAIM CLASSIFICATION
        -- Standardize text values.
        -- Example:
        --   ' medical ' -> 'MEDICAL'
        --   ''          -> NULL
        -- ========================================================

        NULLIF(
            UPPER(TRIM(CLAIM_TYPE)),
            ''
        ) AS CLAIM_TYPE,

        NULLIF(
            UPPER(TRIM(CLAIM_STATUS)),
            ''
        ) AS CLAIM_STATUS,


        -- ========================================================
        -- DATES
        --
        -- RAW columns are already DATE columns.
        -- Use direct casting rather than TRY_TO_DATE().
        -- ========================================================

        SERVICE_DATE::DATE AS SERVICE_DATE,

        RECEIVED_DATE::DATE AS RECEIVED_DATE,


        -- ========================================================
        -- FINANCIAL AMOUNTS
        --
        -- Preserve exact decimal semantics.
        -- Do NOT use FLOAT for monetary values.
        -- ========================================================

        REPORTED_AMOUNT::NUMBER(18,2) AS REPORTED_AMOUNT,

        ALLOWED_AMOUNT::NUMBER(18,2) AS ALLOWED_AMOUNT,

        APPROVED_AMOUNT::NUMBER(18,2) AS APPROVED_AMOUNT,

        PAID_AMOUNT::NUMBER(18,2) AS PAID_AMOUNT,

        MEMBER_RESPONSIBILITY::NUMBER(18,2)
            AS MEMBER_RESPONSIBILITY_AMOUNT,


        -- ========================================================
        -- CLAIM DETAILS
        -- ========================================================

        NULLIF(
            UPPER(TRIM(DIAGNOSIS_CODE)),
            ''
        ) AS DIAGNOSIS_CODE,

        NULLIF(
            UPPER(TRIM(PLACE_OF_SERVICE)),
            ''
        ) AS PLACE_OF_SERVICE,

        NULLIF(
            TRIM(CLAIM_DESCRIPTION),
            ''
        ) AS CLAIM_DESCRIPTION,


        -- ========================================================
        -- SOURCE TIMESTAMPS
        --
        -- RAW columns are already TIMESTAMP_NTZ.
        --
        -- DO NOT use:
        --   TRY_TO_TIMESTAMP_NTZ(CREATED_TIMESTAMP)
        --
        -- because Snowflake does not allow TRY_CAST from
        -- TIMESTAMP_NTZ to TIMESTAMP_NTZ in this context.
        -- ========================================================

        CREATED_TIMESTAMP::TIMESTAMP_NTZ
            AS CREATED_TIMESTAMP,

        UPDATED_TIMESTAMP::TIMESTAMP_NTZ
            AS UPDATED_TIMESTAMP,


        -- ========================================================
        -- INGESTION / LINEAGE METADATA
        -- ========================================================

        NULLIF(
            TRIM(BATCH_ID),
            ''
        ) AS BATCH_ID,

        NULLIF(
            TRIM(SOURCE_SYSTEM),
            ''
        ) AS SOURCE_SYSTEM,

        NULLIF(
            TRIM(SOURCE_FILE_NAME),
            ''
        ) AS SOURCE_FILE_NAME,

        NULLIF(
            TRIM(SOURCE_FILE_PATH),
            ''
        ) AS SOURCE_FILE_PATH,

        INGESTED_TS::TIMESTAMP_NTZ
            AS INGESTED_TS

    FROM {{ source('ameritas_raw', 'CLAIMS') }}

),


-- ============================================================
-- RECORD HASH
--
-- Purpose:
--   Creates a deterministic fingerprint of the important
--   business attributes of the claim.
--
-- Useful for:
--   - Change detection
--   - Incremental processing
--   - Data reconciliation
--   - Audit / lineage
-- ============================================================

hashed_data AS (

    SELECT

        CLAIM_ID,

        CLAIM_NUMBER,

        POLICY_ID,

        MEMBER_ID,

        PROVIDER_ID,

        CLAIM_TYPE,

        CLAIM_STATUS,

        SERVICE_DATE,

        RECEIVED_DATE,

        REPORTED_AMOUNT,

        ALLOWED_AMOUNT,

        APPROVED_AMOUNT,

        PAID_AMOUNT,

        MEMBER_RESPONSIBILITY_AMOUNT,

        DIAGNOSIS_CODE,

        PLACE_OF_SERVICE,

        CLAIM_DESCRIPTION,

        CREATED_TIMESTAMP,

        UPDATED_TIMESTAMP,

        BATCH_ID,

        SOURCE_SYSTEM,

        SOURCE_FILE_NAME,

        SOURCE_FILE_PATH,

        INGESTED_TS,


        SHA2(
            CONCAT_WS(
                '|',

                COALESCE(
                    CLAIM_ID::VARCHAR,
                    ''
                ),

                COALESCE(
                    CLAIM_NUMBER,
                    ''
                ),

                COALESCE(
                    POLICY_ID::VARCHAR,
                    ''
                ),

                COALESCE(
                    MEMBER_ID::VARCHAR,
                    ''
                ),

                COALESCE(
                    PROVIDER_ID::VARCHAR,
                    ''
                ),

                COALESCE(
                    CLAIM_TYPE,
                    ''
                ),

                COALESCE(
                    CLAIM_STATUS,
                    ''
                ),

                COALESCE(
                    SERVICE_DATE::VARCHAR,
                    ''
                ),

                COALESCE(
                    RECEIVED_DATE::VARCHAR,
                    ''
                ),

                COALESCE(
                    REPORTED_AMOUNT::VARCHAR,
                    ''
                ),

                COALESCE(
                    ALLOWED_AMOUNT::VARCHAR,
                    ''
                ),

                COALESCE(
                    APPROVED_AMOUNT::VARCHAR,
                    ''
                ),

                COALESCE(
                    PAID_AMOUNT::VARCHAR,
                    ''
                ),

                COALESCE(
                    MEMBER_RESPONSIBILITY_AMOUNT::VARCHAR,
                    ''
                ),

                COALESCE(
                    DIAGNOSIS_CODE,
                    ''
                ),

                COALESCE(
                    PLACE_OF_SERVICE,
                    ''
                ),

                COALESCE(
                    CLAIM_DESCRIPTION,
                    ''
                ),

                COALESCE(
                    CREATED_TIMESTAMP::VARCHAR,
                    ''
                ),

                COALESCE(
                    UPDATED_TIMESTAMP::VARCHAR,
                    ''
                )
            ),
            256
        ) AS RECORD_HASH

    FROM source_data

),


-- ============================================================
-- DEDUPLICATION
--
-- BUSINESS RULE:
--   Keep exactly one record per CLAIM_ID.
--
-- WINNER:
--   1. Latest UPDATED_TIMESTAMP
--   2. Latest INGESTED_TS
--   3. Latest BATCH_ID
--   4. Latest SOURCE_FILE_NAME
--
-- NULLS LAST:
--   A record with a NULL UPDATED_TIMESTAMP should not win over
--   a record with a valid timestamp.
--
-- IMPORTANT:
--   This logic is appropriate for CLAIMS because CLAIM_ID
--   represents the business grain of this staging model.
--
--   Do NOT blindly apply this logic to event/transaction tables
--   such as CLAIM_PAYMENTS or CLAIM_STATUS_HISTORY, where multiple
--   rows per CLAIM_ID are legitimate.
-- ============================================================

ranked_claims AS (

    SELECT

        CLAIM_ID,

        CLAIM_NUMBER,

        POLICY_ID,

        MEMBER_ID,

        PROVIDER_ID,

        CLAIM_TYPE,

        CLAIM_STATUS,

        SERVICE_DATE,

        RECEIVED_DATE,

        REPORTED_AMOUNT,

        ALLOWED_AMOUNT,

        APPROVED_AMOUNT,

        PAID_AMOUNT,

        MEMBER_RESPONSIBILITY_AMOUNT,

        DIAGNOSIS_CODE,

        PLACE_OF_SERVICE,

        CLAIM_DESCRIPTION,

        CREATED_TIMESTAMP,

        UPDATED_TIMESTAMP,

        BATCH_ID,

        SOURCE_SYSTEM,

        SOURCE_FILE_NAME,

        SOURCE_FILE_PATH,

        INGESTED_TS,

        RECORD_HASH,


        ROW_NUMBER() OVER (

            PARTITION BY CLAIM_ID

            ORDER BY

                UPDATED_TIMESTAMP DESC NULLS LAST,

                INGESTED_TS DESC NULLS LAST,

                BATCH_ID DESC NULLS LAST,

                SOURCE_FILE_NAME DESC NULLS LAST

        ) AS RN

    FROM hashed_data

)


-- ============================================================
-- FINAL STAGING DATASET
--
-- One row per CLAIM_ID.
-- ============================================================

SELECT

    CLAIM_ID,

    CLAIM_NUMBER,

    POLICY_ID,

    MEMBER_ID,

    PROVIDER_ID,

    CLAIM_TYPE,

    CLAIM_STATUS,

    SERVICE_DATE,

    RECEIVED_DATE,

    REPORTED_AMOUNT,

    ALLOWED_AMOUNT,

    APPROVED_AMOUNT,

    PAID_AMOUNT,

    MEMBER_RESPONSIBILITY_AMOUNT,

    DIAGNOSIS_CODE,

    PLACE_OF_SERVICE,

    CLAIM_DESCRIPTION,

    CREATED_TIMESTAMP,

    UPDATED_TIMESTAMP,

    BATCH_ID,

    SOURCE_SYSTEM,

    SOURCE_FILE_NAME,

    SOURCE_FILE_PATH,

    INGESTED_TS,

    RECORD_HASH

FROM ranked_claims

WHERE RN = 1