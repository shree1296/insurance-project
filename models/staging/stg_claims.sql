{{
    config(
        materialized = 'table',
        tags = ['staging', 'claims']
    )
}}


-- ============================================================
-- MODEL: STG_CLAIMS
-- ============================================================
--
-- PURPOSE:
--   Standardize, cleanse, hash, and deduplicate claim records
--   coming from the RAW layer.
--
-- SOURCE:
--   AMERITAS.RAW.CLAIMS
--
-- TARGET:
--   AMERITAS.STAGING.STG_CLAIMS
--
-- GRAIN:
--   One row per CLAIM_ID
--
-- RESPONSIBILITIES:
--   1. Standardize identifiers and text
--   2. Preserve source datatypes
--   3. Standardize financial precision
--   4. Generate a deterministic RECORD_HASH
--   5. Deduplicate multiple versions of the same claim
--
-- IMPORTANT:
--   RAW.CLAIMS already contains strongly typed Snowflake columns.
--   Therefore we do NOT use TRY_TO_DATE(), TRY_TO_DECIMAL(),
--   or TRY_TO_TIMESTAMP_NTZ() on values that are already
--   DATE, NUMBER, or TIMESTAMP_NTZ.
--
--   TRY_* functions are appropriate when the incoming value is
--   stored as an uncertain/string datatype and conversion itself
--   is part of the transformation.
--
-- ============================================================


WITH source_data AS (

    SELECT

        /* =====================================================
           CLAIM IDENTIFIERS
           ===================================================== */

        -- Source:
        --     NUMBER(38,0)
        --
        -- Staging:
        --     NUMBER(38,0)

        CLAIM_ID::NUMBER(38,0) AS CLAIM_ID,

        -- Remove leading/trailing whitespace.
        -- Convert empty strings to NULL.

        NULLIF(
            TRIM(CLAIM_NUMBER),
            ''
        ) AS CLAIM_NUMBER,


        /* =====================================================
           FOREIGN KEYS
           ===================================================== */

        POLICY_ID::NUMBER(38,0) AS POLICY_ID,

        MEMBER_ID::NUMBER(38,0) AS MEMBER_ID,

        PROVIDER_ID::NUMBER(38,0) AS PROVIDER_ID,


        /* =====================================================
           CLAIM ATTRIBUTES
           ===================================================== */

        -- Standardize categorical text to uppercase.

        NULLIF(
            UPPER(TRIM(CLAIM_TYPE)),
            ''
        ) AS CLAIM_TYPE,

        NULLIF(
            UPPER(TRIM(CLAIM_STATUS)),
            ''
        ) AS CLAIM_STATUS,


        /* =====================================================
           CLAIM DATES
           ===================================================== */

        -- RAW.CLAIMS already stores these columns as DATE.
        --
        -- Therefore a TRY_TO_DATE() conversion is unnecessary.
        --
        -- We explicitly preserve the DATE datatype.

        SERVICE_DATE::DATE AS SERVICE_DATE,

        RECEIVED_DATE::DATE AS RECEIVED_DATE,


        /* =====================================================
           FINANCIAL AMOUNTS
           ===================================================== */

        -- Source:
        --     NUMBER(18,2)
        --
        -- Staging:
        --     NUMBER(18,2)
        --
        -- Financial amounts should remain exact DECIMAL/NUMBER
        -- values. FLOAT should NOT be used for these fields.

        REPORTED_AMOUNT::NUMBER(18,2) AS REPORTED_AMOUNT,

        ALLOWED_AMOUNT::NUMBER(18,2) AS ALLOWED_AMOUNT,

        APPROVED_AMOUNT::NUMBER(18,2) AS APPROVED_AMOUNT,

        PAID_AMOUNT::NUMBER(18,2) AS PAID_AMOUNT,

        MEMBER_RESPONSIBILITY::NUMBER(18,2)
            AS MEMBER_RESPONSIBILITY_AMOUNT,


        /* =====================================================
           CLAIM CLASSIFICATION
           ===================================================== */

        -- Normalize diagnosis codes to uppercase.

        NULLIF(
            UPPER(TRIM(DIAGNOSIS_CODE)),
            ''
        ) AS DIAGNOSIS_CODE,

        -- Normalize place-of-service values.

        NULLIF(
            UPPER(TRIM(PLACE_OF_SERVICE)),
            ''
        ) AS PLACE_OF_SERVICE,

        -- Preserve natural casing of free-text descriptions.
        -- Only trim surrounding whitespace.

        NULLIF(
            TRIM(CLAIM_DESCRIPTION),
            ''
        ) AS CLAIM_DESCRIPTION,


        /* =====================================================
           SOURCE TIMESTAMPS
           ===================================================== */

        -- RAW.CLAIMS already contains TIMESTAMP_NTZ values.
        --
        -- Do NOT use:
        --
        --     TRY_TO_TIMESTAMP_NTZ(CREATED_TIMESTAMP)
        --
        -- because the source is already TIMESTAMP_NTZ.
        --
        -- Explicit casts document the intended target datatype.

        CREATED_TIMESTAMP::TIMESTAMP_NTZ
            AS CREATED_TIMESTAMP,

        UPDATED_TIMESTAMP::TIMESTAMP_NTZ
            AS UPDATED_TIMESTAMP,


        /* =====================================================
           INGESTION METADATA
           ===================================================== */

        -- Metadata fields are strings in RAW.
        -- Standardize empty strings to NULL.

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

        -- INGESTED_TS is already TIMESTAMP_NTZ in RAW.

        INGESTED_TS::TIMESTAMP_NTZ AS INGESTED_TS


    FROM {{ source('ameritas_raw', 'CLAIMS') }}

),


/* ============================================================
   RECORD HASH
   ============================================================

   PURPOSE:
       Generate a deterministic hash representing the
       standardized business content of the claim.

   IMPORTANT:
       Hashing occurs AFTER standardization.

   Example:

       ' CLM10001 '
             |
             v
       'CLM10001'

   Therefore insignificant source formatting differences do
   not create different hashes.

   RECORD_HASH can later support:

       - Change detection
       - Incremental processing
       - Auditing
       - Comparing source and target records

   RECORD_HASH is NOT the business key.

   Business key:
       CLAIM_ID

   ============================================================ */

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

                COALESCE(CLAIM_ID::VARCHAR, ''),
                COALESCE(CLAIM_NUMBER, ''),

                COALESCE(POLICY_ID::VARCHAR, ''),
                COALESCE(MEMBER_ID::VARCHAR, ''),
                COALESCE(PROVIDER_ID::VARCHAR, ''),

                COALESCE(CLAIM_TYPE, ''),
                COALESCE(CLAIM_STATUS, ''),

                COALESCE(SERVICE_DATE::VARCHAR, ''),
                COALESCE(RECEIVED_DATE::VARCHAR, ''),

                COALESCE(REPORTED_AMOUNT::VARCHAR, ''),
                COALESCE(ALLOWED_AMOUNT::VARCHAR, ''),
                COALESCE(APPROVED_AMOUNT::VARCHAR, ''),
                COALESCE(PAID_AMOUNT::VARCHAR, ''),
                COALESCE(MEMBER_RESPONSIBILITY_AMOUNT::VARCHAR, ''),

                COALESCE(DIAGNOSIS_CODE, ''),
                COALESCE(PLACE_OF_SERVICE, ''),
                COALESCE(CLAIM_DESCRIPTION, ''),

                COALESCE(CREATED_TIMESTAMP::VARCHAR, ''),
                COALESCE(UPDATED_TIMESTAMP::VARCHAR, '')

            ),
            256
        ) AS RECORD_HASH


    FROM source_data

),


/* ============================================================
   DEDUPLICATION
   ============================================================

   GRAIN:
       One row per CLAIM_ID

   WHY DEDUPLICATION IS REQUIRED:
       The RAW layer may contain multiple versions of the same
       claim because of:

           - Source updates
           - Reprocessing
           - Re-ingestion
           - Multiple ingestion batches

   BUSINESS KEY:
       CLAIM_ID

   RECORD SELECTION PRIORITY:

       1. Most recent UPDATED_TIMESTAMP
       2. Most recent INGESTED_TS
       3. Highest/latest BATCH_ID
       4. SOURCE_FILE_NAME as final tie-breaker

   NULLS LAST:
       Missing timestamps should not incorrectly win over
       records that contain valid timestamps.

   IMPORTANT:
       This deduplication is appropriate for CLAIMS because
       CLAIM_ID represents the claim-level business entity.

       We should NOT blindly apply the same logic to event or
       transaction tables such as:

           CLAIM_PAYMENTS
           CLAIM_STATUS_HISTORY
           CLAIM_DOCUMENTS
           CLAIM_NOTES

       because multiple legitimate records can exist for
       the same CLAIM_ID in those tables.

   ============================================================ */

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


/* ============================================================
   FINAL STAGING OUTPUT
   ============================================================

   Only the latest record for each CLAIM_ID is retained.

   FINAL GRAIN:

       ONE ROW PER CLAIM_ID

   ============================================================ */

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