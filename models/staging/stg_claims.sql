{{
    config(
        materialized = 'view',
        tags = ['staging', 'claims']
    )
}}

WITH source_data AS (

    SELECT

        /* =====================================================
           CLAIM IDENTIFIERS
           ===================================================== */

        CLAIM_ID::NUMBER(38, 0) AS CLAIM_ID,

        NULLIF(
            TRIM(CLAIM_NUMBER),
            ''
        ) AS CLAIM_NUMBER,


        /* =====================================================
           FOREIGN KEYS
           ===================================================== */

        POLICY_ID::NUMBER(38, 0) AS POLICY_ID,

        MEMBER_ID::NUMBER(38, 0) AS MEMBER_ID,

        PROVIDER_ID::NUMBER(38, 0) AS PROVIDER_ID,


        /* =====================================================
           CLAIM ATTRIBUTES
           ===================================================== */

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

           TRY_TO_DATE prevents malformed source values from
           causing the transformation to fail.

           Invalid values become NULL and should be captured
           through DQ testing.
           ===================================================== */

        TRY_TO_DATE(SERVICE_DATE) AS SERVICE_DATE,

        TRY_TO_DATE(RECEIVED_DATE) AS RECEIVED_DATE,


        /* =====================================================
           FINANCIAL AMOUNTS

           Source:
               NUMERIC(18,2)

           Staging:
               NUMBER(18,2)

           TRY_TO_DECIMAL prevents malformed numeric values
           from failing the transformation.

           Invalid values become NULL and should be captured
           through DQ testing.
           ===================================================== */

        TRY_TO_DECIMAL(
            REPORTED_AMOUNT,
            18,
            2
        ) AS REPORTED_AMOUNT,

        TRY_TO_DECIMAL(
            ALLOWED_AMOUNT,
            18,
            2
        ) AS ALLOWED_AMOUNT,

        TRY_TO_DECIMAL(
            APPROVED_AMOUNT,
            18,
            2
        ) AS APPROVED_AMOUNT,

        TRY_TO_DECIMAL(
            PAID_AMOUNT,
            18,
            2
        ) AS PAID_AMOUNT,

        TRY_TO_DECIMAL(
            MEMBER_RESPONSIBILITY,
            18,
            2
        ) AS MEMBER_RESPONSIBILITY_AMOUNT,


        /* =====================================================
           CLAIM CLASSIFICATION
           ===================================================== */

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


        /* =====================================================
           SOURCE TIMESTAMPS
           ===================================================== */

        TRY_TO_TIMESTAMP_NTZ(
            CREATED_TIMESTAMP
        ) AS CREATED_TIMESTAMP,

        TRY_TO_TIMESTAMP_NTZ(
            UPDATED_TIMESTAMP
        ) AS UPDATED_TIMESTAMP,


        /* =====================================================
           INGESTION METADATA
           ===================================================== */

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

        TRY_TO_TIMESTAMP_NTZ(
            INGESTED_TS
        ) AS INGESTED_TS

    FROM {{ source('ameritas_raw', 'CLAIMS') }}

),


/* =========================================================
   RECORD HASH

   Hash is generated AFTER standardization.

   Therefore:

       ' CLM10001 '
       'CLM10001'

   produce the same standardized value and therefore the
   same record hash.

   This is preferable to hashing raw source formatting.
   ========================================================= */

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


/* =========================================================
   DEDUPLICATION

   Business key:
       CLAIM_ID

   Latest record wins based on:

       1. UPDATED_TIMESTAMP
       2. INGESTED_TS
       3. BATCH_ID
       4. SOURCE_FILE_NAME

   NULLS LAST ensures that records with missing timestamps
   or metadata don't incorrectly become the latest record.
   ========================================================= */

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


/* =========================================================
   FINAL STAGING OUTPUT
   ========================================================= */

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