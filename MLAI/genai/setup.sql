USE DATABASE LSP_DB;

CREATE OR REPLACE SCHEMA CORTEX_DEMO;

CREATE OR REPLACE WAREHOUSE lp_cortex_wh WITH
     WAREHOUSE_SIZE='X-SMALL'
     AUTO_SUSPEND = 120
     AUTO_RESUME = TRUE
     INITIALLY_SUSPENDED=TRUE;

 USE WAREHOUSE lp_cortex_wh;

CREATE OR REPLACE STAGE LSP_DB.CORTEX_DEMO.file_stage
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

CREATE OR REPLACE TABLE LSP_DB.CORTEX_DEMO.raw_text AS
SELECT
    RELATIVE_PATH,
    TO_VARCHAR (
        SNOWFLAKE.CORTEX.PARSE_DOCUMENT (
            '@LSP_DB.CORTEX_DEMO.file_stage',
            RELATIVE_PATH,
            {'mode': 'LAYOUT'} ):content
        ) AS EXTRACTED_LAYOUT
FROM
    DIRECTORY('@LSP_DB.CORTEX_DEMO.file_stage')
WHERE
    RELATIVE_PATH LIKE '%.pdf';

SELECT * FROM LSP_DB.CORTEX_DEMO.raw_text;

CREATE OR REPLACE TABLE LSP_DB.CORTEX_DEMO.doc_chunks AS
SELECT
    relative_path,
    BUILD_SCOPED_FILE_URL(@LSP_DB.CORTEX_DEMO.file_stage, relative_path) AS file_url,
    (
        relative_path || ':\n'
        || coalesce(c.value['headers']['header_1'], '')
        || coalesce(' -> ' || c.value['headers']['header_2'], '')
        || coalesce(' -> ' || c.value['headers']['header_3'], '')
        || c.value['chunk']
    ) AS chunk,
    'English' AS language
FROM
    LSP_DB.CORTEX_DEMO.raw_text,
    LATERAL FLATTEN(SNOWFLAKE.CORTEX.SPLIT_TEXT_MARKDOWN_HEADER(
        EXTRACTED_LAYOUT,
        OBJECT_CONSTRUCT('#', 'header_1', '##', 'header_2', '###', 'header_3'),
        2000, -- chunks of 2000 characters
        300 -- 300 character overlap
    )) c;

SELECT * FROM LSP_DB.CORTEX_DEMO.doc_chunks;

CREATE OR REPLACE CORTEX SEARCH SERVICE LSP_DB.CORTEX_DEMO.search
    ON chunk
    ATTRIBUTES language
    WAREHOUSE = lp_cortex_wh
    TARGET_LAG = '1 hour'
    AS (
    SELECT
        chunk,
        relative_path,
        file_url,
        language
    FROM LSP_DB.CORTEX_DEMO.doc_chunks
    );