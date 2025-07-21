CREATE WAREHOUSE IF NOT EXISTS news_warehouse;
USE WAREHOUSE news_warehouse;

CREATE DATABASE IF NOT EXISTS news_database;
USE DATABASE news_database;

CREATE SCHEMA IF NOT EXISTS news_schema;
USE SCHEMA news_schema;


CREATE OR REPLACE STORAGE INTEGRATION s3_news_data_stage
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::136132056428:role/snowflake_s3_access_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://kafkaaabucket/kafka_folder/');


CREATE OR REPLACE FILE FORMAT news_json_format
  TYPE = 'JSON';


CREATE OR REPLACE STAGE news_stage
  URL = 's3://kafkaaabucket/kafka_folder/'
  STORAGE_INTEGRATION = s3_news_data_stage
  FILE_FORMAT = news_json_format;


CREATE OR REPLACE TABLE news_articles (
    source_name STRING,
    author STRING,
    title STRING,
    description STRING,
    url STRING,
    urlToImage STRING,
    publishedAt TIMESTAMP,
    sentiment_score FLOAT
);



COPY INTO news_articles
FROM (
  SELECT
    $1:source_name::STRING,
    $1:author::STRING,
    $1:title::STRING,
    $1:description::STRING,
    $1:url::STRING,
    $1:urlToImage::STRING,
    $1:publishedAt::TIMESTAMP,
    $1:sentiment_score::FLOAT
  FROM @news_stage
)
FILE_FORMAT = news_json_format
ON_ERROR = 'CONTINUE';


SELECT * FROM news_articles;



DESC TABLE news_articles;

SELECT *
FROM news_articles
WHERE author IS NOT NULL
  AND TRIM(author) != '';

 SELECT *
FROM news_articles
WHERE author IS NOT NULL
  AND TRIM(author) != ''
  AND title RLIKE '^[A-Za-z0-9 .,\'"!?()-]{10,}$';


  SELECT *
FROM news_articles
WHERE author IS NOT NULL
  AND TRIM(author) != ''
  AND author RLIKE '^[\\x00-\\x7F]{3,}$';
