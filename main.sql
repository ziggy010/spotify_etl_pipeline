-- Table Definitions
CREATE OR REPLACE TABLE album_table (
    album_id STRING PRIMARY KEY,
    album_name STRING,
    album_release_date DATE,
    album_total_tracks INT,
    album_url STRING
);

CREATE OR REPLACE TABLE song_table (
    song_id STRING PRIMARY KEY,
    song_name STRING,
    song_release_date DATE,
    song_url STRING,
    song_duration INT,
    song_popularity INT,
    album_id STRING,
    FOREIGN KEY (album_id) REFERENCES album_table(album_id)
);

CREATE OR REPLACE TABLE artist_table (
    artist_id STRING PRIMARY KEY,
    artist_name STRING,
    artist_uri STRING,
    artist_url STRING
);

-- Schema for External Stage
CREATE SCHEMA external_stage;

-- Storage Integration and Stage
CREATE OR REPLACE STORAGE INTEGRATION spotify_stage
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::904233088246:role/spotify-snowflake-connection'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-spotify-etl-ziggy/');

CREATE OR REPLACE STAGE snowflake_spotify_stage
    STORAGE_INTEGRATION = spotify_stage
    URL = 's3://snowflake-spotify-etl-ziggy/';

-- File Format Definition
CREATE OR REPLACE FILE FORMAT csv_file_format
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_DELIMITER = ',';