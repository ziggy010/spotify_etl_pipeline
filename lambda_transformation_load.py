import json;
import boto3;
import pandas as pd;
from datetime import datetime;
from io import StringIO;

def songs(data):
    song_list = [];
    for item in data['tracks']['items']:
        for key,value in item.items():
            if key == 'track':
                song_id = value['id'];
                song_name = value['name'];
                song_release_date = value['album']['release_date'];
                song_url = value['external_urls']['spotify'];
                song_duration = value['duration_ms'];
                song_popularity = value['popularity'];
                song_element = {
                    'song_id' : song_id,
                    'song_name' : song_name,
                    'song_release_date' : song_release_date,
                    'song_url' : song_url,
                    'song_duration' : song_duration,
                    'song_popularity' : song_popularity,
                    'album_id' : value['album']['id'],
                }
                song_list.append(song_element);
    return song_list;  

def artist(data):
    artist_list = [];

    for row in data['tracks']['items']:
        for key,value in row.items():
            if key == 'track':
                for item in value['artists']:
                    artist_id = item['id'];
                    artist_name = item['name'];
                    artist_uri = item['uri'];
                    artist_href = item['href'];
                    artist_element = {
                        'artist_id' : artist_id,
                        'artist_name' : artist_name,
                        'artist_uri' : artist_uri,
                        'artist_href' : artist_href
                    }
                    artist_list.append(artist_element);
    return artist_list;

def album(data):
    album_list = [];

    for row in data['tracks']['items']:
        for key,value in row.items():
            if key == 'track':
                album_element = {
                    'album_id' : value['album']['id'],
                    'album_name' : value['album']['name'],
                    'album_release_date' : value['album']['release_date'],
                    'album_total_tracks' : value['album']['total_tracks'],
                    'album_url' : value['album']['href']
                }
                album_list.append(album_element);
    return album_list;
    

def lambda_handler(event, context):
    s3 = boto3.client('s3');
    bucket = 'snowflake-spotify-etl-ziggy';
    key = 'raw_data/to_processed/';

    spotify_data = [];
    spotify_key = [];
    for file in s3.list_objects(Bucket=bucket, Prefix=key)['Contents']:
        file_key = file['Key'];
        print(file_key);
        if file_key.endswith('.json'):
            obj = s3.get_object(Bucket=bucket, Key=file_key);
            data = json.loads(obj['Body'].read().decode('utf-8'));
            spotify_data.append(data);
            spotify_key.append(file_key);
    
    for data in spotify_data:
        song_list = songs(data);
        artist_list = artist(data);
        album_list = album(data);

        song_df = pd.DataFrame.from_dict(song_list);
        song_df['song_release_date'] = pd.to_datetime(song_df['song_release_date']);

        artist_df = pd.DataFrame.from_dict(artist_list);
        artist_df = artist_df.drop_duplicates(subset=['artist_id']);

        album_df = pd.DataFrame.from_dict(album_list);
        album_df = album_df.drop_duplicates(subset=['album_id']);
        album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date']);

        song_key = 'transformed_data/song_data/song_transformed_' + str(datetime.now()) + '.csv';
        artist_key = 'transformed_data/artist_data/artist_transformed_' + str(datetime.now()) + '.csv';
        album_key = 'transformed_data/album_data/album_transformed_' + str(datetime.now()) + '.csv';

        song_buffer = StringIO();
        song_df.to_csv(song_buffer, index=False);
        song_content = song_buffer.getvalue();
        s3.put_object(Bucket=bucket, Key=song_key, Body=song_content);

        artist_buffer = StringIO();
        artist_df.to_csv(artist_buffer, index=False);
        artist_content = artist_buffer.getvalue();
        s3.put_object(Bucket=bucket, Key=artist_key, Body=artist_content);

        album_buffer = StringIO();
        album_df.to_csv(album_buffer, index=False);
        album_content = album_buffer.getvalue();
        s3.put_object(Bucket=bucket, Key=album_key, Body=album_content);

    s3_resource = boto3.resource('s3');
    for key in spotify_key:
        copy_source = {
            'Bucket' : bucket,
            'Key' : key
        }
        s3_resource.meta.client.copy(copy_source, bucket, 'raw_data/processed/' + key.split('/')[-1]);
        s3_resource.Object(bucket, key).delete();





    
