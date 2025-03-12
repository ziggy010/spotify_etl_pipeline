import json;
import os;
import spotipy;
from spotipy.oauth2 import SpotifyClientCredentials;
import boto3;
from datetime import datetime;

def lambda_handler(event, context):
    client_id = os.environ['client_id'];
    client_secret = os.environ['client_secret'];
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret);
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager);    

    playlist_link = 'https://open.spotify.com/playlist/1SXABvBG8yUPLpEyRPwXSN?si=hdUnlmpmQyCnPSpCnAWT8Q';
    playlist_uri = playlist_link.split('/')[-1].split('?')[0];  

    data = sp.playlist(playlist_uri);

    client = boto3.client('s3');

    client.put_object(
        Bucket='snowflake-spotify-etl-ziggy',
        Key=f'raw_data/to_processed/spotify_raw_{str(datetime.now())}.json',
        Body=json.dumps(data)
    )
