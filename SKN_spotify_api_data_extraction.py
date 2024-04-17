import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime as dt

def lambda_handler(event, context):
    # Set Env Vars
    env_client_id = os.environ.get('client_id')
    env_client_secret = os.environ.get('client_secret')
    
    #Create sp instance to pull messy json 'data'
    client_creds_manager = SpotifyClientCredentials(client_id = env_client_id,  client_secret = env_client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_creds_manager)
    playlists = sp.user_playlists('spotify')
    
    playlist_link = 'https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF'
    playlist_URI = playlist_link.split("/")[-1]
    spotify_data = sp.playlist_tracks(playlist_URI)
    
    
    client = boto3.client('s3')
    
    filename = "spotify_raw_" + str(dt.now()) + ".json"
    
    client.put_object(
        Bucket = "spotify-etl-project-skn",
        Key = "raw_data/to_process_data/" + filename,
        Body = json.dumps(spotify_data)
        )