import json
import boto3
import pandas as pd
from datetime import datetime as dt
from io import StringIO

def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_artists = row['track']['album']['artists'][0]['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        
        album_json = {'album_id':album_id, 
                      'album_name':album_name,
                      'album_artists':album_artists,
                      'album_release_date':album_release_date, 
                      'album_total_tracks':album_total_tracks}
        
        album_list.append(album_json)
    return album_list
    

def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_id = artist['id']
                    artist_name = artist['name']
                    artist_url = artist['href']
                    
                    artist_json = {'artist_id':artist_id,
                                   'artist_name':artist_name, 
                                   'artist_url':artist_url}
                    
                    artist_list.append(artist_json)
    return artist_list

def songs(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['href']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['artists'][0]['id']
        
        song_json = {'song_id':song_id, 
                     'song':song, 
                     'song_duration':song_duration, 
                     'song_url':song_url, 
                     'song_added':song_added, 
                     'album_id':album_id, 
                     'artist_id':artist_id}
        
        song_list.append(song_json)
    return song_list


def lambda_handler(event, context):
    #create s3 object to connect to Amazon S3 info
    s3 = boto3.client('s3')
    
    bucket = "spotify-etl-project-skn"
    key = "raw_data/to_process_data/"
    
    #This block iterates through our files in to_process_data and reads/stores its content in order to transform it 
    spotify_keys = []
    spotify_data = []
    for file in s3.list_objects(Bucket=bucket, Prefix=key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket = bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
    
    
    #This block takes our stored spotify_data from to_process_data and runs our df transformation helper funcs on it
    #This block then creates a csv to store our dfs in the transformed_data folders 
    
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = songs(data)
        
        album_df = pd.DataFrame(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])
        album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'])
        
        album_key = "transformed_data/album_data/album_transformed_" + str(dt.now()) + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index = False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket = bucket, Key = album_key, Body = album_content) 
        
        artist_df = pd.DataFrame(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        artist_key = "transformed_data/artist_data/artist_transformed_" + str(dt.now()) + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index = False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket = bucket, Key = artist_key, Body = artist_content) 
        
        song_df = pd.DataFrame(song_list)
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])
        
        song_key = "transformed_data/song_data/song_transformed_" + str(dt.now()) + ".csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index = False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket = bucket, Key = song_key, Body = song_content) 
        
    
    #This code moves data over to processed_data and deletes to_process_data files
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket' : bucket,
            'Key' : key
        }
        s3_resource.meta.client.copy(copy_source, bucket, 'raw_data/processed_data/' + key.split('/')[-1])
        s3_resource.Object(bucket, key).delete()

