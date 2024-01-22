import datetime
from dotenv import load_dotenv
import json
import os
import pandas as pd
import sqlite3
import urllib3


load_dotenv()

CALCULALTED_SONGS = 'calculated_path.sqlite3'
HOST = 'https://amp-api.music.apple.com'
COUNTRY_CODE = 'de'
HEADERS = {
    'Media-User-Token': os.environ.get('MEDIA_USER_TOKEN'),
    'Authorization': f'Bearer {os.environ.get("AUTH_TOKEN")}',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.141 Safari/537.36',
    'Accept': '*/*',
    'Origin': 'https://music.apple.com',
    'Accept-Encoding': 'gzip, deflate'
}


def get_songs():
    """
    Get the songs from the database and return them as a list of integers.
    """
    conn = sqlite3.connect(CALCULALTED_SONGS)
    df = pd.read_sql('SELECT * FROM exported_path', conn)
    return df['Track Identifier'].astype(int).tolist()
    

def get_playlist_id_and_add_songs(song_ids: list) -> str:
    """
    Create a new Apple Music playlist and add the songs to it, if possible.
    """
    def add_song_to_playlist(http: urllib3.PoolManager, playlist_id: str, song_id: int) -> urllib3.HTTPResponse:
        body = json.dumps({
            'data': [{'id': song_id, 'type': 'songs'}]
        })
        response = http.request('POST', f'{HOST}/v1/me/library/playlists/{playlist_id}/tracks', headers=HEADERS, body=body)

        return response

    http = urllib3.PoolManager()

    # Create a new playlist
    playlist_body = json.dumps({
        'attributes': {
            'name': f'Sorted Songs ({datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")})'
        }
    })
    playlist_response = http.request('POST', f'{HOST}/v1/me/library/playlists', headers=HEADERS, body=playlist_body)
    playlist_id = json.loads(playlist_response.data)['data'][0]['id']
    print(f'Playlist created with ID: {playlist_id}')

    # Add songs to the playlist
    for song_id in song_ids:
        print(f'Adding song {song_id} to playlist {playlist_id}...')
        response = add_song_to_playlist(http, playlist_id, song_id)
        if response.status == 500:
            # If the song is not available in the selected catalog, try to find an equivalent song
            print(f'Error {response.status} - Fetching equivalent song...')
            equiv_response = http.request('GET', f'{HOST}/v1/catalog/{COUNTRY_CODE}/songs?filter[equivalents]={song_id}', headers=HEADERS)
            if equiv_response.status == 200:
                try:
                    equivalent_song_id = json.loads(equiv_response.data)['data'][0]['id']
                except (KeyError, IndexError):
                    print(f'IMPORTANT: Error - No equivalent song found for {song_id}')
                else:
                    print(f'Adding equivalent song id {equivalent_song_id} for {song_id} to the playlist...')
                    response = add_song_to_playlist(http, playlist_id, equivalent_song_id)
            if response.status not in (200, 204):
                print(f'ERROR {response.status} - {response.data.decode("utf-8")}')
        elif response.status not in (200, 204):
            print(f'ERROR {response.status} - {response.data.decode("utf-8")}')

    return playlist_id


if __name__ == '__main__':
    song_ids = get_songs()
    playlist_id = get_playlist_id_and_add_songs(song_ids)
    print(f'Songs successfully added to playlist "{playlist_id}".')
