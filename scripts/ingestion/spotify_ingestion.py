# # This is a BigData Project scripts 2023
# # Authors:          Magdalena Pakula & Aleksandra Banasiak
# # Sources of data:  https://developers.soundcloud.com/docs/api/guide
# #                   https://developer.spotify.com/documentation/web-api/reference/get-track
# #                   https://rapidapi.com/songstats-app-songstats-app-default/api/songstats/

# # importing Spotify library
# import spotipy  # Spotify web API wrapper for python
# from spotipy.oauth2 import SpotifyOAuth  # auth
# from spotipy.oauth2 import SpotifyClientCredentials  # auth
# import json  # JSON handling
# import time  # pause execution of loops

# # CLIENT AUTHORISATION
# client_id = '5a5cb989d0e147419065969f51e58710'
# client_secret = 'b17f47d55ef94b3a934645f43fc1cc55'

# client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
# sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# # PLAYLIST LINK TO URI FOR EXTRACTING
# playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=787568002113444e"
# playlist_URI = playlist_link.split("/")[-1].split("?")[0]
# track_uris = [x["track"]["uri"] for x in sp.playlist_items(playlist_URI)["items"]]

# # Create a list to store the track data
# track_data = []

# for track in sp.playlist_items(playlist_URI)["items"]:
#     # URI
#     track_uri = track["track"]["uri"]

#     # Track name
#     track_name = track["track"]["name"]

#     # Main Artist
#     artist_uri = track["track"]["artists"][0]["uri"]
#     artist_info = sp.artist(artist_uri)

#     # Name, popularity, genre
#     artist_name = artist_info["name"]
#     artist_pop = artist_info["popularity"]
#     artist_genres = artist_info["genres"]

#     # Album
#     album = track["track"]["album"]["name"]

#     # Popularity of the track
#     track_pop = track["track"]["popularity"]

#     # Audio features of the track
#     audio_features = sp.audio_features(track_uri)[0]

#     # Duration of the track
#     duration_ms = track["track"]["duration_ms"]
#     duration_sec = duration_ms / 1000

#     # Release date of the track
#     release_date = track["track"]["album"]["release_date"]

#     # Create a dictionary with the track data
#     track_dict = {
#         "Track Name": track_name,
#         "Artist Name": artist_name,
#         "Artist Popularity": artist_pop,
#         "Artist Genres": artist_genres,
#         "Album": album,
#         "Track Popularity": track_pop,
#         "Release Date": release_date,
#         "Duration (sec)": duration_sec,
#         "Audio Features": audio_features
#     }

#     # Add the track data to the list
#     track_data.append(track_dict)

#     # Pause execution for a second to avoid rate limiting
#     time.sleep(1)

# # Save the track data to a JSON file
# with open("../../data/raw/spotify_track_data.json", "w") as json_file:
#     json.dump(track_data, json_file)

# print("Track data saved to spotify_track_data.json")

# FUNCTION VERSION
import spotipy
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials
import json
import time
from kafka import KafkaProducer

def ingest_data_to_kafka(data):
    # Configure Kafka producer
    bootstrap_servers = 'localhost:9092'
    topic = 'test'

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    # Ingest each track data into Kafka
    for track in data:
        producer.send(topic, value=track)
        time.sleep(1)  # Pause execution for 1 second between each ingestion

    producer.flush()
    producer.close()

def scrape_spotify_playlist():
    # CLIENT AUTHORISATION
    client_id = '5a5cb989d0e147419065969f51e58710'
    client_secret = 'b17f47d55ef94b3a934645f43fc1cc55'

    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    # PLAYLIST LINK TO URI FOR EXTRACTING
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=787568002113444e"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    track_uris = [x["track"]["uri"] for x in sp.playlist_items(playlist_URI)["items"]]

    while True:
        # Create a list to store the track data
        track_data = []

        for track in sp.playlist_items(playlist_URI)["items"]:
            # URI
            track_uri = track["track"]["uri"]

            # Track name
            track_name = track["track"]["name"]

            # Main Artist
            artist_uri = track["track"]["artists"][0]["uri"]
            artist_info = sp.artist(artist_uri)

            # Name, popularity, genre
            artist_name = artist_info["name"]
            artist_pop = artist_info["popularity"]
            artist_genres = artist_info["genres"]

            # Album
            album = track["track"]["album"]["name"]

            # Popularity of the track
            track_pop = track["track"]["popularity"]

            # Audio features of the track
            audio_features = sp.audio_features(track_uri)[0]

            # Duration of the track
            duration_ms = track["track"]["duration_ms"]
            duration_sec = duration_ms / 1000

            # Release date of the track
            release_date = track["track"]["album"]["release_date"]

            # Create a dictionary with the track data
            track_dict = {
                "Track Name": track_name,
                "Artist Name": artist_name,
                "Artist Popularity": artist_pop,
                "Artist Genres": artist_genres,
                "Album": album,
                "Track Popularity": track_pop,
                "Release Date": release_date,
                "Duration (sec)": duration_sec,
                "Audio Features": audio_features
            }

            # Add the track data to the list
            track_data.append(track_dict)

            # Pause execution for a second to avoid rate limiting
            time.sleep(1)

        # Save the track data to a JSON file
        with open('spotify_track_data.json', 'w') as file:
            json.dump(track_data, file)

        # Ingest the track data into Kafka
        ingest_data_to_kafka(track_data)

        print("Track data ingested into Kafka")
        time.sleep(300)  # Pause execution for 5 minutes before scraping again

# Example usage
scrape_spotify_playlist()
