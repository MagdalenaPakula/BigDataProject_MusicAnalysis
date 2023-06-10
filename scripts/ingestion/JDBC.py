import jaydebeapi
import mysql.connector
import spotipy
import time
import json
from spotipy.oauth2 import SpotifyClientCredentials

def run_spotify_pipeline():
    # Connect to MySQL database
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        passwd='root',
        database='bigdata_database'
    )
    cursor = mydb.cursor()

    # Spotify client authorization
    client_id = '5a5cb989d0e147419065969f51e58710'
    client_secret = 'b17f47d55ef94b3a934645f43fc1cc55'

    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    # Playlist link to URI for extracting
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=787568002113444e"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    track_uris = [x["track"]["uri"] for x in sp.playlist_items(playlist_URI)["items"]]

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

        # Dictionary with the data
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

        # Adding data to the list
        track_data.append(track_dict)

        # Pausing execution for a second to avoid rate limiting
        time.sleep(1)

    # Save the track data to a JSON file
    with open("../../data/raw/spotify_track_data.json", "w") as json_file:
        json.dump(track_data, json_file)

    # Ingest the Spotify data into the MySQL database
    table_name = 'spotify_track_data'

    # Create the table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS {} (
      `id` INT AUTO_INCREMENT PRIMARY KEY,
      `Track Name` VARCHAR(255),
      `Artist Name` VARCHAR(255),
      `Artist Popularity` INT,
      `Artist Genres` VARCHAR(255),
      `Album` VARCHAR(255),
      `Track Popularity` INT,
      `Release Date` DATE,
      `Duration (sec)` FLOAT,
      `Audio Features` JSON
    );
    """.format(table_name)

    cursor.execute(create_table_query)

    # Prepare the INSERT query
    insert_query = """
    INSERT INTO {} (`Track Name`, `Artist Name`, `Artist Popularity`, `Artist Genres`, `Album`, `Track Popularity`, `Release Date`, `Duration (sec)`, `Audio Features`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """.format(table_name)

    # Insert each track data into the table
    for track in track_data:
        values = (
            track["Track Name"],
            track["Artist Name"],
            track["Artist Popularity"],
            json.dumps(track["Artist Genres"]),
            track["Album"],
            track["Track Popularity"],
            track["Release Date"],
            track["Duration (sec)"],
            json.dumps(track["Audio Features"])
        )
        cursor.execute(insert_query, values)

    # Commit the changes
    mydb.commit()

    print("Spotify data has been ingested into the MySQL database.")

    # Close the cursor and database connection
    cursor.close()
    mydb.close()

run_spotify_pipeline()