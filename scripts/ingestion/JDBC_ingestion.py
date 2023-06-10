import jpype
import jaydebeapi
import scipy as sp


def extract_spotify_data():
    # JDBC connection details
    jdbc_driver = 'com.mysql.jdbc.Driver'
    jdbc_url = 'jdbc:mysql://localhost:3306/bigdata_project'
    jdbc_username = 'root'
    jdbc_password = 'root'

    # Playlist details
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=787568002113444e"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]

    try:
        # Initialize the JVM using jpype
        jpype.startJVM(jpype.getDefaultJVMPath())

        # Establish the database connection
        conn = jaydebeapi.connect(
            jdbc_driver,
            jdbc_url,
            {'user': jdbc_username, 'password': jdbc_password},
            jars=['path/to/mysql-connector-java.jar']
        )

        # Create a cursor
        cursor = conn.cursor()

        # Iterate over tracks and extract data
        track_data = []

        for track in sp.playlist_items(playlist_URI)["items"]:
            # Extract track data
            track_name = track["track"]["name"]
            artist_name = track["track"]["artists"][0]["name"]
            artist_popularity = track["track"]["artists"][0]["popularity"]
            artist_genres = ", ".join(track["track"]["artists"][0]["genres"])
            album = track["track"]["album"]["name"]
            track_popularity = track["track"]["popularity"]
            release_date = track["track"]["album"]["release_date"]
            duration_sec = track["track"]["duration_ms"] / 1000
            # Extract audio features as needed

            # Append track data to the list
            track_data.append((track_name, artist_name, artist_popularity, artist_genres, album,
                               track_popularity, release_date, duration_sec))

        # Define the SQL statement
        sql = "INSERT INTO track_data (track_name, artist_name, artist_popularity, artist_genres, " \
              "album, track_popularity, release_date, duration_sec) " \
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

        # Execute the SQL statement for each track data
        cursor.executemany(sql, track_data)

        # Commit the changes
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        print("Track data saved to the database.")
    except Exception as e:
        print("An error occurred:", str(e))
    finally:
        # Shut down the JVM
        jpype.shutdownJVM()

# Call the function to extract Spotify data
extract_spotify_data()