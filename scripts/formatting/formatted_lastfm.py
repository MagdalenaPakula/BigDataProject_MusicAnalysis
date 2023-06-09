import json
import pandas as pd
import pyarrow.parquet as pq


def format_lastfm_track_data(input_json, output_json, output_parquet):
    # Read the JSON file
    with open(input_json) as json_file:
        lastfm_track_data = json.load(json_file)

    # Extract the necessary information from the JSON data
    formatted_data = []
    for track in lastfm_track_data['tracks']['track']:
        formatted_track = {
            'name': track['name'],
            'artist': track['artist']['name'],
            'image': track['image'][3]['#text'],  # Choose the 'extralarge' image size
            'duration': int(track['duration']),
            'playcount': int(track['playcount']),
            'listeners': int(track['listeners']),
            'url': track['url']
        }
        formatted_data.append(formatted_track)

    # Create a DataFrame from the formatted data
    formatted_df = pd.DataFrame(formatted_data)

    # Save the formatted data to a JSON file
    formatted_df.to_json(output_json, orient='records')

    # Read and display the content of the formatted JSON file
    formatted_data = pd.read_json(output_json)
    print("Formatted lastfm_track_data:")
    print(formatted_data)

    # Save the formatted DataFrame as a Parquet file
    formatted_df.to_parquet(output_parquet)

    # Read the Parquet file
    parquet_data = pq.read_table(output_parquet)
    parquet_df = parquet_data.to_pandas()

    # Display the content of the Parquet file
    print(parquet_df)


# Example usage
input_json = '../data/raw/lastfm_track_data.json'
output_json = 'formatted_lastfm_track_data.json'
output_parquet = 'formatted_lastfm.parquet'
format_lastfm_track_data(input_json, output_json, output_parquet)
