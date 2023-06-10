import pandas as pd

def combine_data_parquet():
    # Specify the file paths for the Parquet files
    billboard_file_path = '../../data/formatted/parquet/formatted_billboard_track_data.parquet'
    lastfm_file_path = '../../data/formatted/parquet/formatted_lastfm.parquet'
    spotify_file_path = '../../data/formatted/parquet/formatted_spotify.parquet'

    # Read the Parquet files into DataFrames
    billboard_data = pd.read_parquet(billboard_file_path)
    lastfm_data = pd.read_parquet(lastfm_file_path)
    spotify_data = pd.read_parquet(spotify_file_path)

    # Merge the DataFrames based on the common key/column
    combined_data = pd.merge(billboard_data, lastfm_data, on='Track Name', how='outer')
    combined_data = pd.merge(combined_data, spotify_data, on='Track Name', how='outer')

    # Specify the output file path for the combined Parquet file
    output_file_path = '../../data/combined/combined_data.parquet'

    # Write the combined data to a new Parquet file
    combined_data.to_parquet(output_file_path, index=False)

    print("Combined Parquet file created successfully.")

# Call the function to execute the data combination and saving
combine_data_parquet()
