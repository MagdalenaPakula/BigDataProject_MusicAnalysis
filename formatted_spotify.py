import pandas as pd
import json
import pyarrow.parquet as pq

# Load the track data from the JSON file
with open("spotify_track_data.json", "r") as json_file:
    spotify_track_data = json.load(json_file)

# Convert the track data to a Pandas DataFrame
df = pd.DataFrame(spotify_track_data)

# Convert release_date column to UTC format
df["Release Date"] = pd.to_datetime(df["Release Date"]).dt.tz_localize("UTC")
df["Release Date"] = df["Release Date"].dt.strftime("%Y-%m-%d %H:%M:%S")  # Convert to string format

# Clean and normalize the columns
df["Track Popularity"] = df["Track Popularity"].astype(int)
df["Duration (sec)"] = df["Duration (sec)"].astype(float)
df["Artist Genres"] = df["Artist Genres"].apply(lambda x: ", ".join(x))

# Save the formatted data to Parquet format
df.to_parquet("formatted_spotify.parquet", index=False)

# Save the formatted data to a new JSON file
formatted_data = df.to_dict(orient='records')
for record in formatted_data:
    record["Release Date"] = pd.to_datetime(record["Release Date"]).timestamp()  # Convert to timestamp
with open("formatted_spotify_data.json", "w") as json_file:
    json.dump(formatted_data, json_file, indent=4)

# Display the formatted data
track_data_flat = pd.json_normalize(spotify_track_data)
print(track_data_flat)

# Read and display the content of the Parquet file
parquet_data = pq.read_table("formatted_spotify.parquet")
parquet_df = parquet_data.to_pandas()
print(parquet_df)
