import pandas as pd
import json
import pyarrow as pa
import pyarrow.parquet as pq


# Load the track data from the JSON file
with open("spotify_track_data.json", "r") as json_file:
    spotify_track_data = json.load(json_file)

# Convert the track data to a Pandas DataFrame
df = pd.DataFrame(spotify_track_data)

# Convert release_date column to UTC format
df["Release Date"] = pd.to_datetime(df["Release Date"]).dt.tz_localize("UTC")

# Clean and normalize the columns
df["Track Popularity"] = df["Track Popularity"].astype(int)
df["Duration (sec)"] = df["Duration (sec)"].astype(float)
df["Artist Genres"] = df["Artist Genres"].apply(lambda x: ", ".join(x))

# Save the formatted data to Parquet format
df.to_parquet("formatted_data.parquet", index=False)

# Display the formatted data
track_data_flat = pd.json_normalize(spotify_track_data)
print(track_data_flat)

# print(df.head())
