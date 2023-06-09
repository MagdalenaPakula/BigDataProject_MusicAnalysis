import json
import pandas as pd
import pyarrow.parquet as pq

# Load the billboard track data from the JSON
with open("../../data/raw/billboard_track_data.json", "r") as json_file:
    billboard_track_data = json.load(json_file)

# Extract the required fields and create a new list of dictionaries
formatted_data = []
for track in billboard_track_data["data"]:
    formatted_track = {
        "Track Name": track["name"],
        "Artist Name": track["artist"],
        "Image": track["image"],
        "Rank": track["rank"],
        "Last Week Rank": track["last_week_rank"],
        "Peak Rank": track["peak_rank"],
        "Weeks on Chart": track["weeks_on_chart"]
    }
    formatted_data.append(formatted_track)

# Create a pandas DataFrame from the formatted data
formatted_df = pd.DataFrame(formatted_data)

# Save the formatted data to a JSON file
formatted_df.to_json("formatted_billboard_track_data.json", orient="records", indent=4)

# Display the formatted data
# print(formatted_df)

# Save the formatted data to a Parquet file
formatted_df.to_parquet("formatted_billboard_track_data.parquet")

# Read the Parquet file
parquet_data = pq.read_table("formatted_billboard_track_data.parquet")
parquet_df = parquet_data.to_pandas()

# Display the content of the Parquet file
print(parquet_df)
