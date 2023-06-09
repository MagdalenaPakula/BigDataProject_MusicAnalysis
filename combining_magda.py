import json

# Load the first JSON data
with open('formatted_files/formatted_billboard_track_data.json') as file:
    data1 = json.load(file)

# Load the second JSON data
with open('formatted_files/formatted_spotify_data.json') as file:
    data2 = json.load(file)

# Combine the data based on the 'Track Name' key
combined_data = {}

# Iterate over the first dataset and add its values to the combined dataset
for item in data1:
    track_name = item['Track Name']
    combined_data[track_name] = item

# Iterate over the second dataset and merge its values with the combined dataset
for item in data2:
    track_name = item['Track Name']
    if track_name in combined_data:
        combined_data[track_name].update(item)
    else:
        combined_data[track_name] = item

# Convert the combined data back to a list
combined_data = list(combined_data.values())

# Save the combined data as a new JSON file
with open('combined_data.json', 'w') as file:
    json.dump(combined_data, file)