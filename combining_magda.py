import json

# Load the first JSON data
with open('formatted_billboard_track_data.json') as file:
    data1 = json.load(file)

# Load the second JSON data
with open('formatted_spotify_data.json') as file:
    data2 = json.load(file)

# Load the third JSON data
with open('formatted_lastfm_track_data.json') as file:
    data3 = json.load(file)

# Combine the data based on the 'Track Name' key
combined_data = {}

# Function to merge two dictionaries
def merge_dicts(dict1, dict2):
    result = dict1.copy()
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = value
    return result

# Iterate over the first dataset and add its values to the combined dataset
for item in data1:
    track_name = item['Track Name']
    combined_data[track_name] = item

# Iterate over the second dataset and merge its values with the combined dataset
for item in data2:
    track_name = item['Track Name']
    if track_name in combined_data:
        combined_data[track_name] = merge_dicts(combined_data[track_name], item)
    else:
        combined_data[track_name] = item

# Iterate over the third dataset and merge its values with the combined dataset
for item in data3:
    track_name = item['name']
    if track_name in combined_data:
        combined_data[track_name] = merge_dicts(combined_data[track_name], item)
    else:
        combined_data[track_name] = item

# Convert the combined data back to a list
combined_data = list(combined_data.values())

# Save the combined data as a new JSON file
with open('combined_data.json', 'w') as file:
    json.dump(combined_data, file)