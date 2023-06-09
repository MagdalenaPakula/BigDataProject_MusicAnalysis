# import json
# import pandas as pd

# # Load the JSON data
# with open('../../data/raw/shazam_track_data.json', 'r') as file:
#     data = json.load(file)

# # Extract the relevant information and convert it into a list of dictionaries
# formatted_data = []
# header = data[1]['null']
# for entry in data[2:]:
#     track_info = {
#         'Track Name': entry['null'][1],
#         'Artist Name': entry['null'][0],
#         'Rank': int(entry['\ufeffShazam Top 200 Global Chart: The most Shazamed tracks in the world'])
#     }
#     formatted_data.append(track_info)

# # Convert the list of dictionaries into a pandas DataFrame
# df = pd.DataFrame(formatted_data)

# # Save the formatted data to a new file in Feather format
# df.to_feather('formatted/shazam_formatted.feather')

import json
import pandas as pd

def format_shazam_track_data(input_json, output_feather):
    # Load the JSON data
    with open(input_json, 'r') as file:
        data = json.load(file)

    # Extract the relevant information and convert it into a list of dictionaries
    formatted_data = []
    header = data[1]['null']
    for entry in data[2:]:
        track_info = {
            'Track Name': entry['null'][1],
            'Artist Name': entry['null'][0],
            'Rank': int(entry['\ufeffShazam Top 200 Global Chart: The most Shazamed tracks in the world'])
        }
        formatted_data.append(track_info)

    # Convert the list of dictionaries into a pandas DataFrame
    df = pd.DataFrame(formatted_data)

    # Save the formatted data to a new file in Feather format
    df.to_feather(output_feather)

# Example usage
input_json = '../../data/raw/shazam_track_data.json'
output_feather = 'formatted/shazam_formatted.feather'
format_shazam_track_data(input_json, output_feather)
