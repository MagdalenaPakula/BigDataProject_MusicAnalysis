# import json

# # Load the first JSON data
# with open('../../data/formatted/json/formatted_billboard_track_data.json') as file:
#     data1 = json.load(file)

# # Load the second JSON data
# with open('../../data/formatted/json/formatted_spotify_data.json') as file:
#     data2 = json.load(file)

# # Load the third JSON data
# with open('../../data/formatted/json/formatted_lastfm_track_data.json') as file:
#     data3 = json.load(file)

# # Combine the data based on the 'Track Name' key
# combined_data = {}

# # Function to merge two dictionaries
# def merge_dicts(dict1, dict2):
#     result = dict1.copy()
#     for key, value in dict2.items():
#         if key in result and isinstance(result[key], dict) and isinstance(value, dict):
#             result[key] = merge_dicts(result[key], value)
#         else:
#             result[key] = value
#     return result

# # Iterate over the first dataset and add its values to the combined dataset
# for item in data1:
#     track_name = item['Track Name']
#     combined_data[track_name] = item

# # Iterate over the second dataset and merge its values with the combined dataset
# for item in data2:
#     track_name = item['Track Name']
#     if track_name in combined_data:
#         combined_data[track_name] = merge_dicts(combined_data[track_name], item)
#     else:
#         combined_data[track_name] = item

# # Iterate over the third dataset and merge its values with the combined dataset
# for item in data3:
#     track_name = item['name']
#     if track_name in combined_data:
#         combined_data[track_name] = merge_dicts(combined_data[track_name], item)
#     else:
#         combined_data[track_name] = item

# # Convert the combined data back to a list
# combined_data = list(combined_data.values())

# # Save the combined data as a new JSON file
# with open('../../data/combined/combined_data.json', 'w') as file:
#     json.dump(combined_data, file)

import json

def combine_data(json_files, output_file):
    combined_data = {}

    # Function for merging two dictionaries
    def merge_dicts(dict1, dict2):
        result = dict1.copy()
        for key, value in dict2.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = merge_dicts(result[key], value)
            else:
                result[key] = value
        return result

    # Iterating over the JSON files and combining the data
    for file_path in json_files:
        with open(file_path) as file:
            data = json.load(file)
            for item in data:
                track_name = item.get('Track Name')
                if track_name is not None:
                    if track_name in combined_data:
                        combined_data[track_name] = merge_dicts(combined_data[track_name], item)
                    else:
                        combined_data[track_name] = item

    # Converting the combined data back to a list
    combined_data = list(combined_data.values())

    # Saving the combined data as a new JSON file
    with open(output_file, 'w') as file:
        json.dump(combined_data, file)

# Example usage
json_files = [
    '../../data/formatted/json/formatted_billboard_track_data.json',
    '../../data/formatted/json/formatted_spotify_data.json',
    '../../data/formatted/json/formatted_lastfm_track_data.json'
]
output_file = '../../data/combined/combined_data.json'
combine_data(json_files, output_file)

