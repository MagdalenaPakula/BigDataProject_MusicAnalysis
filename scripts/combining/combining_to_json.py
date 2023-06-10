import json


def combine_data_json(json_files, output_file):
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
combine_data_json(json_files, output_file)
