import csv
import json

def csv_to_json(csv_file, json_file):
    data = []
    with open(csv_file, 'r', encoding='utf-8') as file:
        csv_data = csv.DictReader(file)
        for row in csv_data:
            data.append(row)

    with open(json_file, 'w') as file:
        json.dump(data, file, indent=4)


