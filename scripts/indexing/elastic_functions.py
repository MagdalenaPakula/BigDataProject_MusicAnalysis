import json
import pyarrow.parquet as pq
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from elasticsearch.client import _make_path

# Create an Elasticsearch client
es = Elasticsearch()

# Enable the ALLOW_NON_NUMERIC_NUMBERS feature
es.transport.get_connection().headers["Content-Type"] = "application/json;+jsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS"

# Rest of your code...




def load_formatted_data(parquet_file_path):
    """
    Load the formatted data from the Parquet file.

    Args:
        parquet_file_path (str): Path to the Parquet file.

    Returns:
        pd.DataFrame: Formatted data as a Pandas DataFrame.
    """
    parquet_data = pq.read_table(parquet_file_path)
    formatted_df = parquet_data.to_pandas()
    return formatted_df


def combine_data(billboard_file, lastfm_file, spotify_file):
    """
    Combine data from different sources.

    Args:
        billboard_file (str): Path to the formatted Billboard track data file.
        lastfm_file (str): Path to the formatted Last.fm track data file.
        spotify_file (str): Path to the formatted Spotify data file.

    Returns:
        pd.DataFrame: Combined data as a Pandas DataFrame.
    """
    # Load the first JSON data
    with open(billboard_file) as file:
        data1 = json.load(file)

    # Load the second JSON data
    with open(spotify_file) as file:
        data2 = json.load(file)

    # Load the third JSON data
    with open(lastfm_file) as file:
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

    # Convert the combined data back to a DataFrame
    combined_df = pd.DataFrame(combined_data.values())
    combined_df.fillna(0) 

    return combined_df


def index_data_to_elasticsearch(df, index_name, es_host='localhost:9200'):
    """
    Index the combined data in Elasticsearch.

    Args:
        df (pd.DataFrame): DataFrame containing the data to be indexed.
        index_name (str): Name of the Elasticsearch index.
        es_host (str): Elasticsearch host URL.

    Returns:
        None
    """
    es = Elasticsearch(es_host)
    
    # Convert NaN values to None
    df = df.where(pd.notnull(df), None)
    
    documents = df.to_dict(orient='records')

    for doc in documents:
        es.index(index=index_name, document=doc)


def get_top_singers_by_genre(genre, top_n=10, index_name='music_data', es_host='localhost:9200'):
    """
    Get the top singers for a particular genre.

    Args:
        genre (str): Genre to filter by.
        top_n (int): Number of top singers to retrieve.
        index_name (str): Name of the Elasticsearch index.
        es_host (str): Elasticsearch host URL.

    Returns:
        None
    """
    query = {
        "size": 0,
        "query": {
            "match": {
                "Artist Genres.keyword": genre
            }
        },
        "aggs": {
            "top_singers": {
                "terms": {
                    "field": "Artist Name.keyword",
                    "size": top_n,
                    "order": {
                        "_count": "desc"
                    }
                }
            }
        }
    }

    es = Elasticsearch(es_host)
    result = es.search(index=index_name, body=query)
    top_singers = result["aggregations"]["top_singers"]["buckets"]

    print(f"Top {top_n} Singers for Genre: {genre}")
    for singer in top_singers:
        print(f"{singer['key']}: {singer['doc_count']}")


# Step 1: Load the formatted data from the Parquet file
parquet_file_path = "../../data/formatted/parquet/formatted_data.parquet"
formatted_df = load_formatted_data(parquet_file_path)

# Step 2: Combine the data from different sources
billboard_file = '../../data/formatted/json/formatted_billboard_track_data.json'
lastfm_file = '../../data/formatted/json/formatted_lastfm_track_data.json'
spotify_file = '../../data/formatted/json/formatted_spotify_data.json'
combined_df = combine_data(billboard_file, lastfm_file, spotify_file)

# Save the combined data to Parquet format
combined_df.to_parquet("combined_data.parquet", index=False)

# Step 3: Index the combined data in Elasticsearch
index_name = 'music_data'
index_data_to_elasticsearch(combined_df, index_name)

# Step 4: Verify the indexing process
es = Elasticsearch('localhost:9200')
res = es.search(index=index_name, size=5)

print("Indexed Documents:")
for hit in res['hits']['hits']:
    print(hit['_source'])

# Example: Get the top 10 singers for the "Pop" genre
get_top_singers_by_genre("Pop", top_n=10)

# Example: Visualize the track popularity over time
date_agg_query = {
    "size": 0,
    "query": {
        "match_all": {}
    },
    "aggs": {
        "track_popularity_over_time": {
            "date_histogram": {
                "field": "Release Date",
                "calendar_interval": "month"
            }
        }
    }
}

date_result = es.search(index=index_name, body=date_agg_query, track_total_hits=True, size=0)
date_histogram = date_result["aggregations"]["track_popularity_over_time"]["buckets"]
popularity_over_time = [(entry["key_as_string"], entry["doc_count"]) for entry in date_histogram]

print("Track Popularity Over Time:")
for entry in popularity_over_time:
    print(f"{entry[0]}: {entry[1]}")
