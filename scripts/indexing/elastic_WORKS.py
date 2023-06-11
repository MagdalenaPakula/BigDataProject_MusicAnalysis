from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np


def load_combined_data(file_path):
    # Load the combined data from JSON file
    combined_df = pd.read_json(file_path)

    # Replace 'NaN' values with np.nan
    combined_df.replace('NaN', np.nan, inplace=True)

    # Print the DataFrame to check for any remaining incompatible values
    print(combined_df)

    return combined_df


def index_data_in_elasticsearch(data_df, index_name):
    # Step 3: Index the combined data in Elasticsearch
    es = Elasticsearch('localhost:9200')

    # Convert DataFrame records to a list of dictionaries
    documents = data_df.to_dict(orient='records')

    # Index each document
    for doc in documents:
        try:
            es.index(index=index_name, document=doc)
        except Exception as e:
            print(f"Failed to index document: {doc}")
            print(f"Error: {e}")


def verify_indexing(index_name):
    # Step 4: Verify the indexing process
    es = Elasticsearch('localhost:9200')
    res = es.search(index=index_name, size=5)
    print("Indexed Documents:")
    for hit in res['hits']['hits']:
        print(hit['_source'])


def get_top_singers_by_genre(index_name, genre, top_n=10):
    # Example: Get the top 10 singers for a particular genre
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

    # Perform the aggregation query
    es = Elasticsearch('localhost:9200')
    result = es.search(index=index_name, body=query)

    # Extract the top singers from the result
    top_singers = result["aggregations"]["top_singers"]["buckets"]

    # Print the top singers
    print(f"Top {top_n} Singers for Genre: {genre}")
    for singer in top_singers:
        print(f"{singer['key']}: {singer['doc_count']}")

def visualize_track_popularity_over_time(index_name):
    # Example: Visualize the track popularity over time
    date_agg_query = {
        "size": 0,
        "query": {
            "match_all": {}  # Add any additional queries if needed
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

    # Perform the date aggregation query
    es = Elasticsearch('localhost:9200')
    date_result = es.search(index=index_name, body=date_agg_query, track_total_hits=True, size=0)

    # Extract the date histogram and average popularity from the result
    date_histogram = date_result["aggregations"]["track_popularity_over_time"]["buckets"]
    popularity_over_time = [(entry["key_as_string"], entry["doc_count"]) for entry in date_histogram]

    # Print the track popularity over time
    print("Track Popularity Over Time:")
    for entry in popularity_over_time:
        print(f"{entry[0]}: {entry[1]}")
# Example usage
file_path = 'C:/Users/aleks/OneDrive/Pulpit/BigDataProject_MusicAnalysis1/data/combined/combined_data.json'
index_name = 'music_data'

combined_data = load_combined_data(file_path)
index_data_in_elasticsearch(combined_data, index_name)
verify_indexing(index_name)

genre = "Pop"
get_top_singers_by_genre(index_name, genre, top_n=10)

visualize_track_popularity_over_time(index_name)