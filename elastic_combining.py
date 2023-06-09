from elasticsearch import Elasticsearch
import pyarrow.parquet as pq
import pandas as pd

# Step 1: Load the formatted data from the Parquet file
parquet_file_path = "parquet_files/formatted_data.parquet"
parquet_data = pq.read_table(parquet_file_path)
formatted_df = parquet_data.to_pandas()

# Step 2: Combine the data from different sources
billboard_df = pd.read_json('formatted_files/formatted_billboard_track_data.json')
lastfm_df = pd.read_json('formatted_files/formatted_lastfm_track_data.json')
spotify_df = pd.read_json('formatted_files/formatted_spotify_data.json')

# Perform a join operation based on common keys
combined_df = billboard_df.merge(lastfm_df, left_on=['Track Name', 'Artist Name'], right_on=['name', 'artist'], how='inner')

# Merge with Spotify data based on track and artist names
combined_df = combined_df.merge(spotify_df, left_on=['Track Name', 'Artist Name'], right_on=['Track Name', 'Artist Name'], how='inner')

# Step 3: Index the combined data in Elasticsearch
es = Elasticsearch('localhost:9200')

# Convert DataFrame records to a list of dictionaries
documents = combined_df.to_dict(orient='records')

# Index each document
for doc in documents:
    es.index(index='music_data', document=doc)

# Step 4: Verify the indexing process
res = es.search(index='music_data', size=5)
print("Indexed Documents:")
for hit in res['hits']['hits']:
    print(hit['_source'])

# Example: Get the top 10 singers for a particular genre
def get_top_singers_by_genre(genre, top_n=10):
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
    result = es.search(index='music_data', body=query)

    # Extract the top singers from the result
    top_singers = result["aggregations"]["top_singers"]["buckets"]

    # Print the top singers
    print(f"Top {top_n} Singers for Genre: {genre}")
    for singer in top_singers:
        print(f"{singer['key']}: {singer['doc_count']}")

# Example: Get the top 10 singers for the "Pop" genre
get_top_singers_by_genre("Pop", top_n=10)

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
date_result = es.search(
    index='music_data',
    body=date_agg_query,
    track_total_hits=True,
    size=0  # Set size=0 to retrieve all buckets
)

# Extract the date histogram and average popularity from the result
date_histogram = date_result["aggregations"]["track_popularity_over_time"]["buckets"]
popularity_over_time = [(entry["key_as_string"], entry["doc_count"]) for entry in date_histogram]

# Print the track popularity over time
print("Track Popularity Over Time:")
for entry in popularity_over_time:
    print(f"{entry[0]}: {entry[1]}")
