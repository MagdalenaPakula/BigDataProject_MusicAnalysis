from elasticsearch import Elasticsearch
import pyarrow.parquet as pq
import pandas as pd

# Step 1: Load the formatted data from the Parquet file
parquet_file_path = "formatted_data.parquet"
parquet_data = pq.read_table(parquet_file_path)
formatted_df = parquet_data.to_pandas()

# Create an Elasticsearch instance
es = Elasticsearch('localhost:9200')

# Convert DataFrame records to a list of dictionaries
documents = formatted_df.to_dict(orient='records')

# Index each document
for doc in documents:
    es.index(index='music_data', body=doc)

# Step 3: Verify the indexing process
res = es.search(index='music_data', size=5)
print("Indexed Documents:")
for hit in res['hits']['hits']:
    print(hit['_source'])
