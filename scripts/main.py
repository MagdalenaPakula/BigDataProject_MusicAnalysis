import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox

from scripts.combining.combining_data_spark import combine_spark
from scripts.combining.combining_to_json import combine_data_json
from scripts.combining.combining_to_parquet import combine_data_parquet
from scripts.combining.machine_learning import train_and_predict_weeks_on_chart
from scripts.formatting.formatted_billboard import format_billboard_track_data
from scripts.formatting.formatted_lastfm import format_lastfm_track_data
from scripts.formatting.formatted_spotify import format_spotify_track_data
from scripts.indexing.elastic_WORKS import get_top_singers_by_genre, date_result, index_data_in_elasticsearch, load_combined_data, verify_indexing, visualize_track_popularity_over_time
from scripts.ingestion.JDBC import run_spotify_pipeline
from scripts.ingestion.csv_converter import csv_to_json
from scripts.ingestion.lastfm_ingestion import fetch_lastfm_track_data
from scripts.ingestion.s3_ingestion import ingest_data_to_s3
from scripts.ingestion.spotify_ingestion import fetch_spotify_track_data

# 1. INGESTION
# 1.1 Spotify + Kafka
fetch_spotify_track_data()
# 1.2 S3 Amazon data ingestion
ingest_data_to_s3()
# 1.3 Last.fm
fetch_lastfm_track_data()
# 1.4 JDBC
run_spotify_pipeline()

# 2. FORMATTING
# 2.1 Billboard
input_json = "../data/raw/billboard_track_data.json"
output_json = "formatted_billboard_track_data.json"
output_parquet = "formatted_billboard_track_data.parquet"
format_billboard_track_data(input_json, output_json, output_parquet)
# 2.2 Last FM
input_json = '../data/raw/lastfm_track_data.json'
output_json = 'formatted_lastfm_track_data.json'
output_parquet = 'formatted_lastfm.parquet'
format_lastfm_track_data(input_json, output_json, output_parquet)
# 2.3 Spotify
input_json = '../data/raw/spotify_track_data.json'
output_json = 'formatted_spotify_data.json'
output_parquet = 'formatted_spotify.parquet'
format_spotify_track_data(input_json, output_json, output_parquet)

# 3. COMBINING
# 3.1 JSON
json_files = [
    '../../data/formatted/json/formatted_billboard_track_data.json',
    '../../data/formatted/json/formatted_spotify_data.json',
    '../../data/formatted/json/formatted_lastfm_track_data.json'
]
output_file = '../../data/combined/combined_data.json'
combine_data_json(json_files, output_file)
# 3.2 Parquet
combine_data_parquet()
# 3.3 Spark
combine_spark()
# 3.4 Machine learning
train_and_predict_weeks_on_chart('../data/combined/combined_data.json')

# 3. INDEXING
# Example: Get the top 10 singers for the "Pop" genre
get_top_singers_by_genre("Pop", top_n=10)
# Extract the date histogram and average popularity from the result
date_histogram = date_result["aggregations"]["track_popularity_over_time"]["buckets"]
popularity_over_time = [(entry["key_as_string"], entry["doc_count"]) for entry in date_histogram]
# Print the track popularity over time
print("Track Popularity Over Time:")
for entry in popularity_over_time:
    print(f"{entry[0]}: {entry[1]}")

# 4. ADDITIONAL
# CSV to JSON converter
csv_file = '../../data/raw/Shazam_Top_200_Global_Chart.csv'
json_file = '../../data/raw/shazam_track_data.json'
csv_to_json(csv_file, json_file)

# GUI
def load_parquet_file():
    file_path = filedialog.askopenfilename(filetypes=[("Parquet Files", "*.parquet")])
    parquet_entry.delete(0, tk.END)
    parquet_entry.insert(tk.END, file_path)

def ingest_data():
    parquet_file_path = parquet_entry.get()
    # Add your code to ingest the data here

def transform_data():
    # Add your code to transform the data here
    pass

def combine_data():
    # Add your code to combine the data here
    pass

def index_data():
    elasticsearch_host = elasticsearch_entry.get()
    index_name = index_entry.get()
    # Add your code to index the data in Elasticsearch here

def execute_data_pipeline():
    try:
        ingest_data()
        transform_data()
        combine_data()
        index_data()
        messagebox.showinfo("Success", "Data pipeline executed successfully.")
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {str(e)}")
def elastic():
    file_path = '../../data/combined/combined_data.json'
    index_name = 'music_data'

    # Load combined data
    combined_data = load_combined_data(file_path)

    # Index data in Elasticsearch
    index_data_in_elasticsearch(combined_data, index_name)

    # Verify indexing
    verify_indexing(index_name)

    # Get top singers by genre
    genre = "Pop"
    get_top_singers_by_genre(index_name, genre, top_n=10)

    # Visualize track popularity over time
    visualize_track_popularity_over_time(index_name)

        

# Create the main window
window = tk.Tk()
window.title("Music Data Analysis")
window.geometry("400x300")

parquet_label = tk.Label(window, text="Parquet File:")
parquet_label.pack()
parquet_entry = tk.Entry(window)
parquet_entry.pack()

parquet_button = tk.Button(window, text="Browse", command=load_parquet_file)
parquet_button.pack()

elasticsearch_label = tk.Label(window, text="Elasticsearch Host:")
elasticsearch_label.pack()
elasticsearch_entry = tk.Entry(window)
elasticsearch_entry.pack()

index_label = tk.Label(window, text="Index Name:")
index_label.pack()
index_entry = tk.Entry(window)
index_entry.pack()

run_button = tk.Button(window, text="Run", command=execute_data_pipeline)
run_button.pack()

window.mainloop()
