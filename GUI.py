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
from scripts.indexing.elastic_WORKS import get_top_singers_by_genre, load_combined_data, \
    index_data_in_elasticsearch, verify_indexing, visualize_track_popularity_over_time
from scripts.ingestion.JDBC import run_spotify_pipeline
from scripts.ingestion.csv_converter import csv_to_json
from scripts.ingestion.lastfm_ingestion import fetch_lastfm_track_data
from scripts.ingestion.s3_ingestion import ingest_data_to_s3
from scripts.ingestion.spotify_ingestion import fetch_spotify_track_data
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
    file_path = 'combined_data.json'
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
