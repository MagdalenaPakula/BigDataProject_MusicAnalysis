import pandas as pd

# Load the formatted data from different sources into DataFrames
billboard_df = pd.read_json('formatted_files/formatted_billboard_track_data.json')
lastfm_df = pd.read_json('formatted_files/formatted_lastfm_track_data.json')
spotify_df = pd.read_json('formatted_files/formatted_spotify_data.json')

# Perform a join operation based on common keys
combined_df = billboard_df.merge(lastfm_df, left_on=['Track Name', 'Artist Name'], right_on=['name', 'artist'], how='inner')

# Perform aggregation to generate KPIs
kpi_df = combined_df.groupby(['Track Name', 'Artist Name']).agg({'playcount': 'sum', 'listeners': 'mean'})

# Example: Perform recommendation based on aggregated data
recommendation_df = kpi_df.sort_values(by='playcount', ascending=False).head(10)

# Save the final output as a CSV file
kpi_df.to_csv('combined_track_data_kpis.csv', index=False)
recommendation_df.to_csv('recommendations.csv', index=False)
