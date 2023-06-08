from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, mean
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS

# Create a SparkSession
spark = SparkSession.builder.appName("MusicDataAnalysis").getOrCreate()

# Load the formatted data from different sources into DataFrames
billboard_df = spark.read.json('formatted_billboard_track_data.json')
lastfm_df = spark.read.json('formatted_lastfm_track_data.json')
spotify_df = spark.read.json('formatted_spotify_data.json')

# Perform a join operation based on common keys
combined_df = billboard_df.join(lastfm_df, (billboard_df['Track Name'] == lastfm_df['name']) & (billboard_df['Artist Name'] == lastfm_df['artist']), 'inner')

# Perform aggregation to generate KPIs
kpi_df = combined_df.groupby('Track Name', 'Artist Name').agg(sum('playcount').alias('playcount'), mean('listeners').alias('listeners'))

# Example: Perform recommendation based on aggregated data
indexer = StringIndexer(inputCols=['Track Name', 'Artist Name'], outputCols=['track_index', 'artist_index'])
indexed_df = indexer.fit(kpi_df).transform(kpi_df)
als = ALS(maxIter=10, regParam=0.01, userCol='track_index', itemCol='artist_index', ratingCol='playcount')
model = als.fit(indexed_df)
recommendation_df = model.recommendForAllUsers(10)

# Save the final output as a CSV file
kpi_df.write.csv('combined_track_data_kpis.csv', header=True, mode='overwrite')
recommendation_df.write.csv('recommendations.csv', header=True, mode='overwrite')

# Stop the SparkSession
spark.stop()
