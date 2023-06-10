import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, mean
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS

def combine_spark():
    # Load the combined JSON data
    with open('../../data/combined/combined_data.json') as file:
        combined_data = json.load(file)

    # Create a SparkSession
    spark = SparkSession.builder.appName("MusicDataAnalysis").getOrCreate()

    # Create a DataFrame from the combined JSON data
    combined_df = spark.createDataFrame(combined_data)

    # Perform aggregation to generate KPIs
    kpi_df = combined_df.groupby('Track Name', 'Artist Name').agg(sum('playcount').alias('playcount'), mean('listeners').alias('listeners'))

    # Perform recommendation based on aggregated data
    indexer = StringIndexer(inputCols=['Track Name', 'Artist Name'], outputCols=['track_index', 'artist_index'])
    indexed_df = indexer.fit(kpi_df).transform(kpi_df)
    als = ALS(maxIter=10, regParam=0.01, userCol='track_index', itemCol='artist_index', ratingCol='playcount')
    model = als.fit(indexed_df)
    recommendation_df = model.recommendForAllUsers(10)

    # Save the KPI DataFrame as a CSV file
    kpi_df.write.csv('/C:/Users/aleks/OneDrive/Pulpit/BigDataProject_MusicAnalysis1/combined_track_data_kpis.csv', header=True, mode='overwrite')

    # Save the recommendation DataFrame as a CSV file
    recommendation_df.write.csv('/C:/Users/aleks/OneDrive/Pulpit/BigDataProject_MusicAnalysis1/recommendations.csv', header=True, mode='overwrite')

    # Stop the SparkSession
    spark.stop()

# Call the function to execute the music data analysis
combine_spark()
