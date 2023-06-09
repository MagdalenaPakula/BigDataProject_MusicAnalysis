from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
import pandas as pd

# Load the formatted data from different sources into DataFrames
billboard_df = pd.read_json('formatted_files/formatted_billboard_track_data.json')
lastfm_df = pd.read_json('formatted_files/formatted_lastfm_track_data.json')
spotify_df = pd.read_json('formatted_files/formatted_spotify_data.json')

# Concatenate all the DataFrames vertically
combined_df = pd.concat([billboard_df, lastfm_df, spotify_df])

# Save the combined DataFrame as a JSON file
combined_df.to_json('combined_data.json')

# Display all the rows of the combined_df
print(combined_df)

# Select the relevant columns for training
selected_columns = ['Track Name', 'Artist Name', 'Artist Popularity', 'Artist Genres', 'Album', 'Track Popularity', 'Release Date', 'Duration (sec)']

# Filter the combined data to include only the selected columns
filtered_df = combined_df[selected_columns]

# Drop any rows with missing values
filtered_df = filtered_df.dropna()

# Encode categorical variables using one-hot encoding
encoded_df = pd.get_dummies(filtered_df, columns=['Artist Genres', 'Album'])

# Split the data into features (X) and target (y)
X = encoded_df.drop(['Track Name', 'Artist Name'], axis=1)
y = encoded_df['Track Name']

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Create and fit the random forest regressor model
model = RandomForestRegressor(random_state=42)
model.fit(X_train, y_train)

# Make predictions on the test set
y_pred = model.predict(X_test)

# Calculate mean absolute error
mae = mean_absolute_error(y_test, y_pred)
print(f"Mean Absolute Error: {mae}")

# Create a mapping dictionary for label encoding
label_mapping = {label: index for index, label in enumerate(y.unique())}

# Inverse transform the target labels (y_test) and predicted labels (y_pred)
actual_track_names = y_test.map(label_mapping)
predicted_track_names = y_pred.round().astype(int).map(label_mapping)

print("Actual Track Names:")
print(actual_track_names.values)

print("Predicted Track Names:")
print(predicted_track_names.values)
