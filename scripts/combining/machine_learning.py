import json
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split


def train_and_predict_weeks_on_chart(data_file):
    # Loading the combined data from the JSON file
    with open(data_file) as file:
        combined_data = json.load(file)

    # Extracting features and target variables
    X = []
    y = []
    for data_point in combined_data:
        track_name = data_point.get('Track Name')
        if track_name is not None:
            artist_name = data_point.get('Artist Name')
            track_popularity = data_point.get('Track Popularity', 0)  # Handling missing data
            duration_sec = data_point.get('Duration (sec)', 0)  # Handling missing data
            weeks_on_chart = data_point.get('Weeks on Chart', 0)  # Target variable

            X.append([track_popularity, duration_sec])
            y.append(weeks_on_chart)

    # Splitting the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Creating and fitting the random forest regressor model
    model = RandomForestRegressor(random_state=42)
    model.fit(X_train, y_train)

    # Making predictions on the testing data
    y_pred = model.predict(X_test)

    # Calculating mean absolute error
    mae = mean_absolute_error(y_test, y_pred)
    print(f"Mean Absolute Error: {mae}")

    # Predicting the weeks on chart for the first 4 rows of data
    predicted_weeks_on_chart = []
    real_weeks_on_chart = []
    for data_point in combined_data[:4]:
        track_popularity = data_point.get('Track Popularity', 0)  # Handling missing data
        duration_sec = data_point.get('Duration (sec)', 0)  # Handling missing data

        prediction = model.predict([[track_popularity, duration_sec]])
        predicted_weeks_on_chart.append(prediction[0])
        real_weeks_on_chart.append(data_point.get('Weeks on Chart'))

    # Printing the predicted and real weeks on chart
    print("Predicted Weeks on Chart:")
    print(predicted_weeks_on_chart)
    print("Real Weeks on Chart:")
    print(real_weeks_on_chart)


train_and_predict_weeks_on_chart('../data/combined/combined_data.json')
