from confluent_kafka import Producer
import json

import logging
logging.basicConfig(level=logging.DEBUG)

def send_to_kafka(topic, data):
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    # Convert data to JSON
    json_data = json.dumps(data)

    # Send data to Kafka
    producer.produce(topic, key=None, value=json_data)
    producer.flush()

def main():
    topic = "my-topic"
    file_path = "raw_data/spotify_track_data.json"

    with open(file_path, 'r') as file:
        data = json.load(file)
        send_to_kafka(topic, data)

if __name__ == "__main__":
    main()
