import os
import time
import json
import requests
import schedule
from kafka import KafkaProducer
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
BKK_API_KEY = os.getenv("BKK_API_KEY")
TOPIC = "bkk_transport_positions"

BKK_URL = f"https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/VehiclePositions.pb?key={BKK_API_KEY}"

def get_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Kafka!")
            return producer
        except Exception as e:
            print(f"Waiting for Kafka... {e}")
            time.sleep(5)

producer = get_producer()

def fetch_and_publish():
    print("Fetching BKK GTFS-Realtime data...")
    try:
        response = requests.get(BKK_URL)
        response.raise_for_status()
        
        # 1. Initialize an empty GTFS Realtime Feed object
        feed = gtfs_realtime_pb2.FeedMessage()
        
        # 2. Parse the binary content from the BKK API into the object
        feed.ParseFromString(response.content)
        
        # 3. Convert the Protobuf object into a standard Python Dictionary
        data_dict = MessageToDict(feed)
        
        # 4. Extract the list of vehicles
        entities = data_dict.get('entity', [])
        
        if not entities:
            print("No entities found in this pull.")
            return

        # 5. Stream each vehicle as an individual Kafka message
        for entity in entities:
            producer.send(TOPIC, entity)
            
        # Ensure all messages are sent
        producer.flush()
        print(f"Successfully pushed {len(entities)} vehicle positions to Kafka at {time.strftime('%X')}")
        
    except Exception as e:
        print(f"Error fetching/publishing data: {e}")

# Run immediately, then schedule every 5 minutes
fetch_and_publish()
schedule.every(5).minutes.do(fetch_and_publish)

if __name__ == "__main__":
    print("Starting BKK GTFS-RT Producer...")
    while True:
        schedule.run_pending()
        time.sleep(1)