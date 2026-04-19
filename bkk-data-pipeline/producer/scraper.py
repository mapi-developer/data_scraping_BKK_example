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

TOPIC_POSITIONS = "bkk_transport_positions"
TOPIC_TRIP_UPDATES = "bkk_trip_updates"

URL_POSITIONS = f"https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/VehiclePositions.pb?key={BKK_API_KEY}"
URL_TRIP_UPDATES = f"https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/TripUpdates.pb?key={BKK_API_KEY}"

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

def fetch_feed(url, topic, feed_name):
    """Generic function to fetch, decode, and publish a GTFS-RT feed."""
    print(f"Fetching BKK {feed_name}...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        data_dict = MessageToDict(feed)
        entities = data_dict.get('entity', [])
        
        if not entities:
            print(f"No entities found for {feed_name}.")
            return

        for entity in entities:
            producer.send(topic, entity)
            
        producer.flush()
        print(f"Successfully pushed {len(entities)} {feed_name} to Kafka at {time.strftime('%X')}")
        
    except Exception as e:
        print(f"Error fetching/publishing {feed_name}: {e}")

def fetch_all():
    fetch_feed(URL_POSITIONS, TOPIC_POSITIONS, "Vehicle Positions")
    fetch_feed(URL_TRIP_UPDATES, TOPIC_TRIP_UPDATES, "Trip Updates")

# Run immediately, then schedule every 5 minutes
fetch_all()
schedule.every(5).minutes.do(fetch_all)

if __name__ == "__main__":
    print("Starting BKK Dual-Feed Producer...")
    while True:
        schedule.run_pending()
        time.sleep(1)