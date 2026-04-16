import os
import json
import time
import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
POSTGRES_URL = os.getenv("POSTGRES_URL")
TOPIC = "bkk_transport_positions"

def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(POSTGRES_URL)
            print("Connected to PostgreSQL!")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Waiting for PostgreSQL... {e}")
            time.sleep(5)

def start_consuming():
    conn = get_db_connection()
    cursor = conn.cursor()

    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='bkk_bronze_group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("Connected to Kafka. Listening for messages...")
            
            for message in consumer:
                raw_payload = message.value
                
                # Insert payload into the bronze layer JSONB column
                cursor.execute(
                    "INSERT INTO bkk_raw_payloads (raw_data) VALUES (%s)",
                    [Json(raw_payload)]
                )
                conn.commit()
                print(f"Inserted payload into database at {time.strftime('%X')}")

        except Exception as e:
            print(f"Consumer error: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)

if __name__ == "__main__":
    print("Starting BKK Consumer...")
    start_consuming()