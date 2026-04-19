import os
import json
import time
import psycopg2
from psycopg2.extras import Json
from kafka import KafkaConsumer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
POSTGRES_URL = os.getenv("POSTGRES_URL")

TOPIC_POSITIONS = "bkk_transport_positions"
TOPIC_TRIP_UPDATES = "bkk_trip_updates"

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

    # Ensure both tables exist before we start consuming
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bkk_raw_payloads (
            id SERIAL PRIMARY KEY,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            raw_data JSONB
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bkk_trip_updates (
            id SERIAL PRIMARY KEY,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            raw_data JSONB
        );
    """)
    conn.commit()

    while True:
        try:
            # Subscribe to BOTH topics
            consumer = KafkaConsumer(
                TOPIC_POSITIONS, TOPIC_TRIP_UPDATES,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='bkk_bronze_group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("Connected to Kafka. Listening for messages on both feeds...")
            
            for message in consumer:
                raw_payload = message.value
                topic = message.topic
                
                # Route the data to the correct table
                target_table = "bkk_raw_payloads" if topic == TOPIC_POSITIONS else "bkk_trip_updates"
                
                cursor.execute(
                    f"INSERT INTO {target_table} (raw_data) VALUES (%s)",
                    [Json(raw_payload)]
                )
                conn.commit()

        except Exception as e:
            print(f"Consumer error: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)

if __name__ == "__main__":
    print("Starting BKK Consumer...")
    start_consuming()