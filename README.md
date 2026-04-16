# BKK Real-Time Data Pipeline (Bronze Layer)

A robust, Dockerized data ingestion pipeline for collecting real-time public transit data from the Budapest Transport Center (BKK).

This project acts as the **Bronze Layer** (raw data ingestion) for a Medallion architecture, pulling Protocol Buffer (Protobuf) GTFS-Realtime data, buffering it through Apache Kafka, and storing the raw JSON payloads into a PostgreSQL database for downstream Machine Learning and data analytics.

## Architecture

![Architecture](https://img.shields.io/badge/Architecture-Event--Driven-blue)
![Stack](https://img.shields.io/badge/Stack-Python%20%7C%20Kafka%20%7C%20PostgreSQL%20%7C%20Docker-success)

1. **Producer (`/producer`)**: A Python scraper that fetches the `VehiclePositions.pb` GTFS-RT feed from the BKK API every 5 minutes. It decodes the binary Protobuf data into JSON and streams each individual vehicle's position to a Kafka topic.
2. **Message Broker (Apache Kafka)**: Runs in KRaft mode (no Zookeeper) to provide a fault-tolerant, high-throughput buffer between the API and the database.
3. **Consumer (`/consumer`)**: A Python loader that continuously listens to the Kafka topic and inserts the incoming vehicle data directly into the database.
4. **Database (PostgreSQL)**: Stores the unstructured payload in a `JSONB` column (`bkk_raw_payloads`), preserving the exact API schema and providing maximum flexibility for future Silver layer transformations.

## Getting Started

### Prerequisites

* Docker and Docker Compose installed.

* A BKK API Key (Request one from the BKK Open Data Portal)

### Installation & Setup

1. **Clone the repository**
   git clone <your-repo-url>
   cd bkk-data-pipeline

2. **Configure Environment Variables**
   Copy the example environment file and add your BKK API key:
   cp .env.example .env

   Edit `.env` and set your key without quotes:
   BKK\_API\_KEY=your\_actual\_api\_key\_here

3. **Launch the Pipeline**
   Spin up the entire infrastructure (Postgres, Kafka, Producer, Consumer) in detached mode:
   docker compose up --build -d

## Monitoring & Verification

Check the health of your containers:
docker compose ps

Monitor the live data flow from your terminal:

### Watch the producer decode Protobufs and send to Kafka

docker compose logs -f producer

### Watch the consumer insert rows into Postgres

docker compose logs -f consumer

Verify data is landing in the database:
docker exec -it bkk\_postgres psql -U user -d bkk\_bronze -c "SELECT COUNT(\*) FROM bkk\_raw\_payloads;"

## Extracting Data for Machine Learning

Once you have collected enough data, you can extract it into a flat CSV format for data exploration (Pandas, Jupyter) or Machine Learning model training.

Run the provided extraction script on your local machine:

### Ensure you have the required local dependencies

pip install pandas sqlalchemy psycopg2-binary

### Run the extraction

python extract\_csv.py

This script will connect to the exposed PostgreSQL port (`5432`), automatically flatten the nested `JSONB` structure, and output a `bkk_raw.csv` file, preserving all raw telemetry for your data cleaning exercises.
