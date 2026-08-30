import pandas as pd
import vaex
from confluent_kafka import Producer
import json
import time
import argparse
from datetime import datetime
import re

parser = argparse.ArgumentParser(description="Kafka producer for Safecast dataset")
parser.add_argument(
    "--dataset",
    type=str,
    default="./data/measurements-out.csv",
    help="Path to Safecast CSV file",
)
args = parser.parse_args()

dataset = args.dataset

# Kafka config
conf = {
    "bootstrap.servers": "kafka:9092",
    "linger.ms": 5,
    "batch.size": 1048576,
    "queue.buffering.max.messages": 1000000,
    "queue.buffering.max.kbytes": 1048576,
    "compression.type": "gzip",
    "retries": 5,
    "retry.backoff.ms": 1000,
}

producer = Producer(conf)
topic = "radiation-data"

df = vaex.from_csv_arrow(dataset, lazy=True)
print(f"Loaded dataset with {len(df)} rows")

sent = 0
number_of_chunks = 0
total_rows = len(df)

# Time range sanity check
min_valid_time = datetime(1990, 1, 1)
max_valid_time = datetime.now()

iso_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}T?\s?\d{2}:\d{2}(:\d{2}(\.\d+)?)?$")

for i in range(total_rows - 1000000, -1, -1000000):
    try:
        print("chunk size",i)
        df_chunk = df[max(0, i) : i + 1000000].to_pandas_df()
        df_chunk = df_chunk.dropna(subset=["Captured Time", "Uploaded Time"])

        # Apply regex to filter valid ISO timestamps
        df_chunk = df_chunk[
            df_chunk['Captured Time'].astype(str).apply(lambda x: bool(iso_pattern.match(x))) &
            df_chunk['Uploaded Time'].astype(str).apply(lambda x: bool(iso_pattern.match(x)))
        ]

        # Parse timestamps
        df_chunk['Captured Time'] = pd.to_datetime(df_chunk['Captured Time'], errors='coerce')
        df_chunk['Uploaded Time'] = pd.to_datetime(df_chunk['Uploaded Time'], errors='coerce')

        # Drop rows with parse errors
        df_chunk = df_chunk.dropna(subset=["Captured Time", "Uploaded Time"])

        # Time sanity
        df_chunk = df_chunk[
            (df_chunk["Uploaded Time"] >= min_valid_time)
            & (df_chunk["Uploaded Time"] <= max_valid_time)
        ]

        # CPM must be >= 0
        df_chunk = df_chunk[df_chunk["Value"] >= 0]

        # Sort by time for sanity
        df_chunk = df_chunk.sort_values("Uploaded Time")

        messages = df_chunk.apply(
            lambda row: {
                "timestamp": row["Captured Time"].isoformat(),
                "event_time": row["Uploaded Time"].isoformat(),
                "latitude": row["Latitude"],
                "longitude": row["Longitude"],
                "cpm": float(row["Value"]),
                "location_name": row["Location Name"],
            },
            axis=1,
        ).tolist()

        for message in messages:
            producer.produce(topic, value=json.dumps(message).encode("utf-8"))
            print(f"Sent: {message}")
            sent += 1
            time.sleep(0.1)

        producer.flush()
        number_of_chunks += 1

    except Exception as e:
        print(f"Error in chunk #{number_of_chunks}: {e}")

print(f"All chunks processed. Total messages sent: {sent}")
producer.flush()
