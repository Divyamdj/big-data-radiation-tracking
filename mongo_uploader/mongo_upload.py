from kafka import KafkaConsumer
import json
from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    consumer = KafkaConsumer(
        "filtered-radiation-data",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="mongo-writer",
    )
    logger.info("Connected to Kafka")
except Exception as e:
    logger.error(f"Failed to connect to Kafka: {e}")
    exit(1)

try:
    mongo_client = MongoClient("mongodb://mongo:27017/", serverSelectionTimeoutMS=5000)
    db = mongo_client["radiation_db"]
    collection = db["filtered_data"]
    mongo_client.server_info()
    logger.info("Connected to MongoDB")
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {e}")
    exit(1)

for msg in consumer:
    try:
        collection.insert_one(msg.value)
        logger.info(f"Inserted: {msg.value}")
    except Exception as e:
        logger.error(f"Error inserting document: {e}")
