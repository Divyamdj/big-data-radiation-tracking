from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common import Types
import json
from datetime import datetime
import math

env = StreamExecutionEnvironment.get_execution_environment()

kafka_props = {
    "bootstrap.servers": "kafka:9092",
    "group.id": "flink-consumer-group",
    "auto.offset.reset": "earliest",
}

consumer = FlinkKafkaConsumer(
    topics="radiation-data",
    deserialization_schema=SimpleStringSchema(),
    properties=kafka_props,
)

producer = FlinkKafkaProducer(
    topic="filtered-radiation-data",
    serialization_schema=SimpleStringSchema(),
    producer_config={"bootstrap.servers": "kafka:9092"},
)

stream = env.add_source(consumer)


def filter_message(message):
    try:
        json_data = json.loads(message)

        # Check required fields exist
        required_fields = ["timestamp", "event_time", "latitude", "longitude", "cpm"]
        for field in required_fields:
            if field not in json_data or json_data[field] is None:
                print(f"Missing required field: {field}")
                return False

        # Validate latitude & longitude
        try:
            lat = float(json_data["latitude"])
            lon = float(json_data["longitude"])
            if not (-90 <= lat <= 90):
                print(f"Invalid latitude: {lat}")
                return False
            if not (-180 <= lon <= 180):
                print(f"Invalid longitude: {lon}")
                return False
        except (ValueError, TypeError):
            print(f"Invalid coordinates: lat={json_data['latitude']}, lon={json_data['longitude']}")
            return False

        # CPM must be positive (changed from <= to <)
        try:
            cpm = float(json_data["cpm"])
            if cpm < 0:  # Changed from <= to < (allow 0 values)
                print(f"Invalid CPM: {cpm}")
                return False
        except (ValueError, TypeError):
            print(f"Invalid CPM value: {json_data['cpm']}")
            return False

        # Validate timestamps parse
        try:
            timestamp = datetime.fromisoformat(json_data["timestamp"])
            event_time = datetime.fromisoformat(json_data["event_time"])
        except ValueError as e:
            print(f"Invalid timestamp format: {e}")
            return False

        return True

    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        return False
    except Exception as e:
        print(f"Filter error: {e}")
        return False


def update_message(message):
    json_data = json.loads(message)

    # Calculate microSv/h
    micro_sv_h = round(json_data["cpm"] * 0.0029, 4)
    json_data["micro_sv_h"] = micro_sv_h

    return json.dumps(json_data)


filtered_stream = stream.filter(filter_message).map(
    update_message, output_type=Types.STRING()
)

filtered_stream.add_sink(producer)
filtered_stream.print()

env.execute("Radiation Data Processing")
