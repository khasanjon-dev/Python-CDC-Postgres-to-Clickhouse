import json
import logging
from io import BytesIO

import requests
from clickhouse_driver import Client
from confluent_kafka import Consumer, KafkaException, KafkaError
from fastavro import parse_schema, schemaless_reader

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka consumer configuration
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',  # Use localhost with the mapped port
    'group.id': 'clickhouse',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

# ClickHouse client configuration
clickhouse_client = Client(host='localhost')

# Schema Registry URL
schema_registry_url = 'http://localhost:8081'  # Use localhost for Schema Registry

# Kafka topic
topic = 'pg_all.public.users'


def get_schema_from_registry(schema_registry_url, schema_id):
    url = f"{schema_registry_url}/schemas/ids/{schema_id}"
    response = requests.get(url)
    response.raise_for_status()
    schema = response.json()
    return schema['schema']


def preprocess_schema(schema_str):
    """
    Replace custom types with standard types that fastavro can understand.
    """
    schema_json = json.loads(schema_str)

    def replace_custom_types(schema):
        if isinstance(schema, dict):
            if 'connect.name' in schema and schema['connect.name'] == 'io.debezium.time.MicroTimestamp':
                schema['type'] = 'long'
                del schema['connect.name']
            for key, value in schema.items():
                replace_custom_types(value)
        elif isinstance(schema, list):
            for item in schema:
                replace_custom_types(item)

    replace_custom_types(schema_json)
    return json.dumps(schema_json)


def consume_and_insert():
    # Initialize Kafka consumer
    consumer = Consumer(kafka_conf)
    consumer.subscribe([topic])

    schema_cache = {}

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    raise KafkaException(msg.error())

            # Debugging: Print raw message
            logger.debug(f"Raw message: {msg.value()}")

            # Deserialize Avro message
            bytes_reader = BytesIO(msg.value())
            try:
                # Read the schema ID (first byte is magic byte, next 4 bytes are the schema ID)
                magic_byte = bytes_reader.read(1)
                schema_id_bytes = bytes_reader.read(4)
                schema_id = int.from_bytes(schema_id_bytes, byteorder='big')

                if schema_id not in schema_cache:
                    avro_schema = get_schema_from_registry(schema_registry_url, schema_id)
                    preprocessed_schema = preprocess_schema(avro_schema)
                    parsed_schema = parse_schema(json.loads(preprocessed_schema))
                    schema_cache[schema_id] = parsed_schema
                else:
                    parsed_schema = schema_cache[schema_id]

                # Deserialize the Avro payload using the parsed schema
                record = schemaless_reader(bytes_reader, parsed_schema)

                # Debugging: Print deserialized record
                logger.debug(f"Deserialized record: {record}")

                # Insert into ClickHouse
                clickhouse_client.execute(
                    "INSERT INTO postgres.users (id, username, email, created_at) VALUES",
                    [{
                        'id': record['id'],
                        'username': record['username'],
                        'email': record['email'],
                        'created_at': record['created_at'] // 1000000,
                    }]
                )
            except ValueError as e:
                logger.error(f"Failed to read Avro message: {e}")
                continue  # Skip this message and continue with the next one
    except KafkaException as e:
        logger.error(f"KafkaException: {e}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == '__main__':
    logger.info("Starting Kafka consumer...")
    consume_and_insert()
