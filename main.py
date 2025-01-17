from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from clickhouse_driver import Client
import json
import datetime

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'broker:29092',
    'group.id': 'clickhouse_consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

schema_registry_url = 'http://schema-registry:8081'
kafka_topic = 'shop.public.users'

# ClickHouse configuration
clickhouse_config = {
    'host': 'clickhouse-server',
    'port': 9000,
    'user': 'default',
    'password': '',
    'database': 'shop'
}

# Initialize ClickHouse client
ch_client = Client(**clickhouse_config)

# Kafka Consumer
consumer = Consumer(kafka_config)


def parse_timestamp(microseconds):
    """Convert microseconds to datetime."""
    return datetime.datetime.fromtimestamp(microseconds / 1e6)


def consume_and_insert():
    try:
        consumer.subscribe([kafka_topic])
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                # Decode the Avro message
                value = json.loads(msg.value().decode('utf-8'))

                # Transform the message
                data = {
                    'user_id': value['user_id'],
                    'username': value['username'],
                    'account_type': value['account_type'],
                    'updated_at': parse_timestamp(value['updated_at']),
                    'created_at': parse_timestamp(value['created_at']),
                    'kafka_time': datetime.datetime.now(),
                    'kafka_offset': msg.offset()
                }

                # Insert into ClickHouse
                query = """
                INSERT INTO shop.users (user_id, username, account_type, updated_at, created_at, kafka_time, kafka_offset)
                VALUES
                """
                ch_client.execute(query, [
                    (
                        data['user_id'],
                        data['username'],
                        data['account_type'],
                        data['updated_at'],
                        data['created_at'],
                        data['kafka_time'],
                        data['kafka_offset']
                    )
                ])

                # Commit Kafka offset
                consumer.commit(msg)
            except SerializerError as e:
                print(f"Message deserialization error: {e}")
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()


if __name__ == '__main__':
    consume_and_insert()
