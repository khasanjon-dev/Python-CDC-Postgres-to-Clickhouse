import io

import avro.schema
from avro.io import DatumReader, BinaryDecoder
from kafka import KafkaConsumer

# Load the schema from the file
schema = avro.schema.parse(open("schema.avsc", "rb").read())
reader = DatumReader(schema)


def decode(msg_value):
    # Decode the message using Avro schema and DatumReader
    message_bytes = io.BytesIO(msg_value)
    decoder = BinaryDecoder(message_bytes)
    event_dict = reader.read(decoder)
    return event_dict


# Create a Kafka consumer
consumer = KafkaConsumer(
    "pg.public.users",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset='earliest',
    group_id='my-consumer-group'
)

print("Connected to Kafka:", consumer.bootstrap_connected())
print("Current subscription:", consumer.subscription())

print("Listening to topic 'pg.public.users'...")
while True:
    try:
        for msg in consumer:
            # Print raw message value for debugging
            print("Raw message value:", msg.value)

            data = decode(msg.value)
            print(data)

    except KeyboardInterrupt:
        print("Consumer stopped by user.")
        break
    except Exception as e:
        print(f"Error: {e}")
        break
