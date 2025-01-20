import io
import json

import avro.schema
from avro.io import DatumReader, BinaryDecoder
from kafka import KafkaConsumer

# Load the Avro schema
schema = avro.schema.parse(open("./schema.avsc", "rb").read())

# Create a DatumReader
reader = DatumReader(schema)


def decode(msg_value):
    message_bytes = io.BytesIO(msg_value)
    message_bytes.seek(7)
    decoder = BinaryDecoder(message_bytes)
    event_dict = reader.read(decoder)
    return event_dict


# configure the Kafka consumer
consumer = KafkaConsumer("postgres.public.ingredients",
                         bootstrap_servers=["localhost:29092"])

print(consumer.bootstrap_connected())
print(consumer.subscription())

# Continuously poll for new messages
print("Listening..")
while True:
    count = 0
    for msg in consumer:
        data = decode(msg.value)
        print(data)

        print("Saving it to s3 bucket..")
        # Save the dictionary to a JSON file
        json_file = 'data.json'
        with open(json_file, 'w') as f:
            json.dump(data, f)

        # Upload the JSON file to S3
        count += 1
