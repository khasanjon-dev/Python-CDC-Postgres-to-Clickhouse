from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# Set up schema registry client
schema_registry_conf = {
    'url': 'http://localhost:8081'
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Configure the Kafka consumer
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka server
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest',
}

# Create the Consumer instance
consumer = Consumer(consumer_conf)

# Set up the AvroDeserializer with the schema registry client
avro_deserializer = AvroDeserializer(schema_registry_client)

# Subscribe to the topic
consumer.subscribe(['pg.public.users'])

while True:
    try:
        msg = consumer.poll(10)
        if msg is None:
            continue  # Skip to the next iteration if no message is returned

        # Log the raw message for debugging
        print(f"Raw Kafka message: {msg.value()}")

        # Check if msg.value() is None before proceeding
        if msg.value() is None:
            print("Received an empty message. Skipping.")
            continue

        # Ensure the message has no error and proceed with deserialization
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())

        # Deserialize the Avro message
        decoded_message = avro_deserializer(msg.value(), None)
        print(f"Decoded message: {decoded_message}")

    except KeyboardInterrupt:
        break
    except Exception as e:
        print(f"Error: {e}")

# Close the consumer when done
consumer.close()
