import binascii

from confluent_kafka import TopicPartition, DeserializingConsumer

topic = "pg.public.users"
start_offset = 0
finish_offset = 5
for i in range(start_offset, finish_offset):
    conf = {'bootstrap.servers': 'localhost:9092',
            'group.id': "my-group",
            'auto.offset.reset': 'earliest',
            'isolation.level': 'read_committed',
            'key.deserializer': lambda v, blabla: binascii.unhexlify(v).decode('utf-8'),
            # 'value.deserializer':lambda v,blabla:json.loads(v.decode('utf-8'))
            }
consumer = DeserializingConsumer(conf)

offset = int(i)
partition = 0
partition = TopicPartition(topic, partition, offset)
consumer.assign([partition])
consumer.seek(partition)
# Read the message
message = consumer.poll()
print("Output: " + str(message.partition()) + " " + str(message.offset()), message.value())
