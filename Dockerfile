FROM debezium/connect:2.7.3.Final


# Create a directory inside the container
RUN mkdir -p /kafka/connect/debezium-avro-converter

# Copy files from the host to the container
COPY ./debezium-avro-converter /kafka/connect/debezium-avro-converter
