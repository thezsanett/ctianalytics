# Developing

## Using Docker

Go to the `flink/flink-CTI` folder.

Run the `build.sh` script.

It builds the project and copies the JAR file to the running Flink container.

## Locally

Add the following 2 lines to kafka's docker-compose.yml, under the brokers environment variables:
```
services:
  ...
  broker:
    ...
    environment:
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      LISTENERS: PLAINTEXT://0.0.0.0:9092
```