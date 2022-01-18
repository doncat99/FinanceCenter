# -*- coding: utf-8 -*-
from kafka import KafkaProducer, KafkaConsumer


def connect_kafka_producer(server):
    producer_instance = None
    try:
        # host.docker.internal is how a docker container connects to the local
        # machine.
        # Don't use in production, this only works with Docker for Mac in
        # development
        producer_instance = KafkaProducer(
            bootstrap_servers=[server],  # findy_config['kafka']
            api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(ex)
    return producer_instance


def connect_kafka_consumer(topic, server):
    return KafkaConsumer(topic, bootstrap_servers=[server])


def publish_message(producer_instance, topic_name, key_bytes, value_bytes):
    try:
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        # print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(ex)
