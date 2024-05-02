# -*- coding: utf-8 -*-
import json
import os

from kafka import KafkaProducer


class KafkaManager:
    _instance = None
    producer: KafkaProducer | None = None

    @staticmethod
    def _get_kafka_producer():
        kafka_server = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', None)
        if not kafka_server:
            return None
        return KafkaProducer(bootstrap_servers=kafka_server)

    def __new__(cls):
        if cls._instance is None:
            print('do init')
            cls._instance = super().__new__(cls)
            cls._instance.producer = cls._get_kafka_producer()
        else:
            print('already init')
        return cls._instance

    def push_data(self, key: str, data_dict: dict, topic: str):
        if not self.producer:
            print('Producer not initialized')
            return
        
        # Convert the data dictionary to JSON
        data_json = json.dumps(data_dict)
        # Push the data to the Kafka topic
        self.producer.send(
            topic, 
            key=key.encode('utf-8'), 
            value=data_json.encode('utf-8'),
        )
        # Flush the producer
        self.producer.flush()

    def close_producer(self):
        if self.producer:
            self.producer.close()
            self.producer = None

    def __del__(self):
        self.close_producer()
