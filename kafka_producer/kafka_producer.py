import os

from kafka import KafkaProducer
from faker import Faker

import json

# Initialize the Faker library
fake = Faker()

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers=os.environ["BOOTSTRAP_SERVERS"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define the topic
topic = os.environ["TOPIC"]

# Generate mock orders insert entries
for _ in range(fake.random_int(min=10, max=20)):
    customer_id = fake.random_int(min=1000, max=9999)

    for _ in range(fake.random_int(min=1, max=5)):
        order_id = fake.random_int(min=1000, max=9999)

        entry = {
            "before": None,
            "after": {
                "order_id": order_id,
                "customer_id": customer_id,
                "product_id": fake.random_int(min=1000, max=9999),
                "quantity": fake.random_int(min=1, max=10),
                "price": fake.random_int(min=10, max=100)
            },
            "source": {
                "version": "1.8.1.Final",
                "connector": "mysql",
                "name": "fake-db-server1",
                "ts_ms": fake.unix_time() * 1000,
                "snapshot": "false",
                "db": "inventory",
                "sequence": None,
                "table": "customers",
                "server_id": 223344,
                "gtid": None,
                "file": "mysql-bin.000003",
                "pos": fake.random_int(min=100, max=2000),
                "row": 0,
                "thread": None,
                "query": None
            },
            "op": "c",
            "ts_ms": fake.unix_time() * 1000,
            "transaction": None
        }
        producer.send(topic, key=str(json.dumps({"order_id": order_id,"customer_id": customer_id})).encode('utf-8'), value=entry)

# Generate mock orders update entries
for _ in range(fake.random_int(min=10, max=20)):
    customer_id = fake.random_int(min=1000, max=9999)

    for _ in range(fake.random_int(min=1, max=5)):
        order_id = fake.random_int(min=1000, max=9999)
        product_id = fake.random_int(min=1000, max=9999)

        entry = {
            "before": {
                "order_id": order_id,
                "customer_id": customer_id,
                "product_id": product_id,
                "quantity": fake.random_int(min=1, max=10),
                "price": fake.random_int(min=10, max=100)
            },
            "after": {
                "order_id": order_id,
                "customer_id": customer_id,
                "product_id": product_id,
                "quantity": fake.random_int(min=1, max=10),
                "price": fake.random_int(min=10, max=100)
            },
            "source": {
                "version": "1.8.1.Final",
                "connector": "mysql",
                "name": "fake-db-server1",
                "ts_ms": fake.unix_time() * 1000,
                "snapshot": "false",
                "db": "inventory",
                "sequence": None,
                "table": "customers",
                "server_id": 223344,
                "gtid": None,
                "file": "mysql-bin.000003",
                "pos": fake.random_int(min=100, max=2000),
                "row": 0,
                "thread": None,
                "query": None
            },
            "op": "c",
            "ts_ms": fake.unix_time() * 1000,
            "transaction": None
        }
        producer.send(topic, key=str(json.dumps({"order_id": order_id,"customer_id": customer_id})).encode('utf-8'), value=entry)

# Generate mock orders delete entries
for _ in range(fake.random_int(min=10, max=20)):
    customer_id = fake.random_int(min=1000, max=9999)

    for _ in range(fake.random_int(min=1, max=5)):
        order_id = fake.random_int(min=1000, max=9999)
        entry = {
            "before": {
                "order_id": order_id,
                "customer_id": customer_id,
                "product_id": fake.random_int(min=1000, max=9999),
                "quantity": fake.random_int(min=1, max=10),
                "price": fake.random_int(min=10, max=100)
            },
            "after": None,
            "source": {
                "version": "1.8.1.Final",
                "connector": "mysql",
                "name": "fake-db-server1",
                "ts_ms": fake.unix_time() * 1000,
                "snapshot": "false",
                "db": "inventory",
                "sequence": None,
                "table": "customers",
                "server_id": 223344,
                "gtid": None,
                "file": "mysql-bin.000003",
                "pos": fake.random_int(min=100, max=2000),
                "row": 0,
                "thread": None,
                "query": None
            },
            "op": "c",
            "ts_ms": fake.unix_time() * 1000,
            "transaction": None
        }
        producer.send(topic, key=str(json.dumps({"order_id": order_id,"customer_id": customer_id})).encode('utf-8'), value=entry)

# Ensure all messages are sent
producer.flush()

# Close the producer
producer.close()