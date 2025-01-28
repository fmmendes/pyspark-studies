import os

from kafka import KafkaProducer
from faker import Faker
from datetime import datetime

import json

EPOCH_BASE = datetime(1970, 1, 1)
START = (datetime.now() - EPOCH_BASE).total_seconds()

# Initialize the Faker library
fake = Faker()

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers=os.environ["BOOTSTRAP_SERVERS"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define the topic
topic = os.environ["TOPIC"]

order_1 = {
    "order_id": 1,
    "customer_id": 1,
    "product_id": 1,
    "quantity": 1,
    "price": 100
}
order_2 = {
    "order_id": 2,
    "customer_id": 2,
    "product_id": 2,
    "quantity": 2,
    "price": 100
}
order_3 = {
    "order_id": 3,
    "customer_id": 3,
    "product_id": 3,
    "quantity": 3,
    "price": 300
}

# Generate mock orders insert entries
for order in [order_1, order_2, order_3]:
    c_entry = {
        "before": None,
        "after": order,
        "source": {
            "version": "1.8.1.Final",
            "connector": "mysql",
            "name": "fake-db-server1",
            "ts_ms": START,
            "snapshot": "false",
            "db": "inventory",
            "sequence": None,
            "table": "orders",
            "server_id": 223344,
            "gtid": None,
            "file": "mysql-bin.000003",
            "pos": fake.random_int(min=100, max=2000),
            "row": 0,
            "thread": None,
            "query": None
        },
        "op": "c",
        "ts_ms": (datetime.now() - EPOCH_BASE).total_seconds(),
        "transaction": None
    }
    producer.send(topic, key=str(json.dumps({"order_id": order["order_id"],"customer_id": order["customer_id"]})).encode('utf-8'), value=c_entry)

# Generate mock update entry
u_entry = {
        "before": order_2,
        "after": order_2,
        "source": {
            "version": "1.8.1.Final",
            "connector": "mysql",
            "name": "fake-db-server1",
            "ts_ms": START+1000,
            "snapshot": "false",
            "db": "inventory",
            "sequence": None,
            "table": "orders",
            "server_id": 223344,
            "gtid": None,
            "file": "mysql-bin.000003",
            "pos": fake.random_int(min=100, max=2000),
            "row": 0,
            "thread": None,
            "query": None
        },
        "op": "u",
        "ts_ms": (datetime.now() - EPOCH_BASE).total_seconds()+1000,
        "transaction": None
    }
u_entry["after"]["quantity"] = 200

producer.send(topic, key=str(json.dumps({"order_id": order_2["order_id"],"customer_id": order_2["customer_id"]})).encode('utf-8'), value=u_entry)

# Generate mock delete entry
d_entry = {
        "before": order_3,
        "after": None,
        "source": {
            "version": "1.8.1.Final",
            "connector": "mysql",
            "name": "fake-db-server1",
            "ts_ms": START+2000,
            "snapshot": "false",
            "db": "inventory",
            "sequence": None,
            "table": "orders",
            "server_id": 223344,
            "gtid": None,
            "file": "mysql-bin.000003",
            "pos": fake.random_int(min=100, max=2000),
            "row": 0,
            "thread": None,
            "query": None
        },
        "op": "d",
        "ts_ms": (datetime.now() - EPOCH_BASE).total_seconds()+2000,
        "transaction": None
    }

producer.send(topic, key=str(json.dumps({"order_id": order_3["order_id"],"customer_id": order_3["customer_id"]})).encode('utf-8'), value=d_entry)


# Ensure all messages are sent
producer.flush()

# Close the producer
producer.close()