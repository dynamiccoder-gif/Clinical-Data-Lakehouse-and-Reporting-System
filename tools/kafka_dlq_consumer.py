import json
import os
import time

from confluent_kafka import Consumer
from dotenv import load_dotenv


load_dotenv(".env.kafka")

consumer = Consumer(
    {
        "bootstrap.servers": os.environ["AIVEN_KAFKA_BOOTSTRAP_SERVER"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": os.environ["AIVEN_KAFKA_USERNAME"],
        "sasl.password": os.environ["AIVEN_KAFKA_PASSWORD"],
        "ssl.ca.location": os.environ["AIVEN_KAFKA_CA_FILE"],
        "group.id": f"clinical360-dlq-check-{int(time.time())}",
        "auto.offset.reset": "earliest",
    }
)

consumer.subscribe(["clinical-dlq"])
received = 0

try:
    while received < 10:
        message = consumer.poll(10)
        if message is None:
            break
        if message.error():
            print(f"Consumer error: {message.error()}")
            continue
        event = json.loads(message.value().decode("utf-8"))
        print(
            f"offset={message.offset()} "
            f"domain={event.get('domain')} "
            f"source_topic={event.get('source_topic')} "
            f"error_reason={event.get('error_reason')} "
            f"kafka_offset={event.get('kafka_offset')} "
            f"raw_payload={event.get('raw_payload')}"
        )
        received += 1
finally:
    consumer.close()

print(f"Received {received} DLQ event(s).")
