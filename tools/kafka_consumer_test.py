import json
import os

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
        "group.id": "clinical360-test-consumer",
        "auto.offset.reset": "earliest",
    }
)

consumer.subscribe(["clinical-vitals"])

received = 0

try:
    while received < 10:
        message = consumer.poll(10)

        if message is None:
            print("No more messages found.")
            break

        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

        event = json.loads(message.value().decode("utf-8"))

        print(
            f"partition={message.partition()} "
            f"offset={message.offset()} "
            f"patient={event.get('patient_id')} "
            f"heart_rate={event.get('heart_rate')}"
        )

        received += 1
finally:
    consumer.close()

print(f"Received {received} event(s).")
