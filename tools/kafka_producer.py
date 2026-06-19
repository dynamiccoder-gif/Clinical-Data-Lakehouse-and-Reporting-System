import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv


load_dotenv(".env.kafka")


bootstrap_server = os.environ["AIVEN_KAFKA_BOOTSTRAP_SERVER"]
username = os.environ["AIVEN_KAFKA_USERNAME"]
password = os.environ["AIVEN_KAFKA_PASSWORD"]
ca_file = os.environ["AIVEN_KAFKA_CA_FILE"]

producer = Producer(
    {
        "bootstrap.servers": bootstrap_server,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": username,
        "sasl.password": password,
        "ssl.ca.location": ca_file,
        "client.id": "clinical360-producer",
    }
)


def delivery_report(error, message):
    if error:
        print(f"Delivery failed: {error}")
    else:
        print(
            f"Sent to {message.topic()} "
            f"partition={message.partition()} "
            f"offset={message.offset()}"
        )


def generate_vital_event():
    patient_number = random.randint(1, 3885)

    return {
        "event_id": str(uuid.uuid4()),
        "patient_id": f"patient-{patient_number}",
        "encounter_id": f"encounter-{random.randint(1, 10000)}",
        "event_type": "vital_update",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "heart_rate": random.randint(55, 130),
        "systolic_bp": random.randint(95, 180),
        "diastolic_bp": random.randint(60, 110),
        "temperature": round(random.uniform(36.0, 40.0), 1),
        "source": "synthetic_kafka_producer",
    }


def main():
    topic = "clinical-vitals"

    for _ in range(10):
        event = generate_vital_event()

        producer.produce(
            topic=topic,
            key=event["patient_id"],
            value=json.dumps(event),
            callback=delivery_report,
        )

        producer.poll(0)
        time.sleep(0.5)

    producer.flush()
    print("Finished sending 10 clinical vital events.")


if __name__ == "__main__":
    main()
