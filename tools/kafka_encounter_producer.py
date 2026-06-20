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
        "client.id": "clinical360-encounter-producer",
    }
)

ENCOUNTER_TYPES = ["emergency", "ambulatory", "inpatient", "urgentcare", "wellness"]
DIAGNOSES = [
    ("I10", "Hypertension"),
    ("E11.9", "Type 2 diabetes mellitus"),
    ("J44.9", "Chronic obstructive pulmonary disease"),
    ("I25.10", "Coronary artery disease"),
    ("R07.9", "Chest pain"),
    ("J06.9", "Acute upper respiratory infection"),
]
FACILITIES = ["North Clinic", "City Hospital", "Lakeside Medical", "Metro Urgent Care"]
PROVIDERS = ["provider-101", "provider-204", "provider-318", "provider-427"]


def delivery_report(error, message):
    if error:
        print(f"Delivery failed: {error}")
    else:
        print(
            f"Sent to {message.topic()} "
            f"partition={message.partition()} "
            f"offset={message.offset()}"
        )


def generate_encounter_event():
    patient_number = random.randint(1, 3885)
    diagnosis_code, diagnosis_description = random.choice(DIAGNOSES)

    return {
        "event_id": str(uuid.uuid4()),
        "encounter_id": str(uuid.uuid4()),
        "patient_id": f"patient-{patient_number}",
        "event_type": "encounter_update",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "encounter_type": random.choice(ENCOUNTER_TYPES),
        "diagnosis_code": diagnosis_code,
        "diagnosis_description": diagnosis_description,
        "facility": random.choice(FACILITIES),
        "provider_id": random.choice(PROVIDERS),
        "length_of_stay_hours": round(random.uniform(0.5, 96.0), 1),
        "source": "synthetic_kafka_encounter_producer",
    }


def main():
    topic = "clinical-encounters"

    for _ in range(10):
        event = generate_encounter_event()
        producer.produce(
            topic=topic,
            key=event["patient_id"],
            value=json.dumps(event),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(0.5)

    producer.flush()
    print("Finished sending 10 clinical encounter events.")


if __name__ == "__main__":
    main()
