import json
import os
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv


load_dotenv(".env.kafka")

producer = Producer(
    {
        "bootstrap.servers": os.environ["AIVEN_KAFKA_BOOTSTRAP_SERVER"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": os.environ["AIVEN_KAFKA_USERNAME"],
        "sasl.password": os.environ["AIVEN_KAFKA_PASSWORD"],
        "ssl.ca.location": os.environ["AIVEN_KAFKA_CA_FILE"],
        "client.id": "clinical360-bad-event-producer",
    }
)


def delivery_report(error, message):
    if error:
        print(f"Delivery failed: {error}")
        return
    print(f"Sent bad event to {message.topic()} offset={message.offset()}")


now = datetime.now(timezone.utc).isoformat()
bad_vitals = {
    "event_id": str(uuid.uuid4()),
    "patient_id": "",
    "encounter_id": f"encounter-{uuid.uuid4()}",
    "event_type": "vital_update",
    "event_timestamp": now,
    "heart_rate": 999,
    "systolic_bp": 120,
    "diastolic_bp": 80,
    "temperature": 37.0,
    "source": "dlq_test_producer",
}

bad_encounter = {
    "event_id": str(uuid.uuid4()),
    "encounter_id": "",
    "patient_id": f"patient-{uuid.uuid4()}",
    "event_type": "encounter_update",
    "event_timestamp": now,
    "encounter_type": "emergency",
    "diagnosis_code": "R07.9",
    "diagnosis_description": "Chest pain",
    "facility": "DLQ Test Hospital",
    "provider_id": "provider-dlq-test",
    "length_of_stay_hours": -5.0,
    "source": "dlq_test_producer",
}

producer.produce(
    topic="clinical-vitals",
    key=bad_vitals["event_id"],
    value=json.dumps(bad_vitals),
    callback=delivery_report,
)
producer.produce(
    topic="clinical-encounters",
    key=bad_encounter["event_id"],
    value=json.dumps(bad_encounter),
    callback=delivery_report,
)
producer.flush()

print("Published one bad vitals event and one bad encounter event.")
