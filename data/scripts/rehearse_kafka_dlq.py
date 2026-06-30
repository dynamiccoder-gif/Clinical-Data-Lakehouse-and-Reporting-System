import argparse
import json
import uuid

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from publish_clinical_topics_to_kafka import envelope, load_config, validate_row


def main():
    parser = argparse.ArgumentParser(description="Publish and verify one invalid Clinical 360 ingestion event.")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    kafka_config = config["kafka"]
    dlq_topic = kafka_config["dlq_topic"]
    run_id = f"dlq-rehearsal-{uuid.uuid4()}"
    invalid_row = {"ID": "invalid-demo-encounter", "PATIENT": "", "DATE": ""}
    error = validate_row(invalid_row, ["ID", "PATIENT", "DATE"])
    message = envelope("encounters", invalid_row, -1, run_id)
    message["error_reason"] = error
    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
    )
    metadata = producer.send(dlq_topic, key=invalid_row["ID"], value=message).get(timeout=30)
    producer.flush()
    producer.close()

    consumer = KafkaConsumer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    assignment = TopicPartition(dlq_topic, metadata.partition)
    consumer.assign([assignment])
    consumer.seek(assignment, metadata.offset)
    verified = None
    for records in consumer.poll(timeout_ms=10000).values():
        for record in records:
            if record.value.get("producer_run_id") == run_id:
                verified = record.value
                break
    consumer.close()
    if verified is None:
        raise RuntimeError("DLQ rehearsal event was not observed after publishing.")
    print(f"[KAFKA DLQ] verified topic={dlq_topic} reason={verified['error_reason']} run_id={run_id}")


if __name__ == "__main__":
    main()
