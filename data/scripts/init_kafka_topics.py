import argparse

import yaml
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def load_config(path):
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def topic_specs(config):
    kafka_config = config["kafka"]
    specs = [
        (domain_config["topic"], int(domain_config.get("partitions", 1)))
        for domain_config in kafka_config["domains"].values()
    ]
    specs.append((kafka_config["dlq_topic"], 1))
    return specs


def create_topics(bootstrap_servers, specs):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="clinical-360-topic-init")
    try:
        existing = set(admin.list_topics())
        topics = [
            NewTopic(name=name, num_partitions=partitions, replication_factor=1)
            for name, partitions in specs
            if name not in existing
        ]
        if topics:
            try:
                admin.create_topics(new_topics=topics, validate_only=False)
            except TopicAlreadyExistsError:
                pass
        for name, partitions in specs:
            action = "exists" if name in existing else "created"
            print(f"[KAFKA INIT] {action:7} {name} partitions={partitions}")
    finally:
        admin.close()


def main():
    parser = argparse.ArgumentParser(description="Create Clinical 360 Kafka domain topics.")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--bootstrap-servers", default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    bootstrap_servers = args.bootstrap_servers or config["kafka"]["bootstrap_servers"]
    create_topics(bootstrap_servers, topic_specs(config))


if __name__ == "__main__":
    main()
