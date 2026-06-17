"""Legacy command wrapper for the validated multi-domain Kafka publisher."""

import sys

from publish_clinical_topics_to_kafka import main


if __name__ == "__main__":
    print("[DEPRECATED] Use scripts/publish_clinical_topics_to_kafka.py --domain encounters")
    if "--domain" not in sys.argv:
        sys.argv.extend(["--domain", "encounters"])
    main()
