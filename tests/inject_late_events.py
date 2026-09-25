import argparse
import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    sensor = f"test-retards-{args.run_id}"

    producer = KafkaProducer(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP", "service-kafka:9092"
        ).split(","),
        acks="all",
        enable_idempotence=True,
    )

    # Même instant de référence pour les quatre événements.
    reference_ms = time.time_ns() // 1_000_000
    cases = [
        (5, 21.0),
        (45, 22.0),
        (25, 23.0),
        (180, 99.0),
    ]

    print(f"Capteur de test : {sensor}", flush=True)

    try:
        for delay_seconds, value in cases:
            timestamp = reference_ms - delay_seconds * 1000

            message = {
                "capteur_id": sensor,
                "type": "temperature",
                "valeur": value,
                "event_time": timestamp,
            }

            metadata = producer.send(
                "iot.mesures",
                key=sensor.encode("utf-8"),
                value=json.dumps(message).encode("utf-8"),
            ).get(timeout=30)

            event_date = datetime.fromtimestamp(
                timestamp / 1000, tz=timezone.utc
            ).isoformat()

            print(
                f"retard={delay_seconds}s "
                f"valeur={value} "
                f"event_time={event_date} "
                f"partition={metadata.partition} "
                f"offset={metadata.offset}",
                flush=True,
            )
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()