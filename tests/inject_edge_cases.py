import argparse
import json
import os
import time

from kafka import KafkaProducer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    sensor = f"test-{args.run_id}"
    timestamp = time.time_ns() // 1_000_000

    measure = {
        "capteur_id": sensor,
        "type": "temperature",
        "valeur": 42.0,
        "event_time": timestamp,
    }

    valid = json.dumps(measure).encode("utf-8")

    missing_value = measure.copy()
    del missing_value["valeur"]

    invalid_value = {
        **measure,
        "valeur": "pas-un-nombre",
    }

    invalid_json = (
        '{"capteur_id": "' + sensor + '", JSON_INVALIDE'
    ).encode("utf-8")

    messages = [
        ("mesure valide", valid),
        ("doublon strict", valid),
        (
            "champ manquant",
            json.dumps(missing_value).encode("utf-8"),
        ),
        (
            "valeur non numérique",
            json.dumps(invalid_value).encode("utf-8"),
        ),
        ("JSON invalide", invalid_json),
    ]

    producer = KafkaProducer(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP", "service-kafka:9092"
        ).split(","),
        acks="all",
        enable_idempotence=True,
    )

    print(f"Capteur de test : {sensor}", flush=True)

    try:
        for label, payload in messages:
            metadata = producer.send(
                "iot.mesures",
                key=sensor.encode("utf-8"),
                value=payload,
            ).get(timeout=30)

            print(
                f"{label} : partition={metadata.partition}, "
                f"offset={metadata.offset}",
                flush=True,
            )
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()