import json
import logging
import os
import random
import signal
import threading
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("iot-producer")

STOP = threading.Event()
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "service-kafka:9092")
TOPIC = "iot.mesures"

# Moyenne, écart-type, borne basse, borne haute.
PROFILES = {
    "temperature": (22.0, 1.5, -20.0, 60.0),
    "humidite": (50.0, 5.0, 0.0, 100.0),
    "vibration": (10.0, 1.0, 0.0, 100.0),
}


def request_stop(signum, frame):
    STOP.set()


def generate_value(measure_type):
    mean, stddev, lower, upper = PROFILES[measure_type]
    value = random.gauss(mean, stddev)
    return round(max(lower, min(upper, value)), 2)


def connect():
    delay = 1

    while not STOP.is_set():
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP.split(","),
                key_serializer=lambda key: key.encode("utf-8"),
                value_serializer=lambda value: json.dumps(
                    value, allow_nan=False
                ).encode("utf-8"),
                acks="all",
                enable_idempotence=True,
                linger_ms=10,
                max_block_ms=10000,
            )
        except NoBrokersAvailable:
            logger.warning("Kafka indisponible, nouvel essai dans %ss", delay)
            STOP.wait(delay)
            delay = min(delay * 2, 30)

    return None


def main():
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    producer = connect()
    if producer is None:
        return

    logger.info("Connecté à %s ; topic=%s", BOOTSTRAP, TOPIC)
    total = 0
    last_log = time.monotonic()

    try:
        while not STOP.is_set():
            started = time.monotonic()
            pending = []

            for sensor_number in range(1, 11):
                for measure_type in PROFILES:
                    measure = {
                        "capteur_id": f"capteur-{sensor_number}",
                        "type": measure_type,
                        "valeur": generate_value(measure_type),
                        "event_time": time.time_ns() // 1_000_000,
                    }
                    pending.append(
                        producer.send(
                            TOPIC,
                            key=measure["capteur_id"],
                            value=measure,
                        )
                    )

            # Vérifier la réception par Kafka avant de compter les messages.
            for future in pending:
                future.get(timeout=30)

            total += len(pending)
            if time.monotonic() - last_log >= 10:
                logger.info("%s mesures confirmées par Kafka", total)
                last_log = time.monotonic()

            elapsed = time.monotonic() - started
            STOP.wait(max(0, 0.5 - elapsed))
    finally:
        producer.close(timeout=10)


if __name__ == "__main__":
    main()