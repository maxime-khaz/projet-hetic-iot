import hashlib
import json
import logging
import math
import os
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne
from pyspark.sql import SparkSession


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("iot-streaming")

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP", "service-kafka:9092"
)
MONGO_URL = os.getenv(
    "MONGO_URL", "mongodb://service-db:27017/iot"
)
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/checkpoints")

MEASURE_TYPES = {"temperature", "humidite", "vibration"}


def validate_message(raw):
    if raw is None:
        raise ValueError("Message Kafka sans valeur")

    message = json.loads(raw)

    if not isinstance(message, dict):
        raise ValueError("Le JSON doit être un objet")

    required = {"capteur_id", "type", "valeur", "event_time"}
    if not required.issubset(message):
        raise ValueError("Champ obligatoire manquant")

    sensor = message["capteur_id"]
    if not isinstance(sensor, str) or not sensor.strip():
        raise ValueError("capteur_id doit être une chaîne non vide")

    measure_type = message["type"]
    if not isinstance(measure_type, str):
        raise ValueError("type doit être une chaîne")
    if measure_type not in MEASURE_TYPES:
        raise ValueError("Type de mesure inconnu")

    value = message["valeur"]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("valeur doit être numérique")
    if not math.isfinite(value):
        raise ValueError("valeur doit être finie")

    timestamp = message["event_time"]
    if type(timestamp) is not int or timestamp < 0:
        raise ValueError("event_time doit être un entier positif en ms")

    event_time = datetime.fromtimestamp(
        timestamp / 1000, tz=timezone.utc
    )

    # Une représentation stable des quatre champs métier.
    canonical = json.dumps(
        {
            "capteur_id": sensor,
            "type": measure_type,
            "valeur": value,
            "event_time": timestamp,
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    identifier = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    return {
        "_id": identifier,
        "capteur_id": sensor,
        "type": measure_type,
        "valeur": float(value),
        "event_time": event_time,
        "event_time_ms": timestamp,
    }


def save_batch(batch, batch_id):
    measurements = []
    rejected = []
    received_at = datetime.now(timezone.utc)

    for row in batch.toLocalIterator():
        position = {
            "topic": row["topic"],
            "partition": row["partition"],
            "offset": row["offset"],
        }

        try:
            document = validate_message(row["raw"])
        except (ValueError, TypeError, OverflowError, OSError) as error:
            identifier = (
                f"{row['topic']}:{row['partition']}:{row['offset']}"
            )
            document = {
                "_id": identifier,
                "message_brut": row["raw"],
                "raison": str(error),
                "received_at": received_at,
                "kafka": position,
            }
            rejected.append(
                UpdateOne(
                    {"_id": identifier},
                    {"$setOnInsert": document},
                    upsert=True,
                )
            )
        else:
            document["received_at"] = received_at
            document["kafka"] = position

            measurements.append(
                UpdateOne(
                    {"_id": document["_id"]},
                    {"$setOnInsert": document},
                    upsert=True,
                )
            )

    if not measurements and not rejected:
        return

    with MongoClient(
        MONGO_URL,
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=10000,
        socketTimeoutMS=30000,
    ) as client:
        database = client.get_default_database()

        if measurements:
            database.mesures_brutes.bulk_write(
                measurements, ordered=False
            )
        if rejected:
            database.dlq.bulk_write(rejected, ordered=False)

    logger.info(
        "Batch %s enregistré : %s messages valides, %s rejets",
        batch_id,
        len(measurements),
        len(rejected),
    )


def main():
    spark = (
        SparkSession.builder
        .appName("iot-ingestion")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    messages = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "iot.mesures")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 3000)
        .option("failOnDataLoss", "true")
        .load()
        .selectExpr(
            "CAST(value AS STRING) AS raw",
            "topic",
            "partition",
            "offset",
        )
    )

    query = (
        messages.writeStream
        .queryName("iot-ingestion")
        .foreachBatch(save_batch)
        .option(
            "checkpointLocation",
            f"{CHECKPOINT_DIR}/ingestion",
        )
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()