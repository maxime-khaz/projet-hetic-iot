import logging
import os
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne
from pyspark.sql import functions as F
from pyspark.sql import types as T


logger = logging.getLogger("iot-aggregates")

MONGO_URL = os.getenv(
    "MONGO_URL", "mongodb://service-db:27017/iot"
)
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/checkpoints")

MEASUREMENT_SCHEMA = T.StructType([
    T.StructField("message_id", T.StringType()),
    T.StructField("capteur_id", T.StringType()),
    T.StructField("type", T.StringType()),
    T.StructField("valeur", T.DoubleType()),
    T.StructField("event_time", T.TimestampType()),
])


def ensure_indexes():
    with MongoClient(
        MONGO_URL, serverSelectionTimeoutMS=10000
    ) as client:
        collection = client.get_default_database().agregats

        collection.create_index(
            [
                ("capteur_id", 1),
                ("type", 1),
                ("fenetre_debut", 1),
            ],
            unique=True,
            name="unique_capteur_type_fenetre",
        )

        collection.create_index(
            [
                ("capteur_id", 1),
                ("type", 1),
                ("fenetre_debut", -1),
            ],
            name="lecture_capteur_type_date",
        )


def save_aggregates(batch, batch_id):
    operations = []

    for row in batch.toLocalIterator():
        start = datetime.fromtimestamp(
            row["debut_epoch"], tz=timezone.utc
        )
        end = datetime.fromtimestamp(
            row["fin_epoch"], tz=timezone.utc
        )

        business_key = {
            "capteur_id": row["capteur_id"],
            "type": row["type"],
            "fenetre_debut": start,
        }

        document = {
            **business_key,
            "fenetre_fin": end,
            "moyenne": row["moyenne"],
            "minimum": row["minimum"],
            "maximum": row["maximum"],
            "ecart_type": row["ecart_type"],
            "nb_mesures": row["nb_mesures"],
        }

        operations.append(
            UpdateOne(
                business_key,
                {"$set": document},
                upsert=True,
            )
        )

    if not operations:
        return

    with MongoClient(
        MONGO_URL,
        serverSelectionTimeoutMS=10000,
        socketTimeoutMS=30000,
    ) as client:
        client.get_default_database().agregats.bulk_write(
            operations, ordered=False
        )

    logger.info(
        "Agrégats batch %s : %s fenêtres enregistrées",
        batch_id,
        len(operations),
    )


def start_aggregates(spark, messages, validator):
    ensure_indexes()

    # Réutiliser exactement les mêmes règles que l'ingestion.
    def parse_valid_message(raw):
        try:
            document = validator(raw)
        except (ValueError, TypeError, OverflowError, OSError):
            return None

        return (
            document["_id"],
            document["capteur_id"],
            document["type"],
            document["valeur"],
            document["event_time"],
        )

    parse_message = F.udf(
        parse_valid_message, MEASUREMENT_SCHEMA
    )

    valid = (
        messages
        .select(parse_message(F.col("raw")).alias("mesure"))
        .where(F.col("mesure").isNotNull())
        .select("mesure.*")
    )

    # Garder un état de dédoublonnage limité par le watermark.
    unique = (
        valid
        .withWatermark("event_time", "1 minute")
        .dropDuplicates(["message_id", "event_time"])
    )

    aggregates = (
        unique
        .groupBy(
            F.col("capteur_id"),
            F.col("type"),
            F.window(
                F.col("event_time"),
                "30 seconds",
                "10 seconds",
            ),
        )
        .agg(
            F.avg("valeur").alias("moyenne"),
            F.min("valeur").alias("minimum"),
            F.max("valeur").alias("maximum"),
            F.stddev_samp("valeur").alias("ecart_type"),
            F.count("*").alias("nb_mesures"),
        )
        .select(
            "capteur_id",
            "type",
            F.col("window.start").cast("long").alias("debut_epoch"),
            F.col("window.end").cast("long").alias("fin_epoch"),
            "moyenne",
            "minimum",
            "maximum",
            "ecart_type",
            "nb_mesures",
        )
    )

    return (
        aggregates.writeStream
        .queryName("iot-aggregates")
        .outputMode("append")
        .foreachBatch(save_aggregates)
        .option(
            "checkpointLocation",
            f"{CHECKPOINT_DIR}/aggregates",
        )
        .trigger(processingTime="5 seconds")
        .start()
    )