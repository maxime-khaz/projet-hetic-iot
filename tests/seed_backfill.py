import os
import random
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient, UpdateOne


def main():
    day = datetime(2026, 9, 19, tzinfo=timezone.utc)
    generated_at = datetime.now(timezone.utc)
    rng = random.Random(20260919)

    profiles = {
        "temperature": (22.0, 1.5),
        "humidite": (50.0, 5.0),
        "vibration": (10.0, 1.0),
    }

    operations = []

    for slot in range(96):
        event_time = day + timedelta(minutes=15 * slot)

        for sensor_number in range(1, 11):
            sensor = f"capteur-{sensor_number}"

            for measure_type, (mean, stddev) in profiles.items():
                identifier = (
                    f"demo-backfill-v1:2026-09-19:"
                    f"{sensor}:{measure_type}:{slot}"
                )

                document = {
                    "_id": identifier,
                    "capteur_id": sensor,
                    "type": measure_type,
                    "valeur": round(rng.gauss(mean, stddev), 2),
                    "event_time": event_time,
                    "event_time_ms": int(event_time.timestamp() * 1000),
                    "received_at": generated_at,
                    "simulated": True,
                    "source": "demo-backfill-v1",
                    "generation_note": (
                        "Historique synthetique pour demonstration batch; "
                        "un releve toutes les 15 minutes"
                    ),
                }

                operations.append(
                    UpdateOne(
                        {"_id": identifier},
                        {"$setOnInsert": document},
                        upsert=True,
                    )
                )

    with MongoClient(os.environ["MONGO_URL"]) as client:
        collection = client.get_default_database().mesures_brutes
        result = collection.bulk_write(operations, ordered=False)

        print(f"Mesures ajoutees : {result.upserted_count}")
        print(
            "Total du jeu simule :",
            collection.count_documents({"source": "demo-backfill-v1"}),
        )


if __name__ == "__main__":
    main()