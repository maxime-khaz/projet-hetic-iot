import json
import math
import os
from collections import Counter
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

import pendulum
import pyarrow as pa
import pyarrow.parquet as pq
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient, UpdateOne


MONGO_URL = os.getenv(
    "MONGO_URL", "mongodb://service-db:27017/iot"
)
WORK = Path("/opt/airflow/state/batch")
LAKE = Path("/opt/airflow/data-lake")

# Bornes de plausibilité de notre simulateur, à documenter.
BOUNDS = {
    "temperature": (-40, 125),
    "humidite": (0, 100),
    "vibration": (0, 1000),
}


def mongo():
    return MongoClient(
        MONGO_URL,
        tz_aware=True,
        serverSelectionTimeoutMS=10000,
        socketTimeoutMS=120000,
    )


def failure_callback(context):
    path = Path("/opt/airflow/state/alertes_airflow.log")
    with path.open("a", encoding="utf-8") as stream:
        stream.write(
            f"{datetime.now(timezone.utc).isoformat()} "
            f"dag={context['dag'].dag_id} "
            f"task={context['task_instance'].task_id} "
            f"run={context['run_id']} "
            f"error={context.get('exception')}\n"
        )


def extraction_info(context):
    return context["ti"].xcom_pull(task_ids="extract")


def iter_measurements(path):
    with open(path, encoding="utf-8") as stream:
        for line in stream:
            yield json.loads(line)


def extract(**context):
    # Pour le cron quotidien à 02:00, la date logique correspond
    # au début de l'intervalle : sa date est la journée à consolider.
    day = context["logical_date"].in_timezone("UTC").date()
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    day_string = day.isoformat()

    folder = WORK / day_string
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / "mesures.jsonl"
    temporary = folder / "mesures.jsonl.tmp"

    period = {"$gte": start, "$lt": end}
    count = 0

    with mongo() as client:
        database = client.get_default_database()

        database.mesures_brutes.create_index("event_time")
        database.mesures_brutes.create_index("received_at")
        database.dlq.create_index("received_at")
        database.alertes.create_index("date_detection")

        cursor = database.mesures_brutes.find(
            {"event_time": period},
            {
                "_id": 0,
                "capteur_id": 1,
                "type": 1,
                "valeur": 1,
            },
        ).batch_size(2000)

        with temporary.open("w", encoding="utf-8") as stream:
            for document in cursor:
                stream.write(
                    json.dumps(document, allow_nan=False) + "\n"
                )
                count += 1

        # Taux de rejet sur la journée de réception :
        # les messages invalides peuvent ne pas avoir d'event_time.
        rejected = database.dlq.count_documents(
            {"received_at": period}
        )
        received = database.mesures_brutes.count_documents(
            {"received_at": period}
        )

        alerts = Counter()
        for alert in database.alertes.find(
            {"date_detection": period},
            {"capteur_id": 1, "type": 1},
        ):
            alerts[(alert["capteur_id"], alert["type"])] += 1

    if count == 0:
        raise ValueError(f"Aucune mesure pour {day_string}")

    temporary.replace(target)

    return {
        "jour": day_string,
        "path": str(target),
        "count": count,
        "rejected": rejected,
        "received": received,
        "alerts": [
            {"capteur_id": key[0], "type": key[1], "count": value}
            for key, value in sorted(alerts.items())
        ],
    }


def quality_check(**context):
    info = extraction_info(context)

    if info["count"] < 1000:
        raise ValueError(
            f"Volume insuffisant : {info['count']} mesures, "
            "minimum attendu 1000"
        )

    invalid = 0
    for row in iter_measurements(info["path"]):
        bounds = BOUNDS.get(row.get("type"))
        value = row.get("valeur")

        if (
            bounds is None
            or isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            invalid += 1
        elif not bounds[0] <= value <= bounds[1]:
            invalid += 1

    if invalid:
        raise ValueError(
            f"{invalid} mesures hors bornes ou invalides"
        )

    total_received = info["received"] + info["rejected"]
    rejection_rate = (
        info["rejected"] / total_received
        if total_received else 0.0
    )

    if rejection_rate >= 0.02:
        raise ValueError(
            f"Taux DLQ {rejection_rate:.2%}, attendu < 2 %"
        )

    return {
        "mesures": info["count"],
        "taux_dlq": rejection_rate,
    }


def consolidate(**context):
    info = extraction_info(context)
    groups = {}

    # Calcul incrémental de la moyenne et de la variance :
    # seules les statistiques des groupes restent en mémoire.
    for row in iter_measurements(info["path"]):
        key = (row["capteur_id"], row["type"])
        value = float(row["valeur"])

        state = groups.setdefault(
            key,
            {
                "n": 0,
                "mean": 0.0,
                "m2": 0.0,
                "min": value,
                "max": value,
            },
        )

        state["n"] += 1
        delta = value - state["mean"]
        state["mean"] += delta / state["n"]
        state["m2"] += delta * (value - state["mean"])
        state["min"] = min(state["min"], value)
        state["max"] = max(state["max"], value)

    alerts = {
        (row["capteur_id"], row["type"]): row["count"]
        for row in info["alerts"]
    }

    operations = []
    for (sensor, measure_type), state in sorted(groups.items()):
        key = {
            "jour": info["jour"],
            "capteur_id": sensor,
            "type": measure_type,
        }

        stddev = (
            math.sqrt(max(0.0, state["m2"] / (state["n"] - 1)))
            if state["n"] > 1 else None
        )

        document = {
            **key,
            "moyenne": state["mean"],
            "minimum": state["min"],
            "maximum": state["max"],
            "ecart_type": stddev,
            "nb_mesures": state["n"],
            "nb_alertes": alerts.get((sensor, measure_type), 0),
        }

        operations.append(
            UpdateOne(key, {"$set": document}, upsert=True)
        )

    with mongo() as client:
        collection = (
            client.get_default_database()
            .consolidations_quotidiennes
        )

        collection.create_index(
            [("jour", 1), ("capteur_id", 1), ("type", 1)],
            unique=True,
            name="unique_consolidation",
        )
        collection.bulk_write(operations, ordered=False)

    return len(operations)


def export_parquet(**context):
    info = extraction_info(context)

    with mongo() as client:
        rows = list(
            client.get_default_database()
            .consolidations_quotidiennes
            .find({"jour": info["jour"]}, {"_id": 0})
            .sort([("capteur_id", 1), ("type", 1)])
        )

    if not rows:
        raise ValueError("Aucune consolidation à exporter")

    schema = pa.schema([
        ("jour", pa.string()),
        ("capteur_id", pa.string()),
        ("type", pa.string()),
        ("moyenne", pa.float64()),
        ("minimum", pa.float64()),
        ("maximum", pa.float64()),
        ("ecart_type", pa.float64()),
        ("nb_mesures", pa.int64()),
        ("nb_alertes", pa.int64()),
    ])

    folder = LAKE / f"date={info['jour']}"
    folder.mkdir(parents=True, exist_ok=True)

    target = folder / f"consolidation-{info['jour']}.parquet"
    temporary = folder / "consolidation.tmp"

    pq.write_table(
        pa.Table.from_pylist(rows, schema=schema),
        temporary,
        compression="snappy",
    )
    temporary.replace(target)

    return str(target)


with DAG(
    dag_id="iot_consolidation_quotidienne",
    start_date=pendulum.datetime(2026, 9, 17, tz="UTC"),
    schedule="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": "maxime",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "on_failure_callback": failure_callback,
    },
    tags=["iot", "batch"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    quality_task = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
    )

    consolidate_task = PythonOperator(
        task_id="consolidate",
        python_callable=consolidate,
    )

    export_task = PythonOperator(
        task_id="export_parquet",
        python_callable=export_parquet,
    )

    extract_task >> quality_task >> consolidate_task >> export_task