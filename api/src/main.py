import os
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Literal

from bson import ObjectId
from fastapi import FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, JSONResponse
from pymongo import MongoClient
from pymongo.errors import PyMongoError


MONGO_URL = os.getenv(
    "MONGO_URL", "mongodb://service-db:27017/iot"
)

MeasureType = Literal["temperature", "humidite", "vibration"]
AlertStatus = Literal["active", "resolue"]


@asynccontextmanager
async def lifespan(app):
    client = MongoClient(
        MONGO_URL,
        tz_aware=True,
        serverSelectionTimeoutMS=5000,
        socketTimeoutMS=15000,
    )

    try:
        client.admin.command("ping")
        database = client.get_default_database()

        database.mesures_brutes.create_index(
            [
                ("capteur_id", 1),
                ("type", 1),
                ("event_time", -1),
            ],
            name="derniere_mesure_capteur_type",
        )

        app.state.database = database
        yield
    finally:
        client.close()


app = FastAPI(
    title="Plateforme IoT",
    description="Consultation des capteurs, statistiques et alertes.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/dashboard", include_in_schema=False)
def dashboard():
    return FileResponse(
        "/app/src/dashboard.html",
        media_type="text/html",
    )


@app.exception_handler(PyMongoError)
async def database_error(request, exception):
    return JSONResponse(
        status_code=503,
        content={
            "detail": "La base de données est temporairement indisponible"
        },
    )


def database():
    return app.state.database


def serialize(value):
    return jsonable_encoder(
        value,
        custom_encoder={ObjectId: str},
    )


def utc(value):
    if value is None:
        return None

    if value.tzinfo is None:
        raise HTTPException(
            status_code=422,
            detail="Les dates doivent préciser un fuseau, par exemple Z pour UTC",
        )

    return value.astimezone(timezone.utc)


def period_filter(start, end):
    start = utc(start)
    end = utc(end)

    if start is not None and end is not None and start >= end:
        raise HTTPException(
            status_code=422,
            detail="La date de début doit précéder la date de fin",
        )

    result = {}

    if start is not None:
        result["$gte"] = start

    if end is not None:
        result["$lt"] = end

    return result


def paginated(collection, filters, sort, page, page_size):
    rows = list(
        collection.find(filters)
        .sort(sort)
        .skip((page - 1) * page_size)
        .limit(page_size + 1)
        .max_time_ms(10000)
    )

    return {
        "page": page,
        "page_size": page_size,
        "has_more": len(rows) > page_size,
        "items": serialize(rows[:page_size]),
    }


@app.get("/health")
def health():
    database().command("ping")
    return {"status": "ok"}


@app.get("/api/capteurs")
def sensors():
    collection = database().mesures_brutes
    sensor_ids = sorted(collection.distinct("capteur_id"))
    result = []

    for sensor in sensor_ids:
        latest = {}

        for measure_type in (
            "temperature",
            "humidite",
            "vibration",
        ):
            document = collection.find_one(
                {
                    "capteur_id": sensor,
                    "type": measure_type,
                },
                {
                    "_id": 0,
                    "valeur": 1,
                    "event_time": 1,
                    "source": 1,
                    "simulated": 1,
                },
                sort=[("event_time", -1)],
                max_time_ms=10000,
            )

            if document is not None:
                latest[measure_type] = document

        result.append(
            {
                "capteur_id": sensor,
                "dernieres_mesures": latest,
            }
        )

    return serialize(result)


@app.get("/api/capteurs/{capteur_id}/agregats")
def sensor_aggregates(
    capteur_id: str,
    type: MeasureType | None = None,
    debut: datetime | None = None,
    fin: datetime | None = None,
    page: int = Query(1, ge=1, le=10000),
    page_size: int = Query(50, ge=1, le=500),
):
    filters = {"capteur_id": capteur_id}

    if type is not None:
        filters["type"] = type

    period = period_filter(debut, fin)

    if period:
        filters["fenetre_debut"] = period

    return paginated(
        database().agregats,
        filters,
        [
            ("fenetre_debut", -1),
            ("type", 1),
            ("_id", 1),
        ],
        page,
        page_size,
    )


@app.get("/api/alertes")
def alerts(
    statut: AlertStatus | None = None,
    capteur_id: str | None = None,
    type: MeasureType | None = None,
    debut: datetime | None = None,
    fin: datetime | None = None,
    page: int = Query(1, ge=1, le=10000),
    page_size: int = Query(50, ge=1, le=500),
):
    filters = {}

    if statut is not None:
        filters["statut"] = statut

    if capteur_id is not None:
        filters["capteur_id"] = capteur_id

    if type is not None:
        filters["type"] = type

    period = period_filter(debut, fin)

    if period:
        filters["date_detection"] = period

    return paginated(
        database().alertes,
        filters,
        [
            ("date_detection", -1),
            ("_id", 1),
        ],
        page,
        page_size,
    )


@app.get("/api/consolidations")
def consolidations(
    jour: date | None = None,
    capteur_id: str | None = None,
    type: MeasureType | None = None,
    page: int = Query(1, ge=1, le=10000),
    page_size: int = Query(50, ge=1, le=500),
):
    filters = {}

    if jour is not None:
        filters["jour"] = jour.isoformat()

    if capteur_id is not None:
        filters["capteur_id"] = capteur_id

    if type is not None:
        filters["type"] = type

    return paginated(
        database().consolidations_quotidiennes,
        filters,
        [
            ("jour", -1),
            ("capteur_id", 1),
            ("type", 1),
        ],
        page,
        page_size,
    )