import logging
from datetime import timedelta


logger = logging.getLogger("iot-anomalies")


def ensure_alert_indexes(database):
    database.alertes.create_index(
        [
            ("capteur_id", 1),
            ("type", 1),
            ("fenetre_debut", 1),
        ],
        unique=True,
        name="unique_alerte_fenetre",
    )

    database.alertes.create_index(
        [
            ("capteur_id", 1),
            ("type", 1),
            ("statut", 1),
            ("fenetre_fin", 1),
        ],
        name="resolution_alertes",
    )

    database.alertes.create_index(
        [("statut", 1), ("date_detection", -1)],
        name="lecture_alertes",
    )


def detect_anomalies(database, windows):
    created = 0
    resolved = 0

    # L'ordre chronologique est essentiel pour gérer les statuts.
    windows = sorted(
        windows,
        key=lambda item: (
            item["fenetre_debut"],
            item["capteur_id"],
            item["type"],
        ),
    )

    for current in windows:
        # Référence de 30 secondes se terminant au début
        # de la fenêtre analysée : aucun chevauchement.
        reference_start = (
            current["fenetre_debut"] - timedelta(seconds=30)
        )

        reference = database.agregats.find_one({
            "capteur_id": current["capteur_id"],
            "type": current["type"],
            "fenetre_debut": reference_start,
            "fenetre_fin": current["fenetre_debut"],
        })

        if reference is None:
            continue

        stddev = reference.get("ecart_type")
        if reference["nb_mesures"] < 5:
            continue
        if stddev is None or stddev <= 0:
            continue

        mean = reference["moyenne"]
        lower = mean - 2 * stddev
        upper = mean + 2 * stddev

        # Si une valeur est hors seuil, le minimum ou le maximum
        # de la fenêtre est nécessairement hors seuil.
        anomalous = (
            current["minimum"] < lower
            or current["maximum"] > upper
        )

        key = {
            "capteur_id": current["capteur_id"],
            "type": current["type"],
            "fenetre_debut": current["fenetre_debut"],
        }

        if anomalous:
            result = database.alertes.update_one(
                key,
                {
                    "$setOnInsert": {
                        **key,
                        "fenetre_fin": current["fenetre_fin"],
                        "statut": "active",
                        # Date événementielle de la fenêtre :
                        # stable lors d'un rejeu.
                        "date_detection": current["fenetre_fin"],
                        "date_resolution": None,
                        "reference_debut": reference["fenetre_debut"],
                        "reference_fin": reference["fenetre_fin"],
                        "moyenne_reference": mean,
                        "ecart_type_reference": stddev,
                        "nb_mesures_reference": reference["nb_mesures"],
                        "seuil_bas": lower,
                        "seuil_haut": upper,
                        "minimum_observe": current["minimum"],
                        "maximum_observe": current["maximum"],
                        "nb_mesures": current["nb_mesures"],
                    }
                },
                upsert=True,
            )

            if result.upserted_id is not None:
                created += 1

        else:
            result = database.alertes.update_many(
                {
                    "capteur_id": current["capteur_id"],
                    "type": current["type"],
                    "statut": "active",
                    "fenetre_fin": {
                        "$lt": current["fenetre_fin"]
                    },
                },
                {
                    "$set": {
                        "statut": "resolue",
                        "date_resolution": current["fenetre_fin"],
                    }
                },
            )
            resolved += result.modified_count

    logger.info(
        "Alertes : %s nouvelles, %s résolues",
        created,
        resolved,
    )