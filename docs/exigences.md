# Liste de suivi du sujet MD5

Source : sujet de passage de Maxime Khaznadji, 13 pages.
Les cases ne seront cochées qu'après vérification.

- [ ] Six services : producer, kafka, spark, db, airflow, api.
- [ ] Services applicatifs non-root, réseau dédié, healthchecks et volumes.
- [ ] Topic iot.mesures : 6 partitions, clé capteur_id.
- [ ] Dix capteurs, temperature / humidite / vibration, cadence 500 ms.
- [ ] Fenêtres événementielles 30 s / 10 s par (capteur_id, type).
- [ ] Moyenne, min, max, écart-type, nombre de mesures.
- [ ] Watermark 1 minute ; retards, désordre et doublons testés.
- [ ] Mesures brutes conservées pour le batch quotidien.
- [ ] DLQ : message brut, raison et horodatage ; JSON invalide sans crash.
- [ ] Upsert agrégats et index unique (capteur_id, type, fenetre_debut).
- [ ] Index API (capteur_id: 1, type: 1, fenetre_debut: -1).
- [ ] Checkpoints persistants ; crash, reprise et absence de doublons démontrés.
- [ ] Anomalies : référence précédente complète, au moins 5 observations,
      écart-type non nul, seuil absolu > 2 écarts-types.
- [ ] Une anomalie injectée par minute et cycle active / resolue démontré.
- [ ] Scaling à deux instances démontré, stratégie explicitée dans une ADR.
- [ ] Airflow à 02:00 : extract >> quality_check >> consolidate >> export_parquet.
- [ ] Journée J-1 calculée à partir des dates logiques Airflow.
- [ ] Qualité : au moins 1000 mesures, bornes physiques, DLQ < 2 %.
- [ ] retries=2, délai et backoff exponentiel, callback d'échec.
- [ ] Backfill de 3 jours exécuté deux fois, contenu identique,
      un seul fichier Parquet par jour.
- [ ] API : capteurs, agrégats filtrés et paginés, alertes, consolidations.
- [ ] README : lancement, ADR, réponses aux questions de chaque partie.
- [ ] .env.example complet ; aucun secret ni environnement virtuel versionné.
- [ ] Script inject_edge_cases.py et traces réelles d'exécution.
- [ ] Support de soutenance : 6 à 10 slides.

## Précisions à traiter

- Le sujet autorise des variantes d'architecture justifiées.
- Le scaling de requêtes Spark indépendantes ne doit pas être assimilé
  au partage automatique de partitions d'un consommateur Kafka classique.
- Ajouter des retards au-delà d'une minute aux cas 5-45 s pour démontrer
  aussi l'exclusion des événements trop anciens.
- Les données historiques de démonstration seront explicitement simulées.
- Le front et le cloud ne sont pas requis ; priorité au socle et aux preuves.
