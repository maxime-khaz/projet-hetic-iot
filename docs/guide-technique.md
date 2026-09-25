# Guide technique

## Modèle de données MongoDB

### mesures_brutes

Une mesure valide contient :

- _id : SHA-256 déterministe ;
- capteur_id, type, valeur ;
- event_time et event_time_ms ;
- received_at ;
- kafka.topic, kafka.partition, kafka.offset.

L'identifiant déterministe protège contre les doublons stricts. L'offset Kafka est conservé pour l'audit.

### dlq

Un rejet contient l'identifiant de position Kafka, le message brut, la raison et received_at. L'écriture utilise un upsert afin qu'un rejeu ne crée pas plusieurs copies.

### agregats

La clé métier est :

(capteur_id, type, fenetre_debut)

Chaque document contient fenetre_fin, moyenne, minimum, maximum, écart-type échantillon et nombre de mesures.

### alertes

Une alerte référence la fenêtre courante et la fenêtre de référence. Elle conserve les seuils calculés, les extrêmes observés, la date de détection, la date de résolution éventuelle et le statut active ou resolue.

### consolidations_quotidiennes

La clé métier est :

(jour, capteur_id, type)

La collection sert à la fois à l'API et à la génération du fichier Parquet quotidien.

## Sémantique temporelle

Les dates métier sont en UTC. Le producteur envoie event_time en millisecondes Unix ; Spark le convertit en timestamp UTC.

Le watermark d'une minute signifie que Spark peut fermer une fenêtre lorsque le temps d'événement observé a suffisamment progressé. Un événement très ancien peut alors être ignoré. Cette règle limite la mémoire d'état et rend le comportement déterministe.

Les fenêtres de 30 secondes avec un pas de 10 secondes se chevauchent volontairement. Une même mesure peut donc contribuer à plusieurs fenêtres, mais une seule fois dans chaque fenêtre après dédoublonnage.

## Idempotence et reprise

Trois mécanismes sont complémentaires :

1. les checkpoints Spark mémorisent la progression des micro-batches ;
2. les clés uniques MongoDB empêchent les doublons au rejeu ;
3. les opérations UpdateOne avec upsert réécrivent le même résultat métier.

Le test de reprise force l'arrêt de Spark, redémarre le conteneur et compare les agrégats avant/après. Le test de redémarrage automatique vérifie RestartCount=1 après l'arrêt brutal du processus Java.

## Qualité batch

La qualité Airflow vérifie :

- un volume minimal de 1000 mesures ;
- les bornes physiques propres à chaque type ;
- un taux de messages rejetés inférieur à 2 % ;
- l'existence d'une sortie non vide avant export.

Les consolidations utilisent un calcul incrémental de Welford pour éviter de charger toute la journée en mémoire. Le fichier Parquet est écrit dans un fichier temporaire puis remplacé atomiquement.

## Backfill

Le DAG est configuré avec catchup=True, mais reste en pause au démarrage pour éviter un rattrapage involontaire. Le backfill de démonstration est lancé explicitement avec des dates logiques.

Les mesures du jour simulé sont identifiées par source=demo-backfill-v1 et simulated=true. Les deux exécutions produisent le même nombre de lignes et les mêmes empreintes de contenu.

## API

L'API utilise une connexion MongoDB créée au démarrage et fermée à l'arrêt. Les listes sont paginées avec une ligne supplémentaire pour calculer has_more sans compter toute la collection.

Les filtres de période sont semi-ouverts :

event_time >= debut AND event_time < fin

Les dates sans fuseau sont refusées afin d'éviter les ambiguïtés entre l'heure locale Windows et UTC.

## Commandes de diagnostic

~~~
docker compose ps
docker compose logs --tail 80 service-spark
docker compose logs --tail 80 service-api
docker compose exec service-db mongosh iot --quiet --eval "db.mesures_brutes.countDocuments({})"
docker compose exec service-db mongosh iot --quiet --eval "db.agregats.countDocuments({})"
docker compose exec service-db mongosh iot --quiet --eval "db.alertes.countDocuments({})"
docker compose exec service-airflow airflow dags list
~~~

Pour vérifier l'API :

~~~
Invoke-RestMethod http://localhost:3001/health
Invoke-RestMethod http://localhost:3001/api/capteurs
Invoke-RestMethod "http://localhost:3001/api/alertes?page_size=5"
~~~

## Limites connues

Le Compose fourni est volontairement local : Kafka est mono-nœud, MongoDB n'est pas authentifié et le scaling horizontal Spark n'est pas activé. Une architecture de production nécessiterait plusieurs brokers, réplication, secrets, authentification, stockage objet et une stratégie explicite de répartition des partitions Spark.
