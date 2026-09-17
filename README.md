# Plateforme de supervision IoT

Projet pédagogique de Maxime Khaznadji : Kafka, Spark Structured Streaming,
MongoDB, Airflow et API REST.

## État du projet

Étape 1 : infrastructure Kafka et MongoDB configurée.
La validation de démarrage sur Docker Desktop est encore à réaliser.
Les quatre services applicatifs seront ajoutés dans les étapes suivantes.
Ce dépôt ne constitue pas encore le livrable final.

## Prérequis

- Docker Desktop démarré, moteur Linux fonctionnel.
- Docker Compose v2 ou supérieur et Git.
- Commandes ci-dessous exécutées à la racine du dépôt dans PowerShell.

## Démarrer l'infrastructure

Après clonage, ou après `git pull --ff-only origin main` dans le dépôt existant :

```powershell
Copy-Item .env.example .env
docker compose config --quiet
docker compose up -d --wait
docker compose ps
```

Ne recopier .env.example que lors de la première installation, pour préserver
les éventuelles valeurs locales. Les deux services doivent être healthy.
Le premier démarrage télécharge les images.

Créer explicitement le topic (commande rejouable) :

```powershell
docker compose exec service-kafka kafka-topics --bootstrap-server service-kafka:9092 --create --if-not-exists --topic iot.mesures --partitions 6 --replication-factor 1
docker compose exec service-kafka kafka-topics --bootstrap-server service-kafka:9092 --describe --topic iot.mesures
docker compose exec service-db mongosh --quiet --eval "db.adminCommand('ping')"
```

Attendu : PartitionCount: 6, ReplicationFactor: 1, et ok: 1 pour MongoDB.
L'option --if-not-exists ne corrige pas un topic existant mal configuré :
toujours vérifier la description.

En cas d'échec :

```powershell
docker compose logs --tail 100 service-kafka service-db
```

Arrêter sans supprimer les données :

```powershell
docker compose down
```

Ne pas utiliser `docker compose down -v` pour un simple arrêt : cette
option supprime les volumes et leurs données.

## ADR 001 : infrastructure locale

Kafka utilise KRaft, avec un seul nœud réunissant broker et contrôleur :
cela évite un service ZooKeeper supplémentaire pour cette démonstration.
Le facteur de réplication vaut 1 ; cette configuration n'offre pas de haute
disponibilité et n'est pas un déploiement de production.

Kafka conserve ses journaux dans kafka-data ; MongoDB conserve ses collections
et index dans mongo-data. Les volumes survivent à la recréation des conteneurs.

Les services communiquent sur iot-net par leurs noms DNS Docker.
Aucun port Kafka ou MongoDB n'est publié sur Windows à ce stade.
Les outils de diagnostic sont lancés dans les conteneurs avec compose exec.
MongoDB n'a pas encore d'authentification : usage local pédagogique uniquement.

Les healthchecks vérifient la disponibilité effective des deux services.
Les prochains producer et Spark utiliseront depends_on avec service_healthy,
ainsi que des tentatives de reconnexion dans leur code : le healthcheck ne
garantit pas que la connexion restera disponible après le démarrage.

## Parcours de réalisation

1. Valider Kafka, MongoDB et les six partitions.
2. Ajouter le producteur : dix capteurs, trois types, cadence de 500 ms,
   clé Kafka capteur_id.
3. Ajouter Spark : fenêtres 30 s / pas 10 s par (capteur_id, type),
   watermark 1 minute, mesures brutes, dédoublonnage, DLQ, upserts et checkpoints.
4. Détecter les anomalies avec la fenêtre précédente complète comme référence,
   puis gérer les statuts active et resolue.
5. Ajouter Airflow : quatre tâches, qualité, consolidation, Parquet,
   retries, alerte d'échec et backfill de trois jours.
6. Ajouter l'API et les démonstrations, compléter les réponses théoriques
   et préparer les slides.

Le scaling Spark nécessite une décision spécifique avant implémentation :
ne pas lancer deux requêtes indépendantes partageant le même checkpoint.
Le partitionnement Kafka ne suffit pas à lui seul à distribuer correctement
deux applications Spark Structured Streaming.

## Référence de configuration

[Configuration Docker Confluent](https://docs.confluent.io/platform/7.6/installation/docker/config-reference.html)
