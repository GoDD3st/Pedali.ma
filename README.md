# Pedali.ma : plateforme Big Data de micro-mobilité (Jalon 1)

Projet du cours Écosystème Big Data 2 (EMSI, Pr. Meryem Ennakri), réalisé par El Mrhari Marouane et son binôme.

Ce dépôt contient le Jalon 1 (Parties 1 à 3) : installation de Spark, Kafka et MinIO, traitement des trajets avec les RDD, puis avec les DataFrames et Spark SQL, et écriture en Parquet dans la couche silver du data lake. Les Parties 4 à 6 sont pour le Jalon 2.

## Architecture

```
 producer.py ──► trips_history.jsonl ──► MinIO  lake/bronze/history
      │                                      │
      └──► Kafka (topic "trips", 3 part.)    ▼
                                      Spark (Master + 2 Workers)
                                             │  Jupyter = Driver
                                             ▼
                                      MinIO  lake/silver/trips_history  (Parquet, partitionBy city)
```

| Service | Rôle | Image |
|---|---|---|
| `kafka` | Broker Kafka (mode KRaft, sans ZooKeeper) | `apache/kafka:3.8.0` |
| `minio` | Stockage objet S3 (data lake) | `scnd/minio-mirror:RELEASE.2025-09-07T16-13-09Z` |
| `minio-init` | Crée le bucket `lake` au démarrage | `scnd/mc-mirror:RELEASE.2025-02-04T04-57-50Z` |
| `spark-master` | Cluster Manager Spark standalone | `quay.io/jupyter/pyspark-notebook:spark-3.5.3` |
| `spark-worker-1`, `spark-worker-2` | Workers (2 cœurs, 1 Gio chacun) | idem |
| `jupyter` | Driver PySpark et notebooks | idem |

## Versions et dépendances

- Apache Spark 3.5.3 (Python 3 et PySpark fournis par l'image Jupyter)
- Apache Kafka 3.8.0 (KRaft)
- MinIO, image miroir (voir Notes) : console sur le port `9001`, API S3 sur `9000`
- Connecteur S3A : `hadoop-aws:3.3.4` et `aws-java-sdk-bundle:1.12.262`, téléchargés automatiquement par `spark.jars.packages`
- Docker Desktop avec Docker Compose v2
- Générateur de données : Python 3.9 ou plus ; `kafka-python` seulement pour le mode Kafka (`pip install kafka-python`)

Prévoir 8 Gio de RAM pour Docker.

## Démarrage rapide

```bash
# 1. Lancer l'infrastructure
docker compose up -d
docker compose ps

# 2. Créer le topic Kafka (3 partitions)
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic trips --partitions 3 --replication-factor 1

# 3. Générer l'historique des trajets (50 000 lignes) dans ./data
cd data
python producer.py --mode file --n 50000     # produit trips_history.jsonl et stations.json
cd ..
```

4. Déposer `trips_history.jsonl` dans MinIO (console http://localhost:9001, `minioadmin` / `minioadmin`), dans le bucket `lake`, dossier `bronze/history/`.
5. Ouvrir Jupyter sur http://localhost:8888 (jeton `pedali`), puis le dossier `work/`.

Pour arrêter : `docker compose down`. Les données MinIO restent dans le volume `minio_data` ; `docker compose down -v` les supprime.

## Interfaces web

| Interface | URL |
|---|---|
| Spark Master | http://localhost:8080 |
| Spark Worker 1 / 2 | http://localhost:8081 / http://localhost:8082 |
| Spark UI de l'application | http://localhost:4040 |
| Jupyter (jeton `pedali`) | http://localhost:8888 |
| Console MinIO | http://localhost:9001 |
| Kafka (depuis l'hôte) | `localhost:29092` |

## Notebooks

Les notebooks s'exécutent dans cet ordre. Le cluster n'a que 4 cœurs, donc il faut arrêter la session Spark de `partie1&2.ipynb` (`spark.stop()`) avant de lancer `partie3.ipynb`.

| Notebook | Contenu |
|---|---|
| `notebooks/partie1&2.ipynb` | Partie 1 : connexion au cluster (Driver, SparkContext). Partie 2 : RDD avec `textFile`, `map`, `filter`, `reduceByKey`, `cache`, lineage (`toDebugString`), DAG et stages dans la Spark UI. |
| `notebooks/partie3.ipynb` | Partie 3 : schémas explicites, DataFrames, Spark SQL, jointure avec les stations, UDF contre fonctions natives, `explain(True)`, écriture Parquet partitionnée par ville dans `s3a://lake/silver/trips_history/`. |

## Résultats

- 50 000 trajets et 400 stations chargés avec un schéma explicite.
- Chiffre d'affaires des trajets `ENDED` par ville (MAD) : Casablanca 98 655,90 ; Rabat 55 483,53 ; Marrakech 53 879,35 ; Tanger 36 678,65. Les versions RDD, DataFrame et SQL donnent les mêmes valeurs.
- Pour le même calcul (CA par ville), le pipeline prend 2,35 s en RDD et 0,82 s en DataFrame. Ce sont des mesures uniques, à lire comme un ordre de grandeur.
- Couche silver : Parquet partitionné par `city`, dédoublonné sur `(trip_id, status)`.

## Structure du dépôt

```
.
├── docker-compose.yml
├── README.md
├── .gitignore
├── data/
│   ├── producer.py          # générateur de trajets (fichier ou Kafka)
│   ├── stations.json        # 400 stations
│   └── trips_history.jsonl  # généré, ignoré par Git
├── notebooks/
│   ├── partie1&2.ipynb
│   └── partie3.ipynb
└── Screenshots/             # captures du rapport (ignoré par Git)
```

## Notes

- `producer.py` et `stations.json` de ce dépôt sont des remplacements : les fichiers de l'enseignant sont arrivés le 22/11, après nos mesures. Le schéma des trajets est le même, mais les chiffres diffèrent de ceux des fichiers officiels ; les notebooks sont à relancer dessus.
- Les images MinIO officielles (`minio/minio`, `quay.io/minio/minio`) n'étaient pas téléchargeables pendant le développement, d'où l'utilisation de miroirs tiers du Docker Hub.
- Le premier passage de l'UDF inclut le démarrage des processus Python, ce qui gonfle sa première mesure.
- Environ 2 % des trajets sont des doublons. Ils sont supprimés à la création de la couche silver.
- Les Parties 4 à 6 (streaming, agrégations temporelles, couche gold) seront faites au Jalon 2.
