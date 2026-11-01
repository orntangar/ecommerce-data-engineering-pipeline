[![integration](https://github.com/orntangar/ecommerce-data-engineering-pipeline/actions/workflows/integration.yml/badge.svg?branch=main)](https://github.com/orntangar/ecommerce-data-engineering-pipeline/actions/workflows/integration.yml)

# E-commerce Data Engineering Pipeline

**Portfolio SQL / Data Engineering** — PostgreSQL · dbt · Apache Airflow · Python · GitHub Actions.

**État du projet : validé en environnement local**

Ce projet démontre un pipeline ELT **rejouable et testé** : ingestion d'événements de commandes, gestion des arrivées tardives et des suppressions logiques, puis calcul du chiffre d'affaires dans un Data Mart avec **Python, PostgreSQL, dbt et Apache Airflow**.

Le pipeline a été exécuté et testé avec succès sur Windows 11 et Docker Desktop.

**Résultats obtenus :**
- 3 lots de données traités.
- 8 événements ingérés.
- 5 commandes enregistrées, dont 4 actives.
- 300 € de chiffre d'affaires calculé.
- 27 opérations dbt réussies lors des validations.
- 2 exécutions Airflow réussies.
- Tests d'intégration et d'idempotence validés.

Le projet constitue une démonstration technique locale et n'est pas encore destiné à un déploiement en production.

## Objectif

Construire un pipeline ELT qui traite des événements de commandes, supporte les mises à jour et les arrivées tardives, teste la qualité des données et reste cohérent après un rejeu.

Architecture détaillée : [docs/architecture.md](docs/architecture.md). Procédures de reprise : [docs/runbook.md](docs/runbook.md).

```mermaid
flowchart LR
    A[JSONL] --> B[Ingestion Python]
    B --> C[(PostgreSQL)]
    C --> D[dbt staging]
    D --> E[dbt MERGE incrémental]
    E --> F[(Data Mart)]
    F --> T[Tests dbt]
    T --> H[Validation du lot]

    G[Airflow] -.-> B
    G -.-> E
    G -.-> H
```

## Installation en local

Pré-requis : Docker Compose et Python 3.11+.

Les identifiants fournis sont **exclusivement réservés au développement local**.

```bash
docker compose up -d
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
$env:DB_DSN = "postgresql://portfolio:portfolio@localhost:55432/portfolio"
```

Sous Windows, adapter l'activation du virtualenv et les variables d'environnement à son terminal.

## Exécuter le pipeline sans Airflow

### Batch 1 — trois commandes

```bash
python scripts/ingest.py --file data/orders_batch_001.jsonl --batch-id demo_001
dbt build --project-dir dbt --profiles-dir dbt --vars '{"batch_id":"demo_001"}'
python scripts/mark_processed.py --batch-id demo_001
```

### Batch 2 — modification, suppression logique et événement tardif

```bash
python scripts/ingest.py --file data/orders_batch_002.jsonl --batch-id demo_002
dbt build --project-dir dbt --profiles-dir dbt --vars '{"batch_id":"demo_002"}'
python scripts/mark_processed.py --batch-id demo_002
python scripts/report.py
```


### Batch 3 — nouvelle commande et validation finale

Ce troisième lot ajoute une commande de 45 € et permet de vérifier le bon fonctionnement du chargement incrémental.

Exécuter les commandes suivantes dans PowerShell :

```powershell
python scripts/ingest.py --file data/orders_batch_003.jsonl --batch-id demo_003
dbt build --project-dir dbt --profiles-dir dbt --vars "batch_id: demo_003"
python scripts/mark_processed.py --batch-id demo_003
python scripts/check_demo.py
```

Le script de validation vérifie les résultats après traitement des trois lots.


Résultats attendus après traitement des trois lots : 8 événements bruts, 5 commandes, 4 commandes actives et 300,00 € de chiffre d'affaires.

### Tester la reprise et l'idempotence

Rejouer les **mêmes** commandes du batch `demo_002` une seconde fois, puis relancer `python scripts/check_demo.py`. Les données finales et le montant ne doivent pas changer.

Pour reconstruire intégralement la table des commandes :

```bash
dbt build --full-refresh --project-dir dbt --profiles-dir dbt --vars '{"batch_id":"demo_002"}'
```

### Exemples de requêtes à montrer en entretien

```sql
SELECT order_id, latest_event_id, amount_eur, is_deleted
FROM analytics.fct_orders ORDER BY order_id;

SELECT order_date, order_count, revenue_eur
FROM analytics.mart_daily_revenue ORDER BY order_date;

SELECT batch_id, status, processed_at
FROM public.ingest_batches ORDER BY created_at;
```

## Exécution avec Airflow

Le DAG d'exemple est dans [`airflow/dags/ecommerce_elt.py`](airflow/dags/ecommerce_elt.py).

Il déclenche en série : **ingestion → dbt build + tests → validation du batch**.

Airflow n'est pas installé dans `requirements.txt` afin de garder l'environnement local léger. Apache Airflow 2.10.5 est exécuté dans un conteneur Docker. Son environnement est défini dans airflow/Dockerfile et le service airflow est configuré dans docker-compose.yml.

Cette configuration permet d'exécuter localement le DAG ecommerce_elt avec PostgreSQL et dbt, sans installer Airflow directement sur Windows.

Le worker doit voir le dépôt, disposer de la commande `dbt`, de `DB_DSN` et des variables `PG*`, ainsi que d'un accès à PostgreSQL.

Déclencher le DAG `ecommerce_elt` successivement avec :

```json
{"batch_id":"demo_001","file_name":"orders_batch_001.jsonl"}
```

puis :

```json
{"batch_id":"demo_002","file_name":"orders_batch_002.jsonl"}
```

{"batch_id":"demo_003","file_name":"orders_batch_003.jsonl"}


Le DAG est pensé pour Airflow 2.x.

## Exécution d'Apache Airflow avec Docker

Le projet intègre Apache Airflow 2.10.5 via Docker Compose. Une image personnalisée est définie dans airflow/Dockerfile et contient les dépendances nécessaires à l'exécution des transformations dbt.

Le pipeline ecommerce_elt a été exécuté avec succès en environnement local. Il automatise l'ingestion Python, les transformations et tests dbt, puis la validation des lots de données.

Cette configuration est destinée au développement et à la démonstration, et non à un environnement de production. 

## Contrôles de qualité

- `event_id` et `order_id` non nuls ; événements uniques.
- Une seule ligne par `order_id` dans `analytics.fct_orders`.
- Clés de dimensions valides.
- Cohérence entre derniers événements admissibles et table de faits.
- Cohérence des agrégats de chiffre d'affaires.
- Un script d'assertions end-to-end et une CI GitHub Actions.

## Scénarios d'entretien à présenter

1. **Rejeu** : pourquoi relancer le batch ne crée pas de doublons.
2. **Événement tardif** : pourquoi `e-007` ne remplace pas `e-004`.
3. **Suppression** : pourquoi une commande reste dans les faits mais sort du revenu.
4. **Panne de transformation** : pourquoi un batch non validé peut être rejoué.
5. **Limites** : ce qu'il faut ajouter pour passer au multi-worker et à des volumes élevés.

## Compétences démontrées

- **Python et PostgreSQL** : ingestion transactionnelle et idempotence des lots.
- **SQL et dbt** : transformations incrémentales, gestion des événements tardifs et suppressions logiques.
- **Apache Airflow** : orchestration de l'ingestion, des transformations et de la validation.
- **Qualité des données** : tests dbt et validations automatisées.
- **GitHub Actions** : intégration continue et tests d'idempotence.

## Preuves d'exécution

### GitHub Actions - intégration continue réussie

![Exécution GitHub Actions réussie](docs/screenshots/github_actions_success.png)

### Apache Airflow - DAG `ecommerce_elt`

![Graphique Airflow - trois tâches réussies](docs/screenshots/airflow_graph_success.png)

### dbt - transformations et tests réussis

![Exécution dbt : 27 opérations réussies, dont 22 tests](docs/screenshots/dbt_build_success.png)

### PostgreSQL - chiffre d'affaires quotidien

![Résultats SQL : 4 commandes actives et 300 euros de chiffre d'affaires](docs/screenshots/sql_daily_revenue_success.png)

