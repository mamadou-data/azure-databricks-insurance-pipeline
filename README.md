# Azure Databricks Insurance Pipeline

## 🚀 Contexte

Ce projet a pour objectif la conception d’une **plateforme de Data Engineering complète sur Microsoft Azure** autour d’un dataset d’assurance auto.

L’objectif est de transformer des données brutes vers des données analytiques prêtes à l’usage (BI ou future exploitation ML) en suivant une **architecture Medallion (Bronze / Silver / Gold)**.

Le dataset utilisé est issu de Kaggle : *Insurance Claims* (58 592 lignes, 41 colonnes).

---
## 💼 Problématique métier

Une compagnie d’assurance souhaite :

- Centraliser ses données contrats et sinistres
- Nettoyer les données issues de différents systèmes
- Disposer d’un modèle analytique fiable pour :
  - Analyse des primes
  - Analyse des risques
  - Analyse régionale

---

## 🏗 Architecture d’ensemble

Le pipeline suit une approche structurée :
```
CSV (Raw)
   ↓
Bronze (Delta Raw)
   ↓
Silver (Cleaned)
   ↓
Gold (Star Schema)
   ↓
BI / Analytics
```
---

## ⚡ Optimisations mises en place

- Format Delta Lake pour performance et ACID
- Partitionnement des tables Gold
- Auto-termination du cluster
- Architecture modulaire évolutive

---

---

## 🏭 Industrialisation & Bonnes Pratiques

- Architecture Medallion (séparation claire des couches)
- Format Delta Lake (ACID, performance, versioning)
- Orchestration centralisée via Databricks Job
- Planification automatique
- Gestion des dépendances et arrêt en cas d’échec
- Optimisation des ressources cloud

---

## 📁 Structure du repositor
```
azure-databricks-insurance-pipeline/
│
├── data/
│ └── insurance_claims_sample.csv
│
├── notebooks/
│ ├── 01_bronze_ingestion.py
│ ├── 02_silver_transformation.py
│ ├── 03_gold_modeling.py
│ └── 04_job_orchestration.md
│
├── pipeline/
│ └── databricks-job-config.json
│
├── screenshots/
│ ├── adls_containers.png
│ ├── databricks_cluster.png
│ ├── job-architecture.png
│
└── README.md
```
---

## 📌 Description des composants

### 🟫 Bronze – Ingestion des données

Le notebook `01_bronze_ingestion.py` lit le CSV source et écrit les données brutes dans le container Bronze au format **Delta Lake**.

Objectif :  
📍 Conserver l’état source sans transformation.

---

### 🟦 Silver – Nettoyage et transformations

Exécuté dans `02_silver_transformation.py`, ce notebook réalise :

- Conversion des colonnes `Yes` / `No` en booléens
- Parsing des colonnes techniques :
  - `max_torque` → `torque_nm`, `torque_rpm`
  - `max_power` → `power_bhp`, `power_rpm`
- Nettoyage des colonnes inutiles
- Standardisation des formats

Objectif :  
🔹 Préparer un jeu de données propre et cohérent pour la modélisation.

---

### 🟩 Gold – Modèle analytique

Le notebook `03_gold_modeling.py` génère un schéma en **étoile** avec :

- `fact_policy`
- `dim_customer`
- `dim_vehicle`
- `dim_region`

Objectif :  
📊 Construire des tables prêtes à l’usage pour BI ou exploration avancée.

---

## ⚙ Orchestration & Planification

La définition du job Databricks est disponible dans :
* `pipeline/databricks-job-config.json`

Le pipeline est orchestré via un **Job Azure Databricks planifié automatiquement**.

Caractéristiques :

- Exécution séquentielle : Bronze → Silver → Gold
- Dépendances explicites entre tâches
- Planification automatique via scheduler Databricks
- Historique complet des runs
- Monitoring intégré
- Auto-termination du cluster pour optimisation des coûts

Cette configuration rapproche le projet d’un environnement de production réel.

---

## 🔍 Visualisation des composants (captures)

Les **captures d’écran** sont disponibles dans le dossier :

📁 `screenshots/`

| Capture                       | Description                                                   |
|------------------------------|---------------------------------------------------------------|
| `adls_containers.png`         | Containers Bronze / Silver / Gold dans Azure Data Lake Gen2  |
| `databricks_cluster.png`      | Configuration du cluster Databricks                           |
| `job-architecture.png`      | l'image de l'exécution des différentes tasks                           |

---

## 🧠 Technologies utilisées

- ☁️ Azure Data Lake Storage Gen2 (ADLS)
- 🔥 Azure Databricks (Spark & Delta Lake)
- 🧪 Spark DataFrame API
- 📁 Python / SQL
- 🔁 Databricks Job (orchestration)
- 📊 (Préparation BI future)

---

## 🎯 Résultats

- Pipeline Data Engineering complet et automatisé
- Architecture Medallion mise en place
- Données structurées prêtes à l’usage
- Base solide pour BI ou Analytics

---

## 📈 Perspectives d’évolution

Voici des pistes d’amélioration futures :

- Intégration d’alertes en cas d’échec
- Connecteur vers Databricks SQL Warehouse / Power BI
- Contrôles de qualité automatisés
- Mise en place CI/CD via Azure DevOps
- Sécurisation via Azure Key Vault

---

## 🧾 Licence

Ce projet est sous licence MIT — libre à utiliser et à adapter.
