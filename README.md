# Azure Databricks Insurance Pipeline

## 🚀 Contexte

Ce projet a pour objectif la conception d’une **plateforme de Data Engineering complète sur Microsoft Azure** autour d’un dataset d’assurance auto.

L’objectif est de transformer des données brutes vers des données analytiques prêtes à l’usage (BI ou future exploitation ML) en suivant une **architecture Medallion (Bronze / Silver / Gold)**.

Le dataset utilisé est issu de Kaggle : *Insurance Claims* (58 592 lignes, 41 colonnes).

---

## 🏗 Architecture d’ensemble

Le pipeline suit une approche structurée :



---

## 📁 Structure du repositor
```
azure-databricks-insurance-pipeline/
│
├── architecture/
│ ├── architecture-diagram.png
│ └── architecture-description.md
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
│ ├── delta_tables.png
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

## ⚙ Orchestration du Pipeline

La définition du job Databricks (fichier JSON) est disponible dans :
* pipeline/databricks-job-config.json

Ce job exécute :

1. Bronze  
2. Silver  
3. Gold

de façon séquentielle, avec auto-termination du cluster.

---

## 🔍 Visualisation des composants (captures)

Les **captures d’écran** sont disponibles dans le dossier :

📁 `screenshots/`

| Capture                       | Description                                                   |
|------------------------------|---------------------------------------------------------------|
| `adls_containers.png`         | Containers Bronze / Silver / Gold dans Azure Data Lake Gen2  |
| `databricks_cluster.png`      | Configuration du cluster Databricks                           |
| `delta_tables.png`            | Tables Gold créées au format Delta                            |

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

- Planification automatique via Databricks Scheduler
- Intégration d’alertes en cas d’échec
- Connecteur vers Databricks SQL Warehouse / Power BI
- Contrôles de qualité automatisés
- Mise en place CI/CD via Azure DevOps
- Sécurisation via Azure Key Vault

---

## 🧾 Licence

Ce projet est sous licence MIT — libre à utiliser et à adapter.
