# Orchestration du Pipeline – Azure Databricks

## Vue d’ensemble

Afin d’industrialiser le workflow de transformation des données, un Job Databricks a été mis en place pour orchestrer l’architecture Medallion.

Ce job automatise l’exécution des trois couches :

1. Bronze – Ingestion des données brutes
2. Silver – Nettoyage et transformation
3. Gold – Modélisation analytique (Schéma en étoile)

Cette orchestration garantit la reproductibilité, l’automatisation et le suivi des traitements.

---

## Architecture du Pipeline

Le pipeline suit un modèle séquentiel avec dépendances :

Bronze → Silver → Gold

Chaque tâche démarre uniquement si la précédente s’est exécutée avec succès.

---

## Configuration du Job

Le Job Databricks comprend :

- Un cluster partagé (ou cluster dédié au job)
- Des tâches ordonnées avec dépendances explicites
- Un système de monitoring des exécutions
- L’auto-termination activée pour optimiser les coûts

---

## Description des Tâches

### 1. Ingestion Bronze

Notebook : `01_bronze_ingestion.py`

Responsabilités :
- Chargement du dataset CSV dans Azure Data Lake (container bronze)
- Conversion au format Delta Lake
- Conservation des données sources sans modification

Objectif :
Assurer la traçabilité et l’intégrité des données brutes.

---

### 2. Transformation Silver

Notebook : `02_silver_transformation.py`

Responsabilités :
- Conversion des colonnes Yes/No en booléens
- Parsing des champs techniques :
  - max_torque → torque_nm, torque_rpm
  - max_power → power_bhp, power_rpm
- Suppression des colonnes inutiles
- Vérification de la cohérence des données
- Écriture du dataset nettoyé au format Delta (container silver)

Objectif :
Standardiser et structurer les données pour un usage analytique.

---

### 3. Modélisation Gold

Notebook : `03_gold_modeling.py`

Responsabilités :
- Construction d’un modèle en étoile :
  - fact_policy
  - dim_customer
  - dim_vehicle
  - dim_region
- Génération de clés de substitution
- Stockage des tables analytiques optimisées au format Delta (container gold)

Objectif :
Préparer les données pour l’exploitation BI et les analyses avancées.

---

## Bénéfices de l’Orchestration

La mise en place d’un Job Databricks permet :

- L’automatisation complète du pipeline
- Un suivi centralisé des exécutions
- La gestion des erreurs
- La reproductibilité des traitements
- La scalabilité de la plateforme
- Le contrôle des coûts grâce à l’auto-termination

---

## Flux d’Exécution

1. Déclenchement manuel ou planifié du job
2. Exécution de la couche Bronze
3. Si succès → exécution Silver
4. Si succès → exécution Gold
5. Données finales prêtes pour consommation analytique

---

## Monitoring

Chaque exécution du job fournit :

- Les logs détaillés
- La durée d’exécution
- Le statut (Succès / Échec)
- Les messages d’erreur le cas échéant

Cela permet une supervision efficace et un diagnostic rapide.

---

## Perspectives d’Amélioration

- Mise en place d’une planification automatique (quotidienne / hebdomadaire)
- Ajout d’alertes en cas d’échec
- Intégration d’un pipeline CI/CD
- Implémentation de contrôles qualité des données avant la couche Gold
- Connexion à une couche BI (Power BI / Databricks SQL)

---

## Conclusion

La mise en place de cette orchestration transforme ce projet en une plateforme Data Engineering de niveau professionnel.

Ce projet démontre des compétences concrètes en :

- Azure Databricks
- Azure Data Lake Gen2
- Delta Lake
- Architecture Medallion
- Modélisation analytique
- Industrialisation des pipelines
- Bonnes pratiques Cloud Data Engineering
