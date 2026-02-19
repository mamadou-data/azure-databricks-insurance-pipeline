# Orchestration du Pipeline – Azure Databricks

## 🎯 Objectif

Ce document décrit la mise en production du pipeline via un **Job Azure Databricks planifié**, garantissant une exécution automatisée, contrôlée et supervisée.

---

## ⚙️ Configuration du Job

Le job est défini via le fichier :

pipeline/databricks-job-config.json

### Paramètres techniques :

- Cluster dédié au job
- Auto-termination activée
- Dépendances explicites entre tâches
- Planification automatique configurée (Scheduler Databricks)

---

## 🕒 Planification Automatique

Le pipeline est exécuté automatiquement selon une fréquence définie via le scheduler Databricks.

Caractéristiques :

- Exécution quotidienne planifiée
- Aucun déclenchement manuel nécessaire
- Historique complet des runs disponible
- Possibilité de relancer un run spécifique

Cette planification permet d’intégrer le pipeline dans un environnement proche de la production.

---

## 🔁 Logique d’Orchestration

Le workflow suit un enchaînement conditionnel :

1. Bronze
2. Silver (si Bronze succès)
3. Gold (si Silver succès)

En cas d’échec :
- Le pipeline s’arrête immédiatement
- L’erreur est journalisée
- Le statut du run passe en "Failed"

Cette logique garantit la cohérence des données analytiques.

---

## 📊 Monitoring et Observabilité

Chaque exécution fournit :

- Logs détaillés par tâche
- Durée d’exécution
- Statut global
- Visualisation des dépendances

Le suivi est centralisé dans l’interface Databricks.

---

## 💰 Optimisation des Ressources

Pour maîtriser les coûts cloud :

- Le cluster démarre uniquement lors du run
- Auto-termination activée
- Aucun cluster permanent

Cette configuration est adaptée aux pipelines batch analytiques.

---

## 🔐 Bonnes Pratiques Appliquées

- Architecture Medallion respectée
- Format Delta Lake (ACID, performance)
- Séparation claire des responsabilités
- Orchestration centralisée
- Planification automatisée

---

## 🚀 Évolutions Possibles

- Alertes automatiques en cas d’échec
- Intégration CI/CD (Azure DevOps)
- Tests de qualité des données
- Intégration Power BI / Databricks SQL
