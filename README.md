# Projet PFA

## 📌 Description
Ce dépôt contient deux projets réalisés dans le cadre du **Projet de Fin d’Année (PFA)** :

- **Projet 1 : Application PHP/JS**
  - Gestion de données avec PHP et JavaScript
  - Opérations CRUD (Create, Read, Delete)
  - Synchronisation avec **AWS DynamoDB**
  
- **Projet 2 : Data Pipeline**
  - Mise en place d’une architecture **Bronze – Silver – Gold**
  - Scripts Python pour le traitement des données
  - Utilisation de **Azure Synapse** pour la partie SQL
  - Production et ingestion de données via `producer.py`

---

## 🚀 Fonctionnalités

### Projet 1
- Ajouter de nouvelles entrées (`insert.php`)
- Lire les données (`read.php`, `read2.php`)
- Supprimer des enregistrements (`delete.php`)
- Synchronisation avec DynamoDB (`SyncFormsToDynamo.php`)
- Scripts front-end en JavaScript (`readRL.js`, `testee.js`)

### Projet 2
- Ingestion des données (Bronze layer)
- Nettoyage et transformation (Silver layer)
- Agrégation et enrichissement (Gold layer)
- Script producteur de données (`producer.py`)
- Requêtes SQL pour l’analyse dans Azure Synapse (`Synapse.sql`)

---

## 🛠️ Technologies utilisées
- **Projet 1 :**
  - PHP
  - JavaScript
  - MySQL / DynamoDB

- **Projet 2 :**
  - Python
  - Azure (Synapse, Event Hub, Databricks)
  - SQL

---

## 📂 Structure du dépôt
```
ProjetPFA/
│── Projet1/
│   ├── insert.php
│   ├── delete.php
│   ├── read.php
│   ├── read2.php
│   ├── SyncFormsToDynamo.php
│   ├── readRL.js
│   ├── testee.js
│
│── Projet2/
│   ├── BronzeLayer.py
│   ├── SilverLayer.py
│   ├── GoldLayer.py
│   ├── producer.py
│   ├── Synapse.sql
│
│── .git/
```

---

## ⚙️ Installation

### Projet 1 (PHP/JS)
1. Cloner le dépôt :
   ```bash
   git clone https://github.com/votre-utilisateur/ProjetPFA.git
   ```
2. Déplacer dans le dossier :
   ```bash
   cd ProjetPFA/Projet1
   ```
3. Configurer votre serveur local (XAMPP, WAMP ou autre) et placer les fichiers dans le répertoire `htdocs`.
4. Lancer l’application depuis `http://localhost/Projet1/`.

### Projet 2 (Pipeline de données)
1. Déplacer dans le dossier :
   ```bash
   cd ProjetPFA/Projet2
   ```
2. Lancer les scripts Python :
   ```bash
   python BronzeLayer.py
   python SilverLayer.py
   python GoldLayer.py
   ```
3. Exécuter les requêtes SQL depuis `Synapse.sql` dans **Azure Synapse**.

---

## 👨‍💻 Auteur
- **Aymen Boubetana**
