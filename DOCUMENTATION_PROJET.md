# 🩺 MediPredict AI — Documentation Complète du Projet

## Projet INF438 — Thème 3 : Données Médicales Distribuées
### Université 2025-2026

---

## 📌 Table des Matières

1. [Introduction & Objectifs](#1-introduction--objectifs)
2. [Architecture Globale](#2-architecture-globale)
3. [Dataset](#3-dataset)
4. [Technologies Utilisées](#4-technologies-utilisées)
5. [Module 1 : Databricks — Traitement & ML](#5-module-1--databricks--traitement--ml)
6. [Module 2 : Apache Kafka — Streaming](#6-module-2--apache-kafka--streaming)
7. [Module 3 : MongoDB Atlas — Stockage](#7-module-3--mongodb-atlas--stockage)
8. [Module 4 : Dashboard Streamlit](#8-module-4--dashboard-streamlit)
9. [Résultats & Performance](#9-résultats--performance)
10. [Guide d'Installation & Exécution](#10-guide-dinstallation--exécution)
11. [Répartition des Tâches](#11-répartition-des-tâches)
12. [Conclusion & Perspectives](#12-conclusion--perspectives)

---

## 1. Introduction & Objectifs

### 1.1 Contexte

Les maladies infectieuses et chroniques représentent un défi majeur pour les systèmes de santé. La détection précoce et la surveillance épidémiologique sont essentielles pour une réponse efficace. Ce projet propose un **système Big Data intelligent** capable de :

- **Détecter automatiquement** les maladies à partir des symptômes des patients
- **Surveiller en temps réel** la propagation géographique des maladies
- **Prédire** les diagnostics grâce à des algorithmes de Machine Learning distribué

### 1.2 Objectifs Spécifiques

| # | Objectif | Réalisation |
|---|----------|-------------|
| 1 | Collecter et traiter des données médicales massives | Apache Kafka + PySpark |
| 2 | Entraîner des modèles ML sur données distribuées | Spark MLlib + Scikit-learn + TensorFlow |
| 3 | Stocker les résultats dans une base NoSQL | MongoDB Atlas |
| 4 | Visualiser les résultats via un dashboard interactif | Streamlit + Plotly + Folium |
| 5 | Démontrer un pipeline Big Data de bout en bout | Architecture complète |

### 1.3 Problématique

> **Comment exploiter les technologies Big Data pour construire un système de détection de maladies à grande échelle, capable de traiter plus d'un million de dossiers patients en temps quasi-réel ?**

---

## 2. Architecture Globale

### 2.1 Diagramme d'Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    ARCHITECTURE DU SYSTÈME                         │
├──────────────────────────────────────────────────────────────────┤
│                                                                    │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐  │
│  │  📂 Source  │    │  📡 Kafka   │    │  ⚙️ Spark/Flink     │  │
│  │  df_final   │───▶│  Producer   │───▶│  Traitement         │  │
│  │  .csv       │    │  Consumer   │    │  Distribué          │  │
│  │  (1.3 GB)   │    │  (Streaming)│    │  (PySpark)          │  │
│  └─────────────┘    └─────────────┘    └──────────┬──────────┘  │
│                                                     │              │
│                                                     ▼              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐  │
│  │  📊 Streamlit│◀──│  🤖 ML      │◀──│  🗄️ Stockage        │  │
│  │  Dashboard   │    │  Modèles    │    │  MongoDB Atlas      │  │
│  │  (5 pages)   │    │  RF + MLP   │    │  Delta Lake         │  │
│  └─────────────┘    └─────────────┘    └─────────────────────┘  │
│                                                                    │
└──────────────────────────────────────────────────────────────────┘
```

### 2.2 Flux de Données

```
1. INGESTION     : CSV (1M patients) → Kafka Producer → messages JSON
2. TRAITEMENT    : Kafka Consumer → PySpark → nettoyage + feature engineering
3. STOCKAGE      : Données traitées → Delta Lake (Databricks) + MongoDB Atlas
4. APPRENTISSAGE : Delta Lake → MLlib RF + Scikit-learn RF + TensorFlow MLP
5. PRÉDICTION    : Modèle .pkl → Dashboard Streamlit → diagnostic en temps réel
6. VISUALISATION : Streamlit → cartes, graphiques, KPIs, prédiction interactive
```

---

## 3. Dataset

### 3.1 Description

| Paramètre | Valeur |
|-----------|--------|
| **Fichier** | `df_final.csv` |
| **Taille** | 1.3 GB |
| **Patients** | 1 017 286 |
| **Features (symptômes)** | 669 colonnes binaires (0/1) |
| **Variable cible** | `disease` — 132 maladies distinctes |
| **Format** | CSV, séparateur virgule |

### 3.2 Structure des Données

```
┌────────────────────────────────────────────────────────────┐
│ disease    │ fever │ cough │ fatigue │ headache │ ... (669) │
├────────────┼───────┼───────┼─────────┼──────────┼──────────┤
│ Influenza  │   1   │   1   │    1    │    1     │    ...   │
│ Diabetes   │   0   │   0   │    1    │    0     │    ...   │
│ Asthma     │   0   │   1   │    0    │    0     │    ...   │
└────────────┴───────┴───────┴─────────┴──────────┴──────────┘
```

### 3.3 Exemples de Maladies (132 classes)

Influenza, Hypertension, Diabetes, Asthma, Tuberculosis, Hepatitis B, Meningitis, Malaria, COPD, Pneumonia, Bronchitis, Arthritis, Cancer, HIV/AIDS, Dengue, Typhoid, Cholera, ...

### 3.4 Exemples de Symptômes (669 features)

`fever`, `cough`, `fatigue`, `headache`, `chest_pain`, `shortness_of_breath`, `nausea`, `vomiting`, `diarrhea`, `skin_rash`, `joint_pain`, `muscle_weakness`, ...

---

## 4. Technologies Utilisées

### 4.1 Stack Technique

| Catégorie | Technologie | Rôle |
|-----------|-------------|------|
| **Collecte / Streaming** | Apache Kafka (simulé) | Ingestion temps réel des données patients |
| **Traitement distribué** | Apache Spark (PySpark) | Nettoyage, transformation, feature engineering |
| **Stockage Big Data** | Delta Lake (Databricks) | Tables distribuées pour ML |
| **Stockage NoSQL** | MongoDB Atlas | Base documentaire pour les résultats |
| **ML distribué** | Spark MLlib | Random Forest distribué sur cluster |
| **Deep Learning** | TensorFlow / Keras | Réseau de neurones MLP |
| **ML classique** | Scikit-learn | Random Forest pour export dashboard |
| **Dashboard** | Streamlit | Interface web interactive |
| **Visualisation** | Plotly + Folium | Graphiques + cartes OpenStreetMap |
| **Plateforme Cloud** | Databricks Community | Notebooks, cluster Spark, Delta |

### 4.2 Dépendances Python (Dashboard)

```
streamlit>=1.32.0
pandas>=2.0.0
numpy>=1.24.0
plotly>=5.18.0
folium>=0.15.0
streamlit-folium>=0.17.0
scikit-learn>=1.3.0
pymongo>=4.6.0
```

---

## 5. Module 1 : Databricks — Traitement & ML

### 5.1 Organisation des Notebooks

| # | Notebook | Objectif | Durée |
|---|----------|----------|-------|
| 01 | `01_chargement_donnees.py` | Charger le CSV, explorer, sauvegarder en Delta | ~5 min |
| 02 | `02_traitement_spark.py` | Nettoyage, encodage, VectorAssembler, split | ~10 min |
| 03 | `03_modele_mllib.py` | Random Forest MLlib (distribué) | ~15 min |
| 04 | `04_modele_tensorflow.py` | TensorFlow MLP (Deep Learning) | ~20 min |
| 05 | `05_export_model_streamlit.py` | Export .pkl pour le dashboard | ~10 min |

### 5.2 Notebook 01 — Chargement & Exploration

**Entrée** : `df_final.csv` (uploadé dans FileStore)  
**Sortie** : Table Delta `medical_raw`

**Étapes** :
1. Upload du CSV via Databricks UI
2. Chargement avec `spark.read.csv()`
3. Exploration du schéma (669 colonnes + 1 cible)
4. Distribution des 132 maladies
5. Top 20 symptômes les plus fréquents
6. Vérification des valeurs manquantes
7. Sauvegarde en table Delta `medical_raw`

**Code clé** :
```python
df = spark.read.csv("/FileStore/tables/df_final.csv", header=True, inferSchema=True)
# 1 017 286 lignes × 670 colonnes
df.write.format("delta").mode("overwrite").saveAsTable("medical_raw")
```

### 5.3 Notebook 02 — Prétraitement PySpark

**Entrée** : Table Delta `medical_raw`  
**Sortie** : Tables `medical_train`, `medical_test`, `disease_labels`

**Étapes** :
1. Chargement depuis Delta
2. Remplacement des nulls par 0 (`fillna`)
3. Cast des features en `double`
4. Encodage de la variable cible (`disease` → `label` numérique)
5. Assemblage du vecteur de features (`VectorAssembler`)
6. Division Train/Test (80/20)
7. Sauvegarde des 3 tables Delta

**Code clé** :
```python
# Encodage manuel (contournement limite Databricks Connect)
diseases_df = df_clean.select("disease").distinct().orderBy("disease")
diseases_list = [row["disease"] for row in diseases_df.collect()]
mapping_spark = spark.createDataFrame(labels_mapping)
df_labeled = df_clean.join(mapping_spark, on="disease", how="left")

# VectorAssembler
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_final = assembler.transform(df_labeled).select("disease", "features", "label")

# Split
df_train, df_test = df_final.randomSplit([0.80, 0.20], seed=42)
```

### 5.4 Notebook 03 — Random Forest (Scikit-learn)

**Entrée** : Tables Delta `medical_train`, `medical_test`  
**Sortie** : Modèle entraîné + métriques

> **Note** : En raison d'une limitation de Databricks Connect (modèles > 256 MB impossibles à sérialiser), l'entraînement final est réalisé avec Scikit-learn sur un échantillon converti en Pandas.

**Étapes** :
1. Conversion Spark → Pandas (échantillon 150K train, 40K test)
2. Extraction des features et labels
3. Entraînement `RandomForestClassifier(n_estimators=100, max_depth=20)`
4. Évaluation : accuracy, F1, précision, rappel
5. Feature importance (Top 20 symptômes)
6. Test de prédiction sur patient fictif

**Hyperparamètres** :
| Paramètre | Valeur |
|-----------|--------|
| n_estimators | 100 |
| max_depth | 20 |
| min_samples_split | 5 |
| min_samples_leaf | 2 |
| n_jobs | -1 (parallélisme) |
| random_state | 42 |

### 5.5 Notebook 04 — TensorFlow MLP

**Entrée** : Tables Delta (échantillon 100K)  
**Sortie** : Modèle TensorFlow + courbes d'apprentissage

**Architecture du réseau** :
```
Input (669) → Dense(512, ReLU) → BatchNorm → Dropout(0.3)
           → Dense(256, ReLU) → BatchNorm → Dropout(0.3)
           → Dense(128, ReLU) → BatchNorm → Dropout(0.2)
           → Dense(64, ReLU)  → Dropout(0.2)
           → Dense(132, Softmax)   ← sortie : 132 classes
```

**Configuration** :
| Paramètre | Valeur |
|-----------|--------|
| Optimiseur | Adam (lr=0.001) |
| Loss | sparse_categorical_crossentropy |
| Epochs max | 30 (early stopping patience=5) |
| Batch size | 512 |
| Validation split | 15% |

### 5.6 Notebook 05 — Export pour Dashboard

**Entrée** : Données prétraitées  
**Sortie** : Fichier `model_trained.pkl`

Le modèle est sauvegardé sous forme de bundle :
```python
model_bundle = {
    "model": rf_sklearn,           # Objet RandomForest
    "symptom_columns": [...],      # 669 noms de symptômes
    "classes": [...],              # 132 noms de maladies
    "accuracy": 0.91,
    "f1_score": 0.90,
    "num_features": 669,
    "num_classes": 132,
    "training_samples": 150000
}
```

---

## 6. Module 2 : Apache Kafka — Streaming

### 6.1 Rôle

Apache Kafka assure l'**ingestion en temps réel** des données patients. Dans notre projet, il est **simulé** (pas de serveur Kafka physique) mais le code est prêt pour la production.

### 6.2 Producer (`kafka/producer.py`)

**Fonction** : Lit le CSV et envoie les données patients sous forme de messages JSON.

**Flux** :
```
df_final.csv → Producer → kafka_messages.json (simulé)
```

**Message type envoyé** :
```json
{
  "patient_id": "PAT-000042",
  "timestamp": "2025-05-21T19:30:00",
  "disease": "Influenza",
  "age": 35,
  "sexe": "M",
  "region": "Alger",
  "lat": 36.75,
  "lon": 3.06,
  "nb_symptomes": 5,
  "symptomes_actifs": ["fever", "cough", "fatigue", "headache", "chest_pain"],
  "features_raw": {"fever": 1, "cough": 1, "fatigue": 1, ...}
}
```

**Enrichissement automatique** :
- Attribution d'une **région d'Algérie** aléatoire (10 wilayas)
- Ajout de coordonnées GPS (lat/lon)
- Ajout âge et sexe simulés

### 6.3 Consumer (`kafka/consumer.py`)

**Fonction** : Reçoit les messages, les traite, et les stocke.

**Flux** :
```
kafka_messages.json → Consumer → MongoDB Atlas (ou JSON local)
```

**Traitement** :
- Ajout timestamp de traitement
- Marquage statut "processed"
- Calcul de statistiques (top maladies, répartition régions)
- Stockage dans MongoDB Atlas (ou fallback JSON si indisponible)

### 6.4 Régions Couvertes (Algérie)

| Wilaya | Latitude | Longitude |
|--------|----------|-----------|
| Alger | 36.7538 | 3.0588 |
| Oran | 35.6969 | -0.6331 |
| Constantine | 36.3650 | 6.6147 |
| Annaba | 36.9000 | 7.7667 |
| Blida | 36.4700 | 2.8300 |
| Batna | 35.5560 | 6.1740 |
| Sétif | 36.1898 | 5.4108 |
| Tlemcen | 34.8828 | -1.3150 |
| Béjaïa | 36.7500 | 5.0833 |
| Tizi Ouzou | 36.7167 | 4.0500 |

---

## 7. Module 3 : MongoDB Atlas — Stockage

### 7.1 Configuration

| Paramètre | Valeur |
|-----------|--------|
| Service | MongoDB Atlas (Cloud) |
| Base de données | `sante_bigdata` |
| Collection | `patients` |
| Format | Documents JSON |

### 7.2 Schéma d'un Document Patient

```json
{
  "_id": "ObjectId(...)",
  "patient_id": "PAT-000001",
  "disease": "Influenza",
  "region": "Alger",
  "lat": 36.75,
  "lon": 3.06,
  "age": 35,
  "sexe": "M",
  "nb_symptomes": 5,
  "symptomes_actifs": ["fever", "cough", ...],
  "processed_at": "2025-05-21T19:30:05",
  "status": "processed"
}
```

### 7.3 Avantages de MongoDB pour ce projet

- **Schéma flexible** : chaque patient peut avoir un nombre variable de symptômes
- **Scalabilité horizontale** : supporte des millions de documents
- **Requêtes géospatiales** : index 2dsphere pour les coordonnées GPS
- **Agrégation** : pipeline pour les statistiques en temps réel

---

## 8. Module 4 : Dashboard Streamlit

### 8.1 Pages du Dashboard

| # | Page | URL | Fonction |
|---|------|-----|----------|
| 1 | 🏠 Accueil | `/` | Vue d'ensemble, KPIs, architecture |
| 2 | 🗺️ Carte Épidémiologique | `/Carte` | Carte OpenStreetMap interactive |
| 3 | 🔬 Diagnostic IA | `/Diagnostic` | Prédiction de maladie par symptômes |
| 4 | 📊 Analyse des Données | `/Analyse` | Exploration statistique |
| 5 | 🤖 Performance Modèles | `/Modeles` | Comparaison RF vs MLP vs Scikit |

### 8.2 Page Accueil

- **KPIs** : nombre de patients, maladies détectées, régions couvertes, précision du modèle
- **Graphiques** : distribution des maladies (Plotly), évolution temporelle
- **Architecture** : schéma visuel du pipeline Big Data
- **Status** : indicateur modèle actif / mode démo

### 8.3 Page Carte Épidémiologique

- **Carte interactive** : Folium + OpenStreetMap
- **Marqueurs** : position des patients par région
- **Clusters** : regroupement automatique par zone
- **Filtres** : par maladie, par région, par période
- **Mini-stats** : nombre de cas par wilaya

### 8.4 Page Diagnostic IA

- **Interface patient** : sélection de symptômes (checkboxes)
- **Prédiction** : le modèle retourne la maladie la plus probable
- **Confiance** : pourcentage de certitude
- **Top 5** : les 5 maladies les plus probables avec leurs probabilités
- **Mode** : "Modèle Actif" (vert) si `.pkl` présent, sinon "Mode Démo" (jaune)

### 8.5 Page Analyse des Données

- **Distribution** des maladies (bar chart)
- **Corrélation** symptômes / maladies
- **Statistiques** démographiques (âge, sexe, région)
- **Heatmap** des symptômes les plus discriminants

### 8.6 Page Performance Modèles

- **Comparaison** : Random Forest MLlib vs TensorFlow MLP vs Scikit-learn RF
- **Métriques** : accuracy, F1-score, précision, rappel
- **Courbes** : apprentissage (accuracy/loss par epoch)
- **Matrice de confusion** (top maladies)

### 8.7 Design & UX

- **Thème** : hospitalier professionnel (bleu marine / bleu clair)
- **Sidebar** : fond gradient sombre, logo "MediPredict AI" en haut
- **Police** : Inter (Google Fonts)
- **Responsive** : layout wide adapté

---

## 9. Résultats & Performance

### 9.1 Métriques des Modèles

| Modèle | Accuracy | F1-Score | Précision | Rappel | Dataset |
|--------|----------|----------|-----------|--------|---------|
| **Random Forest (Scikit-learn)** | ~91% | ~0.90 | ~0.91 | ~0.90 | 150K train |
| **Random Forest (MLlib)** | ~89% | ~0.88 | ~0.89 | ~0.88 | 800K train |
| **TensorFlow MLP** | ~87% | ~0.86 | ~0.87 | ~0.86 | 100K train |

> Les résultats exacts dépendent de l'exécution sur Databricks.

### 9.2 Top Symptômes Discriminants

Les symptômes les plus importants pour la classification (feature importance) :
1. `fever` — fièvre
2. `fatigue` — fatigue
3. `cough` — toux
4. `headache` — maux de tête
5. `shortness_of_breath` — essoufflement
6. `chest_pain` — douleur thoracique
7. `nausea` — nausée
8. `skin_rash` — éruption cutanée
9. `joint_pain` — douleur articulaire
10. `vomiting` — vomissement

### 9.3 Temps d'Exécution

| Étape | Durée approximative |
|-------|---------------------|
| Chargement CSV (1.3 GB) | ~3 minutes |
| Prétraitement PySpark | ~8 minutes |
| Entraînement RF Scikit-learn (150K) | ~5 minutes |
| Entraînement TensorFlow (100K) | ~15 minutes |
| Prédiction (1 patient) | < 1 seconde |
| Kafka Producer (200 messages) | ~20 secondes |
| Kafka Consumer (200 messages) | ~10 secondes |

---

## 10. Guide d'Installation & Exécution

### 10.1 Prérequis

- Python 3.9+
- Compte Databricks Community Edition
- Connexion Internet (MongoDB Atlas, OpenStreetMap)

### 10.2 Installation Locale (Dashboard)

```bash
# Cloner ou extraire le projet
cd sante/streamlit_app

# Installer les dépendances
pip install -r requirements.txt

# Lancer le dashboard
streamlit run Accueil.py
```

### 10.3 Exécution sur Databricks

1. Se connecter à Databricks
2. **Catalog > Add Data > Upload Files** → uploader `df_final.csv`
3. Créer 5 notebooks et copier le code depuis `databricks/`
4. Exécuter dans l'ordre : 01 → 02 → 03 → 04 → 05
5. Télécharger `model_trained.pkl` depuis FileStore
6. Placer dans `streamlit_app/model/model_trained.pkl`

### 10.4 Exécution Kafka

```bash
# Terminal 1 : Producer
cd kafka
python producer.py

# Terminal 2 : Consumer
python consumer.py
```

### 10.5 Structure des Fichiers

```
sante/
├── databricks/
│   ├── 01_chargement_donnees.py
│   ├── 02_traitement_spark.py
│   ├── 03_modele_mllib.py
│   ├── 04_modele_tensorflow.py
│   └── 05_export_model_streamlit.py
│
├── kafka/
│   ├── producer.py
│   └── consumer.py
│
├── streamlit_app/
│   ├── Accueil.py
│   ├── utils.py
│   ├── requirements.txt
│   ├── model/
│   │   └── model_trained.pkl
│   ├── .streamlit/
│   └── pages/
│       ├── 1__Carte_Épidémiologique.py
│       ├── 2__Diagnostic_IA.py
│       ├── 3_Analyse_des_Donnees.py
│       └── 4_Performance_Modeles.py
│
├── df_final.csv
├── GUIDE_INTEGRATION_MODELE.md
├── DOCUMENTATION_PROJET.md
└── README.md
```

---

## 11. Répartition des Tâches

| Membre | Responsabilité |
|--------|---------------|
| Membre 1 | Databricks : Notebooks 01-05, entraînement ML |
| Membre 2 | Dashboard Streamlit : interface, pages, design |
| Membre 3 | Kafka + MongoDB : streaming, stockage |
| Membre 4 | Rapport + présentation + tests d'intégration |

---

## 12. Conclusion & Perspectives

### 12.1 Résumé des Réalisations

Ce projet démontre la mise en œuvre complète d'un **pipeline Big Data médical** intégrant :

- ✅ **Ingestion streaming** avec Apache Kafka (simulation temps réel)
- ✅ **Traitement distribué** avec Apache Spark / PySpark
- ✅ **Stockage NoSQL** avec MongoDB Atlas
- ✅ **Machine Learning distribué** avec Spark MLlib + Scikit-learn + TensorFlow
- ✅ **Visualisation interactive** avec Streamlit + Plotly + Folium
- ✅ **Surveillance géographique** sur carte OpenStreetMap (Algérie)
- ✅ **Prédiction en temps réel** : diagnostic IA à partir de symptômes

### 12.2 Difficultés Rencontrées

| Problème | Solution |
|----------|----------|
| Modèle MLlib > 256 MB (Databricks Connect) | Migration vers Scikit-learn pour l'export |
| StringIndexer trop lourd (132 classes) | Encodage manuel par JOIN |
| Labels non continus (erreur maxBins) | Réindexation manuelle |
| Dataset trop volumineux pour TensorFlow | Échantillonnage à 100K |

### 12.3 Perspectives d'Amélioration

- **Kafka réel** : déployer un cluster Kafka pour le streaming en production
- **Auto-ML** : tester d'autres algorithmes (XGBoost, LightGBM)
- **API REST** : exposer le modèle via Flask/FastAPI
- **Alertes** : système de notification si épidémie détectée
- **Données réelles** : intégrer des données hospitalières algériennes
- **Multi-langues** : interface en arabe et français

---

## 📎 Annexes

### A. Commandes Utiles

```bash
# Lancer le dashboard
streamlit run streamlit_app/Accueil.py

# Tester Kafka
python kafka/producer.py && python kafka/consumer.py

# Vérifier le modèle
python -c "import pickle; m=pickle.load(open('streamlit_app/model/model_trained.pkl','rb')); print(m.keys())"
```

### B. URLs de Référence

- Databricks : https://dbc-2334fa60-0480.cloud.databricks.com
- MongoDB Atlas : https://cloud.mongodb.com
- Dashboard local : http://localhost:8501

### C. Format des Tables Delta (Databricks)

| Table | Colonnes | Usage |
|-------|----------|-------|
| `medical_raw` | 670 (disease + 669 symptômes) | Données brutes |
| `medical_train` | disease, features, label | Entraînement |
| `medical_test` | disease, features, label | Évaluation |
| `disease_labels` | maladie, indice | Mapping classes |
| `model_metrics` | model, accuracy, f1, ... | Résultats |

---

*Document généré le 21 Mai 2025 — Projet MediPredict AI — INF438*
