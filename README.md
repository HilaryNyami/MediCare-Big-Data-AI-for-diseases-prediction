# 🏥 MedPredict — Détection de Maladies par Big Data

**Projet INF438 — Thème 3 : Données Médicales Distribuées**

## 📊 Dataset
- **1 017 286** patients
- **669** symptômes binaires (features)
- **132** maladies à prédire (classification multi-classes)
- Fichier : `df_final.csv` (~1.3 GB)

## 🏗️ Architecture

```
df_final.csv  →  [Kafka]  →  [Spark/Flink]  →  [MongoDB]  →  [MLlib/TF]  →  [Streamlit]
```

## 🗂️ Structure du Projet

```
sante/
├── databricks/
│   ├── 01_chargement_donnees.py    ← Upload CSV + exploration Spark
│   ├── 02_traitement_spark.py      ← Nettoyage + VectorAssembler
│   ├── 03_modele_mllib.py          ← Random Forest MLlib + métriques
│   └── 04_modele_tensorflow.py     ← TensorFlow MLP + comparaison
│
├── kafka/
│   ├── producer.py                 ← Simulation flux données patients
│   └── consumer.py                 ← Traitement + stockage MongoDB
│
├── streamlit_app/
│   ├── app.py                      ← Page d'accueil
│   ├── utils.py                    ← Fonctions partagées
│   ├── pages/
│   │   ├── 1_carte.py             ← Carte OpenStreetMap interactive
│   │   ├── 2_simulation.py        ← Prédiction patient
│   │   ├── 3_analyse.py           ← Analyse exploratoire
│   │   └── 4_modele.py            ← Performance des modèles
│   ├── model/                      ← Modèle exporté depuis Databricks
│   └── requirements.txt
│
├── df_final.csv                    ← Dataset principal
└── README.md
```

## 🚀 Étapes d'Exécution

### 1. Databricks (entraînement ML)

1. Aller sur [Databricks](https://dbc-2334fa60-0480.cloud.databricks.com)
2. **Catalog > Add Data > Upload Files** → uploader `df_final.csv`
3. Créer 4 notebooks et copier le code de chaque fichier `databricks/0X_*.py`
4. Exécuter dans l'ordre : 01 → 02 → 03 → 04

### 2. Dashboard Streamlit (démo locale)

```bash
cd streamlit_app
pip install -r requirements.txt
streamlit run app.py
```

### 3. Simulation Kafka

```bash
cd kafka
python producer.py   # Génère kafka_messages.json
python consumer.py   # Traite les messages
```

## 🛠️ Technologies

| Catégorie | Outil |
|-----------|-------|
| Collecte | Apache Kafka (simulé) |
| Traitement | Apache Spark (PySpark) |
| Streaming | Apache Flink (simulé) |
| Stockage | MongoDB Atlas + Delta Lake |
| ML | MLlib + TensorFlow + Scikit-learn |
| Dashboard | Streamlit + Folium + Plotly |
| Carte | OpenStreetMap |

## 📈 Résultats Modèles

| Modèle | Accuracy | Dataset |
|--------|----------|---------|
| Random Forest (MLlib) | ~91% | ~800K patients |
| TensorFlow MLP | ~87% | 100K patients |
| Scikit-learn RF | ~89% | ~800K patients |
