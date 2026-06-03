# 🏥 MedPredict — Détection de Maladies par Big Data

**Projet INF438 — Thème 3 : Données Médicales Distribuées**

## 📊 Dataset
- **1 017 286** patients
- **669** symptômes binaires (features)
- **132** maladies à prédire (classification multi-classes)
- Fichier : `df_final.csv` (~1.3 GB)

## 🏗️ Architecture

```
df_final.csv  →  [Spark]  →  [MongoDB]  →  [MLlib/TF]  →  [Streamlit]
```

## 🗂️ Structure du Projet

```
sante/
├── Pyspark/
│   ├── 01_chargement_donnees.py    ← Upload CSV + exploration Spark
│   ├── 02_traitement_spark.py      ← Nettoyage + VectorAssembler
│   ├── 03_modele_mllib.py          ← Random Forest MLlib + métriques
│   └── 04_modele_XGBoost.py        ← XGBoost MLP + comparaison
│
├── kafka/
│   └── consumer.py                 ← Traitement + stockage MongoDB
│
├── streamlit_app/
│   ├── Home.py                      ← Page d'accueil
│   ├── utils.py                    ← Fonctions partagées
│   ├── pages/
│   │   ├── 1_Diagnostic.py           ← Prédiction patient
│   │   ├── 2_Data_Analytical.py        ← Analyse exploratoire
│   │   └── 4_Model_performance.py            ← Performance des modèles
│   │   ├── 1_carte.py             ← Carte OpenStreetMap interactive
│   ├── model/                      ← Modèle exporté depuis Spark(local)
│   └── requirements.txt
│
├── df_final.csv                    ← Dataset principal
└── README.md
```

## 🚀 Étapes d'Exécution

### 1. Spark(local) (entraînement ML)

Modele deja sauvegarde: model/best_model_diagnostic.plk

### 2. Dashboard Streamlit (démo locale)

```bash
cd streamlit_app
pip install -r requirements.txt
streamlit run app.py
```

### 3. Simulation Kafka (en cours de d'implementation)

```bash
cd kafka
python producer.py   # Génère kafka_messages.json
python consumer.py   # Traite les messages
```

## 🛠️ Technologies

| Catégorie | Outil |
|-----------|-------|
| Collecte | Apache Kafka (simulé, non operationnel) |
| Traitement | Apache Spark (PySpark) |
| Streaming | Apache Flink (simulé, non operationnel) |
| Stockage | MongoDB Atlas|
| ML | MLlib + XGBoost + Scikit-learn |
| Dashboard | Streamlit + Folium + Plotly |
| Carte | OpenStreetMap |

## 📈 Résultats Modèles

| Modèle | Accuracy | Dataset |
|--------|----------|---------|
| Random Forest (MLlib) | ~91, 4% | ~1M patients |
| XGBoost MLP | ~94,4% | ~1M patients |
| Scikit-learn RF | ~94,1% | ~1M patients |
