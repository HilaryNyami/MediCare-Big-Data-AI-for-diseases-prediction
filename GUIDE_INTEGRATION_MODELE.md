# 🩺 Guide d'Intégration — MediPredict AI

## Pour le Chef de Groupe / Responsable Modèle

---

## 🏗️ Architecture du Projet

```
sante/
├── databricks/                        ← Notebooks pour Databricks (5 notebooks)
│   ├── 01_chargement_donnees.py       → Charger CSV → Delta
│   ├── 02_traitement_spark.py         → Nettoyer, encoder, split
│   ├── 03_modele_mllib.py            → Random Forest MLlib
│   ├── 04_modele_tensorflow.py        → TensorFlow MLP
│   └── 05_export_model_streamlit.py   → ⭐ Export .pkl pour le dashboard
│
├── kafka/                             ← Simulation Kafka (lancer sur PC)
│   ├── producer.py                    → Simule envoi de données patients
│   └── consumer.py                    → Traite et stocke dans MongoDB
│
├── streamlit_app/                     ← Dashboard (l'interface)
│   ├── Accueil.py                     → Page principale
│   ├── utils.py                       → Fonctions utilitaires
│   ├── requirements.txt               → Dépendances Python
│   ├── model/                         → ⭐ METTRE LE MODÈLE ICI
│   │   └── model_trained.pkl          → (à générer par Notebook 05)
│   ├── .streamlit/                    → Config
│   └── pages/                         → Pages du dashboard
│       ├── 1__Carte_Épidémiologique.py
│       ├── 2__Diagnostic_IA.py
│       ├── 3_Analyse_des_Donnees.py
│       └── 4_Performance_Modeles.py
│
├── df_final.csv                       ← Dataset (1M patients, 669 symptômes)
└── GUIDE_INTEGRATION_MODELE.md        ← CE FICHIER
```

---

## 📋 Chronologie d'Exécution (DANS CET ORDRE)

### Sur Databricks (les 5 notebooks) :

| # | Notebook | Ce qu'il fait | Résultat |
|---|----------|--------------|----------|
| 1 | `01_chargement_donnees.py` | Upload CSV → table Delta `medical_raw` | Table Delta |
| 2 | `02_traitement_spark.py` | Nettoyage, encodage, split 80/20 | `medical_train`, `medical_test` |
| 3 | `03_modele_mllib.py` | Random Forest MLlib (distribué) | Métriques + modèle Spark |
| 4 | `04_modele_tensorflow.py` | TensorFlow MLP (Deep Learning) | Métriques + modèle TF |
| 5 | `05_export_model_streamlit.py` | ⭐ Export Scikit-learn → `.pkl` | **model_trained.pkl** |

### Sur PC (après Databricks) :

| # | Action | Commande |
|---|--------|----------|
| 6 | Télécharger `model_trained.pkl` | Depuis FileStore Databricks |
| 7 | Placer dans le bon dossier | `streamlit_app/model/model_trained.pkl` |
| 8 | Installer les dépendances | `pip install -r streamlit_app/requirements.txt` |
| 9 | Lancer le dashboard | `streamlit run streamlit_app/Accueil.py` |
| 10 | Lancer Kafka | `python kafka/producer.py` puis `python kafka/consumer.py` |

---

## ⭐ INTÉGRATION DU MODÈLE (le plus important)

### Ce que le dashboard attend :

| Paramètre | Valeur requise |
|-----------|---------------|
| **Fichier** | `model_trained.pkl` |
| **Emplacement** | `sante/streamlit_app/model/model_trained.pkl` |
| **Format** | Pickle (Scikit-learn) |
| **Input** | Vecteur de 669 valeurs binaires (0 ou 1) |
| **Output** | Nom de maladie (string) + probabilités |
| **Méthodes requises** | `.predict()`, `.predict_proba()`, `.classes_` |

### Le Notebook 05 fait tout ça automatiquement !

Il suffit d'exécuter `05_export_model_streamlit.py` sur Databricks après les notebooks 01-04 :
1. Il charge les données prétraitées
2. Entraîne un Random Forest Scikit-learn
3. Sauvegarde `model_trained.pkl` dans FileStore
4. Vous n'avez qu'à télécharger et placer dans `streamlit_app/model/`

### Téléchargement depuis Databricks :
```
URL : https://<votre-workspace>.databricks.com/files/models/model_trained.pkl
Ou : Catalog → FileStore → models → model_trained.pkl
```

---

## 🔑 Format du modèle exporté (pour comprendre)

Le Notebook 05 sauvegarde un dictionnaire :
```python
{
    "model": rf_sklearn,              # L'objet RandomForest entraîné
    "symptom_columns": [...],         # Liste des 669 noms de symptômes
    "classes": [...],                 # Liste des 132 noms de maladies
    "accuracy": 0.91,                 # Score obtenu
    "f1_score": 0.90,
    "num_features": 669,
    "num_classes": 132,
    "training_samples": 200000
}
```

---

## 🚀 Lancer le Dashboard

```bash
# 1. Aller dans le dossier
cd sante/streamlit_app

# 2. Installer les packages (une seule fois)
pip install -r requirements.txt

# 3. Lancer
streamlit run Accueil.py
```

Le dashboard s'ouvre sur http://localhost:8501

### Pages disponibles :
- **🏠 Accueil** — Vue d'ensemble + KPIs + Architecture
- **🗺️ Carte Épidémiologique** — Carte OpenStreetMap interactive
- **🔬 Diagnostic IA** — ⭐ Utilise le modèle pour prédire
- **📊 Analyse des Données** — Statistiques exploratoires
- **🤖 Performance Modèles** — Comparaison des 3 algorithmes

### Vérifier que le modèle est bien connecté :
- Page "Diagnostic IA" → badge vert "Modèle Actif" (au lieu de jaune "Mode Démo")

---

## 🐛 Problèmes courants

| Problème | Solution |
|----------|----------|
| Badge jaune "Mode Démo" | Le fichier `model_trained.pkl` n'est pas dans `streamlit_app/model/` |
| Erreur dimension | Le modèle n'a pas été entraîné sur les 669 features du CSV |
| Maladies inconnues | Le modèle doit prédire les NOMS de maladies (pas des indices) |
| Import error | Lancer `pip install -r requirements.txt` |

---

## 📡 Kafka (Simulation Streaming)

Pour démontrer la partie Big Data temps réel :

```bash
# Terminal 1 : Producer (simule les flux de données patients)
python kafka/producer.py

# Terminal 2 : Consumer (traite et stocke)
python kafka/consumer.py
```

Pour MongoDB Atlas : modifier l'URI dans `kafka/consumer.py` ligne 13.

---

## ✅ Résumé pour la présentation au prof

1. **Montrer Databricks** → exécution des 5 notebooks (chargement → entraînement)
2. **Montrer Kafka** → producer envoie, consumer traite
3. **Montrer le Dashboard** → navigation dans les 5 pages
4. **Tester la prédiction** → cocher des symptômes → résultat IA
5. **Expliquer l'architecture** → page Accueil du dashboard
