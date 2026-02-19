"""
modele_prediction.py — Régression logistique pour prédire la non-pertinence ECBU.

Entraîne un pipeline scikit-learn sur les données de v_algo_avicenne
et le sérialise dans modele_np.pkl.

Utilisation :
    python modele_prediction.py              # entraîne et sauvegarde
    python modele_prediction.py --evaluate   # affiche AUC, rapport, top features
"""

import argparse
import os
import pickle
import sys

import pandas as pd

try:
    from sklearn.compose import ColumnTransformer
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler
except ImportError:
    print("scikit-learn requis : pip install scikit-learn>=1.4")
    sys.exit(1)

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH   = os.path.join(SCRIPT_DIR, "modele_np.pkl")

COL_DEC      = "Décision Algorithme"
FEATURES_NUM = ["Age", "Leucocyturie", "Bactériurie UFC/mL"]
FEATURES_BIN = [
    "Symptomatique_bin", "Sondé_bin", "Immunodéprimé_bin",
    "Enceinte_bin", "ATB_bin", "Sexe_bin",
]
FEATURES_CAT = ["Service", "Mode Prélèvement"]


# =============================================================================
# Préparation des données
# =============================================================================

def preparer_features(df: pd.DataFrame):
    """
    Transforme un DataFrame issu de v_algo_avicenne en (X, y, feature_names).

    Cible y : 1 = NÉGATIF ou REJET (non-pertinent), 0 = pertinent.

    Returns:
        (X : DataFrame, y : Series, colonnes : list[str])
    """
    df_work = df.copy()
    y = df_work[COL_DEC].str.contains("NÉGATIF|REJET", case=False, na=False).astype(int)

    # Variables binaires à partir des colonnes textuelles de la vue
    df_work["Symptomatique_bin"] = (df_work.get("Symptomatique",  "") == "Oui").astype(int)
    df_work["Sondé_bin"]         = (df_work.get("Sondé",          "") == "Oui").astype(int)
    df_work["Immunodéprimé_bin"] = (df_work.get("Immunodéprimé",  "") == "Oui").astype(int)
    df_work["Enceinte_bin"]      = (df_work.get("Enceinte",       "") == "Oui").astype(int)
    df_work["ATB_bin"]           = (df_work.get("ATB en cours",   "") == "Oui").astype(int)
    df_work["Sexe_bin"]          = (df_work.get("Sexe",           "") == "Homme").astype(int)

    for col in FEATURES_NUM:
        if col in df_work.columns:
            df_work[col] = pd.to_numeric(df_work[col], errors="coerce").fillna(0)

    colonnes = (
        [c for c in FEATURES_NUM if c in df_work.columns]
        + [c for c in FEATURES_BIN if c in df_work.columns]
        + [c for c in FEATURES_CAT if c in df_work.columns]
    )
    return df_work[colonnes], y, colonnes


# =============================================================================
# Pipeline scikit-learn
# =============================================================================

def construire_pipeline(colonnes: list[str]) -> Pipeline:
    num_cols = [c for c in colonnes if c in FEATURES_NUM + FEATURES_BIN]
    cat_cols = [c for c in colonnes if c in FEATURES_CAT]
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), num_cols),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_cols),
        ],
        remainder="drop",
    )
    return Pipeline([
        ("prep", preprocessor),
        ("clf",  LogisticRegression(C=1.0, max_iter=1000, random_state=42)),
    ])


# =============================================================================
# Entraînement
# =============================================================================

def entrainer_modele(df: pd.DataFrame):
    """
    Entraîne le modèle et retourne (pipeline, X_test, y_test, colonnes).

    Raises:
        ValueError : si df a moins de 20 lignes.
    """
    if len(df) < 20:
        raise ValueError(f"Données insuffisantes ({len(df)} lignes, minimum 20).")

    X, y, colonnes = preparer_features(df)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y,
    )
    pipeline = construire_pipeline(colonnes)
    pipeline.fit(X_train, y_train)
    return pipeline, X_test, y_test, colonnes


# =============================================================================
# Évaluation
# =============================================================================

def evaluer_modele(pipeline, X_test, y_test) -> dict:
    """Retourne AUC, accuracy, rapport de classification et matrice de confusion."""
    y_pred  = pipeline.predict(X_test)
    y_proba = pipeline.predict_proba(X_test)[:, 1]
    return {
        "auc":      round(float(roc_auc_score(y_test, y_proba)), 4),
        "accuracy": round(float((y_pred == y_test).mean()), 4),
        "rapport":  classification_report(
            y_test, y_pred, target_names=["Pertinent", "Non pertinent"]
        ),
        "cm": confusion_matrix(y_test, y_pred).tolist(),
    }


def coefficients_modele(pipeline, colonnes: list[str]) -> pd.DataFrame:
    """DataFrame (Feature, Coefficient) trié par valeur absolue décroissante."""
    prep     = pipeline.named_steps["prep"]
    clf      = pipeline.named_steps["clf"]
    num_cols = [c for c in colonnes if c in FEATURES_NUM + FEATURES_BIN]
    cat_cols = [c for c in colonnes if c in FEATURES_CAT]
    ohe_feats = (
        list(prep.named_transformers_["cat"].get_feature_names_out(cat_cols))
        if cat_cols else []
    )
    return (
        pd.DataFrame({"Feature": num_cols + ohe_feats, "Coefficient": clf.coef_[0]})
        .sort_values("Coefficient", key=abs, ascending=False)
    )


# =============================================================================
# Sérialisation
# =============================================================================

def sauvegarder_modele(pipeline, path: str = MODEL_PATH):
    with open(path, "wb") as f:
        pickle.dump(pipeline, f)
    print(f"Modèle sauvegardé : {path}")


def charger_modele(path: str = MODEL_PATH):
    with open(path, "rb") as f:
        return pickle.load(f)


# =============================================================================
# Prédiction individuelle
# =============================================================================

def predire_np(pipeline, patient: dict) -> dict:
    """
    Prédit la non-pertinence d'un ECBU individuel.

    Args:
        patient : dict avec les mêmes clés que le DataFrame d'entraînement.

    Returns:
        dict {probabilite_np, prediction, label}.
    """
    df_p = pd.DataFrame([patient])
    df_p["Symptomatique_bin"] = int(patient.get("Symptomatique") == "Oui")
    df_p["Sondé_bin"]         = int(patient.get("Sondé")         == "Oui")
    df_p["Immunodéprimé_bin"] = int(patient.get("Immunodéprimé") == "Oui")
    df_p["Enceinte_bin"]      = int(patient.get("Enceinte")      == "Oui")
    df_p["ATB_bin"]           = int(patient.get("ATB en cours")  == "Oui")
    df_p["Sexe_bin"]          = int(patient.get("Sexe")          == "Homme")
    for col in FEATURES_NUM:
        if col in df_p.columns:
            df_p[col] = pd.to_numeric(df_p[col], errors="coerce").fillna(0)
    proba = float(pipeline.predict_proba(df_p)[0][1])
    pred  = int(proba >= 0.5)
    return {
        "probabilite_np": round(proba, 4),
        "prediction":     pred,
        "label":          "Non pertinent" if pred else "Pertinent",
    }


# =============================================================================
# Point d'entrée
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Entraîne le modèle de prédiction NP ECBU",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--evaluate", action="store_true",
                        help="Affiche AUC, rapport de classification et top features")
    args = parser.parse_args()

    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(SCRIPT_DIR, ".env"))
    except ImportError:
        pass

    from sqlalchemy import create_engine
    db_user = os.getenv("DB_USER", "postgres")
    db_pass = os.getenv("DB_PASS", "")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")

    if not db_pass:
        print("DB_PASS non défini. Vérifiez votre .env.")
        sys.exit(1)

    engine = create_engine(
        f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    )
    print("Chargement des données depuis v_algo_avicenne...")
    df = pd.read_sql("SELECT * FROM v_algo_avicenne", engine)
    print(f"  {len(df)} lignes chargées")

    print("Entraînement du modèle...")
    pipeline, X_test, y_test, colonnes = entrainer_modele(df)

    if args.evaluate:
        m = evaluer_modele(pipeline, X_test, y_test)
        print(f"\nAUC-ROC  : {m['auc']}")
        print(f"Accuracy : {m['accuracy']}")
        print(f"\nRapport :\n{m['rapport']}")
        print(f"Matrice de confusion : {m['cm']}")
        print("\nTop 10 features :")
        print(coefficients_modele(pipeline, colonnes).head(10).to_string(index=False))

    sauvegarder_modele(pipeline)
