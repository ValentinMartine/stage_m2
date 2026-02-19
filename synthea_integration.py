"""
Intégration des données démographiques Synthea.

Synthea est un générateur open-source de patients synthétiques (MITRE Corporation).
Il produit des historiques médicaux complets et réalistes, libres de droits.
Documentation : https://synthetichealth.github.io/synthea/

Ce module extrait uniquement les données démographiques (date de naissance, sexe)
pour enrichir le générateur ECBU avec des profils plus vraisemblables.

Usage rapide :
    from synthea_integration import charger_patients_synthea, mapper_demographics

    df = charger_patients_synthea("patients.csv")
    demo = mapper_demographics(df.iloc[0])
    # → {'ddn': datetime(...), 'sexe': 2, 'cat_age': 'adulte'}
"""

import io
import urllib.request
from datetime import datetime

import pandas as pd

# ---------------------------------------------------------------------------
# URLs publiques de données Synthea pré-générées (fallback ordonné)
# ---------------------------------------------------------------------------
SYNTHEA_SAMPLE_URLS = [
    (
        "https://raw.githubusercontent.com/synthetichealth/synthea-sample-data"
        "/master/csv/10k_synthea_covid19_csv/patients.csv"
    ),
]

_COLONNES_REQUISES = ["BIRTHDATE", "GENDER"]


# ---------------------------------------------------------------------------
# Fonctions internes
# ---------------------------------------------------------------------------

def _preparer_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Valide, nettoie et normalise le DataFrame Synthea."""
    manquantes = [c for c in _COLONNES_REQUISES if c not in df.columns]
    if manquantes:
        raise ValueError(f"Colonnes Synthea manquantes : {manquantes}")

    df = df[_COLONNES_REQUISES].copy()
    df["BIRTHDATE"] = pd.to_datetime(df["BIRTHDATE"], errors="coerce")
    df["GENDER"] = df["GENDER"].str.upper().str.strip()
    df = df[df["GENDER"].isin(["M", "F"])].dropna(subset=["BIRTHDATE"])
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# API publique
# ---------------------------------------------------------------------------

def charger_patients_synthea(chemin: str) -> pd.DataFrame:
    """
    Charge un fichier patients.csv Synthea depuis le disque local.

    Args:
        chemin : Chemin vers le fichier CSV (ex: '/data/patients.csv').

    Returns:
        DataFrame avec colonnes BIRTHDATE (datetime64) et GENDER ('M' ou 'F').

    Raises:
        FileNotFoundError : si le fichier n'existe pas.
        ValueError        : si les colonnes requises sont absentes.
    """
    df = pd.read_csv(chemin, low_memory=False)
    df = _preparer_dataframe(df)
    print(f"[Synthea] {len(df)} patients chargés depuis {chemin}")
    return df


def telecharger_patients_synthea(url: str = None, n_max: int = 2000) -> pd.DataFrame | None:
    """
    Télécharge des données Synthea depuis une URL publique.

    Si l'URL fournie échoue, tente les URLs de secours intégrées.
    Retourne None si toutes les tentatives échouent (le générateur
    basculera automatiquement sur ses profils internes).

    Args:
        url   : URL custom (optionnel). Utilise SYNTHEA_SAMPLE_URLS sinon.
        n_max : Nombre maximum de patients à charger (défaut : 2000).

    Returns:
        DataFrame ou None en cas d'échec total.
    """
    urls = [url] if url else SYNTHEA_SAMPLE_URLS
    for u in urls:
        try:
            print(f"[Synthea] Téléchargement depuis {u} ...")
            req = urllib.request.Request(u, headers={"User-Agent": "stage_m2/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                content = resp.read().decode("utf-8")
            df = _preparer_dataframe(pd.read_csv(io.StringIO(content), low_memory=False))
            df = df.head(n_max)
            print(f"[Synthea] {len(df)} patients chargés.")
            return df
        except Exception as exc:
            print(f"[Synthea] Echec ({u}) : {exc}")

    print("[Synthea] Toutes les URLs ont echoue — profils internes utilises.")
    return None


def mapper_demographics(patient: pd.Series) -> dict:
    """
    Mappe un patient Synthea vers les champs démographiques du générateur ECBU.

    Args:
        patient : Ligne d'un DataFrame Synthea (colonnes BIRTHDATE, GENDER).

    Returns:
        dict avec :
            'ddn'     : datetime — date de naissance
            'sexe'    : int — 1 = Homme, 2 = Femme
            'cat_age' : str — 'bebe' | 'enfant' | 'adulte' | 'vieux'
    """
    today = datetime.now()
    ddn = patient["BIRTHDATE"]
    if hasattr(ddn, "to_pydatetime"):
        ddn = ddn.to_pydatetime()

    age_ans = (today - ddn).days // 365

    if age_ans <= 2:
        cat_age = "bebe"
    elif age_ans <= 17:
        cat_age = "enfant"
    elif age_ans <= 74:
        cat_age = "adulte"
    else:
        cat_age = "vieux"

    return {
        "ddn": ddn,
        "sexe": 1 if patient["GENDER"] == "M" else 2,
        "cat_age": cat_age,
    }
