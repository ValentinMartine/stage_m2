"""
Script d'anonymisation des données ECBU — conforme RGPD / HDS

IMPORTANT — Conformité réglementaire
======================================
Ce script doit être exécuté LOCALEMENT sur une infrastructure HDS ou on-premise.
Il ne doit JAMAIS être exécuté dans GitHub Actions avec de vraies données patients.

Il lit les données individuelles depuis PostgreSQL et produit un fichier CSV
d'agrégats statistiques qui :
  - ne contient AUCUN identifiant (ni IPP, ni ID hashé)
  - ne permet PAS de ré-identifier un patient (k-anonymat ≥ 5)
  - peut être commité sur GitHub sans restriction RGPD

Workflow :
    1. python anonymiser_donnees.py           → produit donnees_anonymes.csv
    2. git add donnees_anonymes.csv
    3. git push                               → GitHub Actions peut l'utiliser

Utilisation :
    python anonymiser_donnees.py [--output chemin.csv] [--k-min N]
"""

import argparse
import os
import sys

import pandas as pd
from sqlalchemy import create_engine

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, ".env")

try:
    from dotenv import load_dotenv
    load_dotenv(ENV_PATH)
except ImportError:
    pass

DEFAULT_OUTPUT = os.path.join(SCRIPT_DIR, "donnees_anonymes.csv")
K_ANONYMAT_MIN = 5  # Un agrégat avec moins de N patients est supprimé


# =============================================================================
# Connexion base de données
# =============================================================================

def get_engine():
    db_user = os.getenv("DB_USER", "postgres")
    db_pass = os.getenv("DB_PASS", "")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    return create_engine(
        f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    )


# =============================================================================
# Fonctions de transformation
# =============================================================================

def age_vers_tranche(age: pd.Series) -> pd.Series:
    """Convertit un âge exact en tranche quinquennale (0-17, 18-44, 45-64, 65-74, 75+)."""
    bins   = [0, 17, 44, 64, 74, 200]
    labels = ["0-17", "18-44", "45-64", "65-74", "75+"]
    return pd.cut(age, bins=bins, labels=labels, right=True).astype(str)


def date_vers_mois(date_col: pd.Series) -> pd.Series:
    """Réduit une date exacte à l'année-mois (ex : 2026-03)."""
    return pd.to_datetime(date_col, errors="coerce").dt.to_period("M").astype(str)


def supprimer_colonnes_identifiantes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Supprime toutes les colonnes permettant une ré-identification directe ou indirecte.

    Colonnes supprimées :
        - ID Anonyme      : identifiant pseudonymisé (hashé, mais réversible)
        - Date Naissance  : date exacte → supprimée (remplacée par tranche d'âge)
        - Date Prélèvement: date exacte → remplacée par Mois
        - Adresse / IPP   : si présents
    """
    colonnes_a_supprimer = [
        "ID Anonyme", "Date Naissance", "IPP", "Adresse",
        "Nom", "Prenom", "Date de naissance",
    ]
    existantes = [c for c in colonnes_a_supprimer if c in df.columns]
    return df.drop(columns=existantes)


# =============================================================================
# Pipeline d'anonymisation
# =============================================================================

def anonymiser(df_brut: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme un DataFrame individuel en agrégats anonymes.

    Étapes :
    1. Supprimer les colonnes identifiantes
    2. Convertir l'âge exact → tranche
    3. Convertir la date exacte → mois
    4. Agréger par (Service, Tranche âge, Sexe, Décision, Bactérie, Mois)
    5. Appliquer le k-anonymat : supprimer les groupes < K_ANONYMAT_MIN patients
    """
    df = df_brut.copy()

    # Étape 1 — Suppression des colonnes identifiantes
    df = supprimer_colonnes_identifiantes(df)

    # Étape 2 — Âge → tranche
    if "Age" in df.columns:
        df["Tranche Age"] = age_vers_tranche(df["Age"])
        df.drop(columns=["Age"], inplace=True)

    # Étape 3 — Date → mois
    if "Date Prélèvement" in df.columns:
        df["Mois"] = date_vers_mois(df["Date Prélèvement"])
        df.drop(columns=["Date Prélèvement"], inplace=True)

    # Étape 4 — Agrégation
    axes = [c for c in ["Service", "Tranche Age", "Sexe", "Décision Algorithme",
                         "Bactérie", "Mois", "Symptomatique", "Mode Prélèvement"]
            if c in df.columns]

    if not axes:
        print("ERREUR : aucune colonne d'agrégation trouvée.", file=sys.stderr)
        return pd.DataFrame()

    df_agg = (
        df.groupby(axes, dropna=False)
        .size()
        .reset_index(name="Nombre ECBU")
    )

    # Étape 5 — K-anonymat
    avant = len(df_agg)
    df_agg = df_agg[df_agg["Nombre ECBU"] >= K_ANONYMAT_MIN]
    supprimes = avant - len(df_agg)

    if supprimes > 0:
        print(f"  K-anonymat (k={K_ANONYMAT_MIN}) : {supprimes} groupes supprimés "
              f"({supprimes / avant * 100:.1f}% des lignes d'agrégat)")

    return df_agg


# =============================================================================
# Vérification post-anonymisation
# =============================================================================

def verifier_anonymisation(df_agg: pd.DataFrame) -> bool:
    """
    Vérifie qu'aucune colonne identifiante ne subsiste.
    Retourne True si le fichier est sûr pour GitHub.
    """
    colonnes_interdites = [
        "ID Anonyme", "IPP", "Date Naissance", "Date de naissance",
        "Nom", "Prenom", "Adresse",
    ]
    trouvees = [c for c in colonnes_interdites if c in df_agg.columns]
    if trouvees:
        print(f"ALERTE SÉCURITÉ : colonnes identifiantes présentes : {trouvees}",
              file=sys.stderr)
        return False

    nb_min = df_agg["Nombre ECBU"].min() if "Nombre ECBU" in df_agg.columns else 0
    if nb_min < K_ANONYMAT_MIN:
        print(f"ALERTE K-ANONYMAT : groupe avec {nb_min} patients (< {K_ANONYMAT_MIN})",
              file=sys.stderr)
        return False

    return True


# =============================================================================
# Point d'entrée
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Anonymise les données ECBU pour publication GitHub",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--output", type=str, default=DEFAULT_OUTPUT, metavar="CSV",
        help="Chemin du fichier CSV anonymisé de sortie",
    )
    parser.add_argument(
        "--k-min", type=int, default=K_ANONYMAT_MIN, metavar="K",
        help="Seuil k-anonymat : supprimer les groupes < K patients",
    )
    args = parser.parse_args()
    K_ANONYMAT_MIN = args.k_min

    print("Connexion à PostgreSQL...")
    try:
        engine = get_engine()
        df_brut = pd.read_sql("SELECT * FROM v_algo_avicenne", engine)
    except Exception as exc:
        print(f"Erreur connexion : {exc}", file=sys.stderr)
        print("Vérifiez votre fichier .env et que PostgreSQL est actif.")
        sys.exit(1)

    print(f"  {len(df_brut)} lignes chargées depuis v_algo_avicenne")
    print(f"  Colonnes : {list(df_brut.columns)}")

    print("\nAnonymisation en cours...")
    df_anon = anonymiser(df_brut)

    if df_anon.empty:
        print("Échec de l'anonymisation.", file=sys.stderr)
        sys.exit(1)

    print(f"  {len(df_anon)} groupes d'agrégats produits")
    print(f"  Total ECBU représentés : {df_anon['Nombre ECBU'].sum()}")

    print("\nVérification de sécurité...")
    if not verifier_anonymisation(df_anon):
        print("Le fichier n'est PAS sûr pour publication. Abandon.", file=sys.stderr)
        sys.exit(1)
    print("  OK — aucune donnée identifiante détectée")

    df_anon.to_csv(args.output, index=False, encoding="utf-8")
    print(f"\nFichier anonymisé exporté : {args.output}")
    print("Ce fichier peut être commité sur GitHub sans restriction RGPD.")
    print("\nProchaines étapes :")
    print("  git add donnees_anonymes.csv")
    print("  git commit -m 'data: mise à jour agrégats anonymisés'")
    print("  git push")
