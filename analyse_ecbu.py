"""
ETL ECBU — Import Excel → PostgreSQL + Calcul KPIs
v2 : mapping élargi (leucocyturie, mode prélèvement, nb espèces, contexte clinique),
     credentials via variables d'environnement, corrections de bugs.

Prérequis :
  1. Exécuter script_sql_final.sql dans DBeaver pour créer la table + vue
  2. pip install pandas sqlalchemy psycopg2-binary python-dotenv openpyxl
  3. Créer un fichier .env à côté de ce script (voir modèle ci-dessous)

Modèle .env :
    DB_USER=postgres
    DB_PASS=votre_mot_de_passe
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=postgres
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from kpis_ecbu import taux_non_pertinence, compter_asb, compter_infections_decapitees, \
    compter_prelev_risque, stats_par_service

# =============================================================================
# CONFIGURATION — Credentials depuis .env ou variables d'environnement
# =============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, '.env')

try:
    from dotenv import load_dotenv
    load_dotenv(ENV_PATH)
except ImportError:
    pass  # python-dotenv optionnel, on utilise les variables système

DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', '')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'postgres')

if not DB_PASS:
    print("ATTENTION : DB_PASS non défini. Créez un fichier .env ou définissez la variable.")
    print("  Exemple .env :")
    print("    DB_USER=postgres")
    print("    DB_PASS=mon_mot_de_passe")
    sys.exit(1)

conn_string = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Chemin du fichier source (relatif au script)
FICHIER_SOURCE = os.path.join(SCRIPT_DIR, "export_mars_2026.xlsx")


# =============================================================================
# MAPPING : Colonnes Excel → Colonnes PostgreSQL
# =============================================================================
MAPPING_COLONNES = {
    'NUM_IPP':           'id_dossier_x99',
    'DT_NAISS':          'ddn_pat',
    'CD_SEXE':           'code_genre',
    'UF_EXEC':           'unit_func_lbl',
    'DT_PRELEVEMENT':    'dt_prelevement',
    'MODE_PRELEVEMENT':  'mode_prelevement',
    'LEUCOCYTURIE':      'leucocyturie',
    'RES_VAL':           'valeur_res_num',
    'GERME_NOM':         'germe_nom',
    'NB_ESPECES':        'nb_especes',
    'SYMPTOMES':         'est_symptomatique',
    'EST_SONDE':         'est_sonde',
    'EST_IMMUNODEPRIME': 'est_immunodeprime',
    'EST_ENCEINTE':      'est_enceinte',
    'ANTIBIO_EN_COURS':  'antibio_en_cours',
    'PROFIL_RESISTANCE': 'profil_resistance',
}


def etape_extract_load(engine, fichier):
    """ÉTAPE 1 : Lecture Excel, renommage, nettoyage, envoi vers PostgreSQL."""
    print(f"\n{'='*60}")
    print("ÉTAPE 1 : IMPORT DES DONNÉES")
    print(f"{'='*60}")
    print(f"Source : {fichier}")

    # Lecture
    df = pd.read_excel(fichier)
    print(f"  Lignes lues : {len(df)}")
    print(f"  Colonnes Excel : {list(df.columns)}")

    # Vérification des colonnes attendues
    colonnes_manquantes = set(MAPPING_COLONNES.keys()) - set(df.columns)
    if colonnes_manquantes:
        print(f"  ATTENTION — Colonnes manquantes dans le fichier : {colonnes_manquantes}")
        print(f"  Le mapping sera partiel. Colonnes disponibles : {list(df.columns)}")

    # Renommage (on ne garde que les colonnes présentes)
    colonnes_presentes = {k: v for k, v in MAPPING_COLONNES.items() if k in df.columns}
    df = df.rename(columns=colonnes_presentes)
    df = df[list(colonnes_presentes.values())]

    # Nettoyage des types
    if 'ddn_pat' in df.columns:
        df['ddn_pat'] = pd.to_datetime(df['ddn_pat'], dayfirst=True, errors='coerce')
    if 'dt_prelevement' in df.columns:
        df['dt_prelevement'] = pd.to_datetime(df['dt_prelevement'], dayfirst=True, errors='coerce')
    if 'valeur_res_num' in df.columns:
        df['valeur_res_num'] = df['valeur_res_num'].astype(str)

    # Vidage de la table avant import (évite les doublons)
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE orbis_export_brut CASCADE;"))
        conn.commit()

    # Envoi
    df.to_sql('orbis_export_brut', engine, if_exists='append', index=False)
    print(f"  → {len(df)} lignes importées dans orbis_export_brut")
    return len(df)


def etape_analyse(engine):
    """ÉTAPE 2 : Lecture de la vue algorithmique + calcul des KPIs."""
    print(f"\n{'='*60}")
    print("ÉTAPE 2 : ANALYSE (via vue v_algo_avicenne)")
    print(f"{'='*60}")

    df = pd.read_sql("SELECT * FROM v_algo_avicenne", engine)
    total = len(df)

    if total == 0:
        print("  Aucune donnée dans la vue. Vérifiez l'import.")
        return

    col_decision = "Décision Algorithme"
    repartition  = df[col_decision].value_counts()

    print(f"\n  Total ECBU analysés : {total}")
    print(f"\n  Répartition des décisions :")
    for decision, count in repartition.items():
        pct = count / total * 100
        print(f"    {decision:55s} : {count:4d}  ({pct:5.1f}%)")

    # --- KPIs via kpis_ecbu ---
    tnp    = taux_non_pertinence(df)
    nb_asb = compter_asb(df)
    nb_dec = compter_infections_decapitees(df)
    nb_ris = compter_prelev_risque(df)

    print(f"\n  {'─'*50}")
    print(f"  Non pertinents (NÉGATIF + REJET) : {tnp['nb_np']} / {tnp['total']}")
    print(f"  TAUX DE NON-PERTINENCE           : {tnp['taux']:.1f}%")
    print(f"  IC 95% Wilson                    : [{tnp['ci_low']:.1f}% – {tnp['ci_high']:.1f}%]")
    print(f"  {'─'*50}")
    print(f"\n  Bactériuries asymptomatiques (ASB) identifiées  : {nb_asb}")
    print(f"  Prélèvements à risque de contamination          : {nb_ris}")
    print(f"  Alertes infection décapitée (ATB + leucocyturie): {nb_dec}")

    # --- Par service ---
    df_svc = stats_par_service(df)
    print(f"\n  Taux de non-pertinence par service :")
    for _, row in df_svc.iterrows():
        print(f"    {row['Service']:25s} : {row['Non pertinents']:3d}/{row['Total ECBU']:3d}"
              f"  ({row['Taux NP (%)']:5.1f}%)")

    return df


# =============================================================================
# MAIN
# =============================================================================
if __name__ == '__main__':
    try:
        engine = create_engine(conn_string)

        # Test connexion
        with engine.connect() as conn:
            print("Connexion PostgreSQL OK")

        etape_extract_load(engine, FICHIER_SOURCE)
        etape_analyse(engine)

    except FileNotFoundError:
        print(f"ERREUR : Fichier non trouvé : {FICHIER_SOURCE}")
        print("  → Exécutez d'abord generateur_donnees.py ou placez votre export ici.")
    except Exception as e:
        print(f"ERREUR : {e}")
        import traceback
        traceback.print_exc()
