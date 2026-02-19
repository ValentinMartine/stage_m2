"""
Générateur de données ECBU simulées — v2
Produit un fichier Excel avec des profils cliniques cohérents
pour tester le pipeline ETL + algorithme SAD.

Les corrélations épidémiologiques sont encodées :
  - Gériatrie / femme âgée → forte prévalence ASB (E. coli asymptomatique)
  - Urgences / adulte jeune → infections classiques symptomatiques
  - Réanimation → patients sondés, neutropéniques
  - Pédiatrie → malformations possibles, germes spécifiques
  - Urologie → matériel (sonde JJ, néphrostomie), contexte pré-opératoire
"""

import pandas as pd
import random
from datetime import datetime, timedelta
import os

# =============================================================================
# CONFIGURATION
# =============================================================================
NOMBRE_ECBU = 200
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
FILENAME = "export_mars_2026.xlsx"

SERVICES = [
    'URGENCES', 'MEDECINE_INTERNE', 'GASTRO_ENTERO', 'ONCOLOGIE',
    'GERIATRIE_A', 'PEDIATRIE', 'UROLOGIE_B', 'REANIMATION',
    'CHIRURGIE_DIGESTIVE', 'NEPHROLOGIE'
]

# Germes uropathogènes classés par catégorie REMIC
GERMES_PRIORITAIRES = ['Escherichia coli', 'Staphylococcus saprophyticus']
GERMES_AUTRES = ['Klebsiella pneumoniae', 'Enterococcus faecalis',
                 'Proteus mirabilis', 'Pseudomonas aeruginosa']
GERMES_CONTAMINATION = ['Flore polymorphe', 'Flore cutanée']

MODES_PRELEVEMENT = [
    'MILIEU_JET', 'SONDAGE_AR', 'SONDE_DEMEURE',
    'PENILEX', 'POCHE', 'NEPHROSTOMIE', 'SONDE_JJ'
]

# =============================================================================
# PROFILS CLINIQUES (corrélations réalistes)
# =============================================================================

def _resistance(poids: dict) -> str:
    """Tire un profil de résistance selon des probabilités épidémiologiques."""
    return random.choices(list(poids.keys()), weights=list(poids.values()), k=1)[0]


def generer_profil_geriatrie_asb():
    """Femme âgée asymptomatique — bactériurie asymptomatique classique (à ne PAS traiter)."""
    return {
        'sexe': 2,
        'cat_age': 'vieux',
        'service': 'GERIATRIE_A',
        'symptomes': 0,
        'germe': 'Escherichia coli',
        'quantite': random.choice([10000, 100000]),
        'leucocyturie': random.choice([0, 10000]),  # Peut avoir une leucocyturie modérée
        'mode_prelevement': random.choice(['POCHE', 'PENILEX', 'SONDE_DEMEURE']),
        'nb_especes': 1,
        'est_sonde': False,
        'est_immunodeprime': False,
        'est_enceinte': False,
        'antibio_en_cours': False,
        'profil_resistance': _resistance({'Sensible': 95, 'BLSE': 5}),
    }

def generer_profil_iu_classique():
    """Adulte (souvent femme) avec infection urinaire symptomatique — à traiter."""
    sexe = random.choices([1, 2], weights=[20, 80])[0]
    return {
        'sexe': sexe,
        'cat_age': 'adulte',
        'service': random.choice(['URGENCES', 'MEDECINE_INTERNE', 'GASTRO_ENTERO']),
        'symptomes': 1,
        'germe': random.choices(
            GERMES_PRIORITAIRES + GERMES_AUTRES[:2],
            weights=[50, 10, 20, 15]
        )[0],
        'quantite': random.choice([10000, 100000, 1000000]),
        'leucocyturie': random.choice([10000, 50000, 100000]),
        'mode_prelevement': 'MILIEU_JET',
        'nb_especes': 1,
        'est_sonde': False,
        'est_immunodeprime': False,
        'est_enceinte': (sexe == 2 and random.random() < 0.1),
        'antibio_en_cours': False,
        'profil_resistance': _resistance({'Sensible': 80, 'BLSE': 18, 'Carbapenemase': 2}),
    }

def generer_profil_contamination():
    """Prélèvement de mauvaise qualité — flore polymorphe, à rejeter."""
    return {
        'sexe': random.choice([1, 2]),
        'cat_age': random.choice(['adulte', 'vieux']),
        'service': random.choice(['GERIATRIE_A', 'MEDECINE_INTERNE', 'URGENCES']),
        'symptomes': random.choice([0, 1]),
        'germe': random.choice(GERMES_CONTAMINATION),
        'quantite': random.choice([1000, 5000, 10000]),
        'leucocyturie': random.choice([0, 5000, 10000]),
        'mode_prelevement': random.choice(['POCHE', 'PENILEX']),
        'nb_especes': random.randint(3, 5),
        'est_sonde': False,
        'est_immunodeprime': False,
        'est_enceinte': False,
        'antibio_en_cours': False,
        'profil_resistance': 'Inconnu',
    }

def generer_profil_sterile():
    """ECBU négatif — pas d'infection, prescription potentiellement inutile."""
    return {
        'sexe': random.choice([1, 2]),
        'cat_age': random.choice(['adulte', 'vieux']),
        'service': random.choice(SERVICES),
        'symptomes': random.choice([0, 0, 0, 1]),  # 75% sans symptômes
        'germe': 'Stérile',
        'quantite': 0,
        'leucocyturie': 0,
        'mode_prelevement': 'MILIEU_JET',
        'nb_especes': 0,
        'est_sonde': False,
        'est_immunodeprime': False,
        'est_enceinte': False,
        'antibio_en_cours': False,
        'profil_resistance': 'Inconnu',
    }

def generer_profil_sonde_uro():
    """Patient sondé en urologie — matériel en place, pas de seuil de bactériurie."""
    return {
        'sexe': random.choices([1, 2], weights=[70, 30])[0],
        'cat_age': random.choice(['adulte', 'vieux']),
        'service': 'UROLOGIE_B',
        'symptomes': random.choice([0, 1]),
        'germe': random.choice(GERMES_PRIORITAIRES + GERMES_AUTRES),
        'quantite': random.choice([100, 1000, 10000, 100000]),
        'leucocyturie': random.choice([0, 10000, 50000]),
        'mode_prelevement': random.choice(['SONDE_DEMEURE', 'SONDE_JJ', 'NEPHROSTOMIE']),
        'nb_especes': random.choice([1, 1, 1, 2]),
        'est_sonde': True,
        'est_immunodeprime': False,
        'est_enceinte': False,
        'antibio_en_cours': False,
        'profil_resistance': _resistance({'Sensible': 75, 'BLSE': 20, 'Carbapenemase': 5}),
    }

def generer_profil_reanimation():
    """Patient de réanimation — souvent sondé, parfois neutropénique."""
    immunodep = random.random() < 0.4
    return {
        'sexe': random.choice([1, 2]),
        'cat_age': random.choice(['adulte', 'vieux']),
        'service': 'REANIMATION',
        'symptomes': 0,  # Patient non verbal (sédaté)
        'germe': random.choice(GERMES_PRIORITAIRES + GERMES_AUTRES),
        'quantite': random.choice([1000, 10000, 100000]),
        'leucocyturie': 0 if immunodep else random.choice([10000, 50000]),
        'mode_prelevement': 'SONDE_DEMEURE',
        'nb_especes': 1,
        'est_sonde': True,
        'est_immunodeprime': immunodep,
        'est_enceinte': False,
        'antibio_en_cours': random.random() < 0.5,
        'profil_resistance': _resistance({'Sensible': 50, 'BLSE': 30, 'Carbapenemase': 15, 'MRSA': 5}),
    }

def generer_profil_homme_limite():
    """Homme avec germe non prioritaire au seuil limite (10^3) — teste la règle REMIC homme."""
    return {
        'sexe': 1,
        'cat_age': 'adulte',
        'service': random.choice(['URGENCES', 'MEDECINE_INTERNE']),
        'symptomes': 1,
        'germe': random.choice(GERMES_AUTRES),
        'quantite': random.choice([500, 1000, 5000]),  # Autour du seuil
        'leucocyturie': random.choice([10000, 50000]),
        'mode_prelevement': 'MILIEU_JET',
        'nb_especes': 1,
        'est_sonde': False,
        'est_immunodeprime': False,
        'est_enceinte': False,
        'antibio_en_cours': False,
        'profil_resistance': _resistance({'Sensible': 85, 'BLSE': 15}),
    }

def generer_profil_femme_seuil():
    """Femme avec germe non prioritaire au seuil 10^4 — teste la règle REMIC femme."""
    return {
        'sexe': 2,
        'cat_age': 'adulte',
        'service': random.choice(['URGENCES', 'GYNECOLOGIE', 'MEDECINE_INTERNE']),
        'symptomes': 1,
        'germe': random.choice(GERMES_AUTRES),
        'quantite': random.choice([1000, 5000, 10000, 50000]),  # Autour du seuil
        'leucocyturie': random.choice([10000, 50000]),
        'mode_prelevement': 'MILIEU_JET',
        'nb_especes': 1,
        'est_sonde': False,
        'est_immunodeprime': False,
        'est_enceinte': False,
        'antibio_en_cours': False,
        'profil_resistance': _resistance({'Sensible': 80, 'BLSE': 18, 'Carbapenemase': 2}),
    }

def generer_profil_onco_immunodeprime():
    """Patient d'oncologie immunodéprimé — peut avoir infection sans leucocyturie."""
    return {
        'sexe': random.choice([1, 2]),
        'cat_age': random.choice(['adulte', 'vieux']),
        'service': 'ONCOLOGIE',
        'symptomes': random.choice([0, 1]),
        'germe': random.choice(GERMES_PRIORITAIRES + GERMES_AUTRES[:2]),
        'quantite': random.choice([1000, 10000, 100000]),
        'leucocyturie': random.choice([0, 0, 5000, 10000]),  # Souvent basse
        'mode_prelevement': random.choice(['MILIEU_JET', 'SONDAGE_AR']),
        'nb_especes': 1,
        'est_sonde': False,
        'est_immunodeprime': True,
        'est_enceinte': False,
        'antibio_en_cours': random.random() < 0.3,
        'profil_resistance': _resistance({'Sensible': 65, 'BLSE': 25, 'Carbapenemase': 10}),
    }

def generer_profil_infection_decapitee():
    """Patient sous antibiotiques — culture faussement négative possible."""
    return {
        'sexe': random.choice([1, 2]),
        'cat_age': random.choice(['adulte', 'vieux']),
        'service': random.choice(['MEDECINE_INTERNE', 'CHIRURGIE_DIGESTIVE']),
        'symptomes': 1,
        'germe': 'Stérile',  # Faussement négatif à cause des ATB
        'quantite': 0,
        'leucocyturie': random.choice([10000, 50000, 100000]),  # Leucocyturie présente !
        'mode_prelevement': 'MILIEU_JET',
        'nb_especes': 0,
        'est_sonde': False,
        'est_immunodeprime': False,
        'est_enceinte': False,
        'antibio_en_cours': True,
        'profil_resistance': 'Inconnu',
    }

def generer_profil_pediatrie():
    """Enfant — particularités pédiatriques (malformations, seuils différents)."""
    return {
        'sexe': random.choice([1, 2]),
        'cat_age': random.choice(['bebe', 'enfant']),
        'service': 'PEDIATRIE',
        'symptomes': random.choice([0, 1]),  # Nourrisson non verbal
        'germe': random.choices(
            ['Escherichia coli', 'Proteus mirabilis', 'Stérile'],
            weights=[50, 20, 30]
        )[0],
        'quantite': random.choice([0, 1000, 10000, 100000]),
        'leucocyturie': random.choice([0, 10000, 50000]),
        'mode_prelevement': random.choice(['POCHE', 'SONDAGE_AR', 'MILIEU_JET']),
        'nb_especes': random.choice([0, 1, 1, 2]),
        'est_sonde': False,
        'est_immunodeprime': False,
        'est_enceinte': False,
        'antibio_en_cours': False,
        'profil_resistance': _resistance({'Sensible': 90, 'BLSE': 10}),
    }


# Distribution des profils (pondérations épidémiologiques approximatives)
PROFILS = [
    (generer_profil_geriatrie_asb,       18),  # ASB gériatrique : fréquent
    (generer_profil_iu_classique,        22),  # IU classique : le plus fréquent
    (generer_profil_contamination,       12),  # Contaminations
    (generer_profil_sterile,             15),  # Stérile
    (generer_profil_sonde_uro,            8),  # Urologie sondé
    (generer_profil_reanimation,          7),  # Réanimation
    (generer_profil_homme_limite,         5),  # Homme seuil limite
    (generer_profil_femme_seuil,          5),  # Femme seuil limite
    (generer_profil_onco_immunodeprime,   4),  # Oncologie immunodéprimé
    (generer_profil_infection_decapitee,  2),  # Infection décapitée
    (generer_profil_pediatrie,            2),  # Pédiatrie
]

# =============================================================================
# UTILITAIRES
# =============================================================================


def generer_ipp(index):
    return f"800{str(index).zfill(5)}"

def generer_date_naissance(categorie):
    today = datetime.now()
    if categorie == 'bebe':
        jours = random.randint(1, 365 * 2)
    elif categorie == 'enfant':
        jours = random.randint(3 * 365, 17 * 365)
    elif categorie == 'adulte':
        jours = random.randint(18 * 365, 74 * 365)
    else:
        jours = random.randint(75 * 365, 100 * 365)
    return today - timedelta(days=jours)

def generer_date_prelevement():
    """Date aléatoire sur le mois de mars 2026."""
    jour = random.randint(1, 31)
    try:
        return datetime(2026, 3, jour, random.randint(6, 22), random.randint(0, 59))
    except ValueError:
        return datetime(2026, 3, 28, random.randint(6, 22), random.randint(0, 59))

def formater_quantite(q):
    """Simule du texte 'sale' comme dans les exports réels (10% du temps)."""
    if q == 0:
        return "0"
    if random.random() < 0.1:
        return f"> {q}"
    return str(q)

# =============================================================================
# GÉNÉRATION
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Générateur de données ECBU simulées",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--n", type=int, default=NOMBRE_ECBU, metavar="N",
        help="Nombre d'ECBU à générer",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Graine aléatoire pour la reproductibilité",
    )
    parser.add_argument(
        "--output", type=str, default=os.path.join(OUTPUT_DIR, FILENAME),
        metavar="FICHIER",
        help="Chemin du fichier Excel de sortie",
    )
    parser.add_argument(
        "--synthea", type=str, default=None, metavar="CSV",
        help="Chemin vers un fichier patients.csv Synthea (démographies réelles)",
    )
    parser.add_argument(
        "--calibrate-from-db", action="store_true",
        help="Lire la distribution réelle depuis PostgreSQL et afficher les poids suggérés",
    )
    parser.add_argument(
        "--synthea-url", type=str, default=None, metavar="URL",
        help="URL pour télécharger les données démographiques Synthea",
    )
    args = parser.parse_args()

    # -------------------------------------------------------------------------
    # --calibrate-from-db : analyse la distribution réelle et affiche les poids
    # -------------------------------------------------------------------------
    if args.calibrate_from_db:
        import sys as _sys
        try:
            from dotenv import load_dotenv as _ldenv
            _ldenv(os.path.join(OUTPUT_DIR, ".env"))
        except ImportError:
            pass
        try:
            from sqlalchemy import create_engine as _ce
        except ImportError:
            print("sqlalchemy requis : pip install sqlalchemy psycopg2-binary")
            _sys.exit(1)

        _db_pass = os.getenv("DB_PASS", "")
        if not _db_pass:
            print("DB_PASS non défini. Vérifiez votre .env.")
            _sys.exit(1)

        _engine = _ce(
            f"postgresql+psycopg2://{os.getenv('DB_USER','postgres')}:{_db_pass}"
            f"@{os.getenv('DB_HOST','localhost')}:{os.getenv('DB_PORT','5432')}"
            f"/{os.getenv('DB_NAME','postgres')}"
        )
        _df_dist = pd.read_sql(
            'SELECT "Service", COUNT(*) AS n FROM v_algo_avicenne '
            'GROUP BY "Service" ORDER BY n DESC',
            _engine,
        )
        _total = _df_dist["n"].sum()

        # Correspondance service → profil
        _SVC_TO_PROFIL = {
            "GERIATRIE_A":         "generer_profil_geriatrie_asb",
            "URGENCES":            "generer_profil_iu_classique",
            "MEDECINE_INTERNE":    "generer_profil_iu_classique",
            "GASTRO_ENTERO":       "generer_profil_iu_classique",
            "REANIMATION":         "generer_profil_reanimation",
            "UROLOGIE_B":          "generer_profil_sonde_uro",
            "ONCOLOGIE":           "generer_profil_onco_immunodeprime",
            "PEDIATRIE":           "generer_profil_pediatrie",
            "CHIRURGIE_DIGESTIVE": "generer_profil_infection_decapitee",
            "NEPHROLOGIE":         "generer_profil_sonde_uro",
        }

        print("\nDistribution réelle par service (depuis v_algo_avicenne) :")
        print(f"  {'Service':30s} {'N':>6}  {'%':>6}  Profil suggéré")
        print(f"  {'─'*75}")
        for _, _row in _df_dist.iterrows():
            _svc   = _row["Service"]
            _n     = _row["n"]
            _pct   = _n / _total * 100
            _prof  = _SVC_TO_PROFIL.get(_svc, "???")
            print(f"  {_svc:30s} {_n:>6}  {_pct:>5.1f}%  {_prof}")

        print(f"\n  Total : {_total} ECBU")
        print("\nPoids calibrés suggérés (arrondis) :")
        for _, _row in _df_dist.iterrows():
            _poids = max(1, round(_row["n"] / _total * 100))
            print(f"  ({_SVC_TO_PROFIL.get(_row['Service'], '???')}, {_poids}),")

        print("\nNote : remplacez les poids dans PROFILS[] pour refléter la réalité.")
        _sys.exit(0)

    if args.seed is not None:
        random.seed(args.seed)
        print(f"Graine aléatoire : {args.seed}")

    # -------------------------------------------------------------------------
    # Chargement optionnel des démographies Synthea
    # -------------------------------------------------------------------------
    synthea_df = None
    if args.synthea:
        from synthea_integration import charger_patients_synthea
        synthea_df = charger_patients_synthea(args.synthea)
    elif args.synthea_url:
        from synthea_integration import telecharger_patients_synthea
        synthea_df = telecharger_patients_synthea(url=args.synthea_url)

    if synthea_df is not None:
        from synthea_integration import mapper_demographics
        print(f"[Synthea] Démographies actives ({len(synthea_df)} patients disponibles)")

    # -------------------------------------------------------------------------
    # Génération
    # -------------------------------------------------------------------------
    fonctions, poids = zip(*PROFILS)
    data = []
    print(f"Fabrication de {args.n} ECBU simulés (profils corrélés)...")

    for i in range(1, args.n + 1):
        fn = random.choices(fonctions, weights=poids, k=1)[0]
        profil = fn()

        if synthea_df is not None:
            patient = synthea_df.iloc[(i - 1) % len(synthea_df)]
            demo = mapper_demographics(patient)
            ddn = demo["ddn"]
            profil["sexe"] = demo["sexe"]
            profil["cat_age"] = demo["cat_age"]
        else:
            ddn = generer_date_naissance(profil["cat_age"])

        date_prelev = generer_date_prelevement()

        row = {
            'NUM_IPP':            generer_ipp(i),
            'DT_NAISS':           ddn.strftime("%d/%m/%Y"),
            'CD_SEXE':            profil['sexe'],
            'UF_EXEC':            profil['service'],
            'DT_PRELEVEMENT':     date_prelev.strftime("%d/%m/%Y %H:%M"),
            'MODE_PRELEVEMENT':   profil['mode_prelevement'],
            'LEUCOCYTURIE':       profil['leucocyturie'],
            'RES_VAL':            formater_quantite(profil['quantite']),
            'GERME_NOM':          profil['germe'],
            'NB_ESPECES':         profil['nb_especes'],
            'SYMPTOMES':          profil['symptomes'],
            'EST_SONDE':          int(profil['est_sonde']),
            'EST_IMMUNODEPRIME':  int(profil['est_immunodeprime']),
            'EST_ENCEINTE':       int(profil['est_enceinte']),
            'ANTIBIO_EN_COURS':   int(profil['antibio_en_cours']),
            'PROFIL_RESISTANCE':  profil.get('profil_resistance', 'Inconnu'),
        }
        data.append(row)

    # =========================================================================
    # EXPORT
    # =========================================================================
    df = pd.DataFrame(data)
    path = args.output

    try:
        df.to_excel(path, index=False)
        print(f"Fichier créé : {path}")
        print(f"  → {len(df)} lignes, {len(df.columns)} colonnes")
    except Exception as e:
        print(f"Erreur : {e}")

    # Résumé des profils générés
    print("\nRépartition par service :")
    print(df['UF_EXEC'].value_counts().to_string())
    print(f"\nTaux symptomatiques : {df['SYMPTOMES'].mean()*100:.0f}%")
    print(f"Taux sondés         : {df['EST_SONDE'].mean()*100:.0f}%")
    print(f"Taux immunodéprimés : {df['EST_IMMUNODEPRIME'].mean()*100:.0f}%")
    print(f"\nAperçu :")
    print(df.head(10).to_string(index=False))
