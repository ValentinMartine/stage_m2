"""
Module algo_ecbu.py — Miroir Python de la logique CASE WHEN de v_algo_avicenne.

Implémente les règles cliniques REMIC 2022 / SPILF 2015 / HAS 2023 (R0–R10)
identiques à la vue PostgreSQL v_algo_avicenne.

Colonnes d'entrée attendues :
    bacteriurie_num    : int  — UFC/mL (nettoyé)
    leucocyturie       : int  — leucocytes/mL
    germe_nom          : str  — nom du germe
    nb_especes         : int  — nombre d'espèces
    est_sonde          : int  — 1 si matériel en place
    est_immunodeprime  : int  — 1 si neutropénie/immunodépression
    est_enceinte       : int  — 1 si grossesse
    antibio_en_cours   : int  — 1 si antibiothérapie avant ECBU
    code_genre         : int  — 1=Homme, 2=Femme
"""

import pandas as pd


def _decision(row) -> str:
    """Applique les règles R0–R10 sur une ligne (Series ou dict)."""
    germe  = str(row.get("germe_nom", "")).lower()
    bact   = int(row.get("bacteriurie_num", 0))
    leuco  = int(row.get("leucocyturie", 0))
    nb_esp = int(row.get("nb_especes", 1))
    sonde  = int(row.get("est_sonde", 0))
    immuno = int(row.get("est_immunodeprime", 0))
    encein = int(row.get("est_enceinte", 0))
    atb    = int(row.get("antibio_en_cours", 0))
    genre  = int(row.get("code_genre", 0))

    # R0 — Culture stérile sans leucocyturie ni ATB
    if "stérile" in germe and leuco < 10000 and atb == 0:
        return "NÉGATIF : Stérile"

    # R1 — Contamination (polymicrobisme ≥ 3 espèces OU flore polymorphe/cutanée)
    if nb_esp >= 3 or "polymorphe" in germe or "cutanée" in germe:
        return "REJET : Contamination probable"

    # R2 — Matériel en place (sonde, néphrostomie) — pas de seuil
    if sonde == 1 and bact > 0:
        return "POSITIF : Matériel (pas de seuil)"

    # R3 — Immunodéprimé / neutropénique — culture justifiée sans leucocyturie
    if immuno == 1 and bact > 0:
        return "POSITIF : Immunodéprimé (exception leucocyturie)"

    # R4 — Femme enceinte — dépistage colonisation
    if encein == 1 and bact >= 1000:
        return "POSITIF : Grossesse (dépistage colonisation)"

    # R5 — Infection décapitée (ATB + leucocyturie + culture stérile)
    if atb == 1 and leuco >= 10000 and ("stérile" in germe or bact == 0):
        return "ALERTE : Infection décapitée possible (ATB + leucocyturie)"

    # R6 — Leucocyturie < 10^4 sans exception → pas d'infection probable
    if leuco < 10000 and immuno == 0 and sonde == 0 and encein == 0:
        return "NÉGATIF : Leucocyturie non significative"

    # R7 — Germes hautement uropathogènes (E. coli, S. saprophyticus) : seuil ≥ 10^3
    if row.get("germe_nom", "") in ("Escherichia coli", "Staphylococcus saprophyticus") \
            and bact >= 1000:
        return "POSITIF : Germe prioritaire (seuil 10^3)"

    # R8 — Autres germes — Homme : seuil ≥ 10^3
    if genre == 1 and bact >= 1000:
        return "POSITIF : Homme (seuil 10^3)"

    # R9 — Autres germes — Femme : seuil ≥ 10^4
    if genre == 2 and bact >= 10000:
        return "POSITIF : Femme (seuil 10^4)"

    # R10 — Sous les seuils
    return "NÉGATIF : Sous seuil / Colonisation"


def appliquer_algorithme(df: pd.DataFrame) -> pd.Series:
    """
    Applique les règles R0–R10 sur chaque ligne du DataFrame.

    Args:
        df : DataFrame avec colonnes bacteriurie_num, leucocyturie, germe_nom,
             nb_especes, est_sonde, est_immunodeprime, est_enceinte,
             antibio_en_cours, code_genre.

    Returns:
        pd.Series de chaînes de décision (même index que df).
    """
    return df.apply(_decision, axis=1).rename("Décision Algorithme")
