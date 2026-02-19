"""
Module kpis_ecbu.py — Fonctions KPI pures pour le pilotage de la juste prescription ECBU.

Exportées vers analyse_ecbu.py et dashboard.py.
Toutes les fonctions sont déterministes (DataFrame → scalaire / dict / DataFrame).
"""

import pandas as pd
from statsmodels.stats.proportion import proportion_confint

COL_DEC = "Décision Algorithme"


def taux_non_pertinence(df: pd.DataFrame, alpha: float = 0.05) -> dict:
    """
    Calcule le taux de non-pertinence avec intervalle de confiance Wilson 95 %.

    Args:
        df    : DataFrame issu de v_algo_avicenne (doit contenir COL_DEC).
        alpha : Niveau de signification (défaut 0.05 → IC 95 %).

    Returns:
        dict {nb_np, total, taux, ci_low, ci_high} — taux et bornes IC en pourcentage.
    """
    total = len(df)
    if total == 0:
        return {"nb_np": 0, "total": 0, "taux": 0.0, "ci_low": 0.0, "ci_high": 0.0}

    nb_np = int(
        df[COL_DEC].str.contains("NÉGATIF|REJET", case=False, na=False).sum()
    )
    taux = nb_np / total * 100
    ci_low_frac, ci_high_frac = proportion_confint(
        nb_np, total, alpha=alpha, method="wilson"
    )
    return {
        "nb_np":   nb_np,
        "total":   total,
        "taux":    round(taux, 2),
        "ci_low":  round(ci_low_frac * 100, 2),
        "ci_high": round(ci_high_frac * 100, 2),
    }


def compter_asb(df: pd.DataFrame) -> int:
    """Compte les bactériuries asymptomatiques (ASB) dans la colonne Recommandation."""
    if "Recommandation" not in df.columns:
        return 0
    return int(df["Recommandation"].str.contains("ASB", case=False, na=False).sum())


def compter_infections_decapitees(df: pd.DataFrame) -> int:
    """Compte les alertes d'infection décapitée (ATB + leucocyturie)."""
    return int(df[COL_DEC].str.contains("décapitée", case=False, na=False).sum())


def compter_prelev_risque(df: pd.DataFrame) -> int:
    """Compte les prélèvements à risque de contamination (Alerte Prélèvement != OK)."""
    if "Alerte Prélèvement" not in df.columns:
        return 0
    return int((df["Alerte Prélèvement"] != "OK").sum())


def stats_par_service(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les indicateurs de pilotage par service.

    Returns:
        DataFrame trié par Taux NP (%) décroissant, colonnes :
        Service, Total ECBU, Positifs, Non pertinents, Taux NP (%),
        Asymptomatiques, % Asymptomatiques.
    """
    rows = []
    for svc in sorted(df["Service"].dropna().unique()):
        sub    = df[df["Service"] == svc]
        n      = len(sub)
        n_np   = int(sub[COL_DEC].str.contains("NÉGATIF|REJET", case=False, na=False).sum())
        n_pos  = int(sub[COL_DEC].str.contains("POSITIF",       case=False, na=False).sum())
        n_asym = int((sub["Symptomatique"] == "Non").sum()) if "Symptomatique" in sub.columns else 0
        rows.append({
            "Service":           svc,
            "Total ECBU":        n,
            "Positifs":          n_pos,
            "Non pertinents":    n_np,
            "Taux NP (%)":       round(n_np / n * 100, 1) if n > 0 else 0.0,
            "Asymptomatiques":   n_asym,
            "% Asymptomatiques": round(n_asym / n * 100, 1) if n > 0 else 0.0,
        })
    _cols = ["Service", "Total ECBU", "Positifs", "Non pertinents",
             "Taux NP (%)", "Asymptomatiques", "% Asymptomatiques"]
    if not rows:
        return pd.DataFrame(columns=_cols)
    return pd.DataFrame(rows).sort_values("Taux NP (%)", ascending=False)
