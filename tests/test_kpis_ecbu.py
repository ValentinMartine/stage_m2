"""
Tests pour kpis_ecbu.py — fonctions KPI de pilotage ECBU.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pytest

from kpis_ecbu import (
    taux_non_pertinence,
    compter_asb,
    compter_infections_decapitees,
    compter_prelev_risque,
    stats_par_service,
)

COL_DEC = "Décision Algorithme"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def df_vide():
    return pd.DataFrame(columns=[
        COL_DEC, "Recommandation", "Alerte Prélèvement", "Service", "Symptomatique",
    ])


@pytest.fixture
def df_echantillon():
    return pd.DataFrame({
        COL_DEC: [
            "POSITIF : Germe prioritaire (seuil 10^3)",
            "NÉGATIF : Stérile",
            "REJET : Contamination probable",
            "ALERTE : Infection décapitée possible (ATB + leucocyturie)",
            "POSITIF : Homme (seuil 10^3)",
        ],
        "Recommandation": [
            None,
            "ASB probable — Ne pas traiter (IDSA/SPILF)",
            None,
            None,
            None,
        ],
        "Alerte Prélèvement": [
            "OK", "OK",
            "Prélèvement à risque de contamination",
            "OK", "OK",
        ],
        "Service": [
            "URGENCES", "GERIATRIE_A", "GERIATRIE_A", "REANIMATION", "URGENCES",
        ],
        "Symptomatique": ["Oui", "Non", "Non", "Non", "Oui"],
    })


# ---------------------------------------------------------------------------
# taux_non_pertinence
# ---------------------------------------------------------------------------

class TestTauxNonPertinence:

    def test_df_vide_retourne_zeros(self, df_vide):
        r = taux_non_pertinence(df_vide)
        assert r["total"] == 0
        assert r["taux"] == 0.0
        assert r["nb_np"] == 0

    def test_decompte_negatif_et_rejet(self, df_echantillon):
        r = taux_non_pertinence(df_echantillon)
        assert r["nb_np"] == 2      # NÉGATIF + REJET
        assert r["total"] == 5
        assert r["taux"] == 40.0

    def test_wilson_ci_dans_bornes(self, df_echantillon):
        r = taux_non_pertinence(df_echantillon)
        assert 0.0 <= r["ci_low"] <= r["taux"]
        assert r["taux"] <= r["ci_high"] <= 100.0

    def test_tous_np(self):
        df = pd.DataFrame({COL_DEC: ["NÉGATIF : Stérile"] * 5})
        r = taux_non_pertinence(df)
        assert r["taux"] == 100.0
        assert r["ci_high"] == 100.0

    def test_aucun_np(self):
        df = pd.DataFrame({COL_DEC: ["POSITIF : Germe prioritaire"] * 5})
        r = taux_non_pertinence(df)
        assert r["taux"] == 0.0
        assert r["ci_low"] == 0.0


# ---------------------------------------------------------------------------
# compter_asb
# ---------------------------------------------------------------------------

class TestCompterAsb:

    def test_vide(self, df_vide):
        assert compter_asb(df_vide) == 0

    def test_echantillon(self, df_echantillon):
        assert compter_asb(df_echantillon) == 1

    def test_sans_colonne_recommandation(self):
        df = pd.DataFrame({COL_DEC: ["NÉGATIF : Stérile"]})
        assert compter_asb(df) == 0


# ---------------------------------------------------------------------------
# compter_infections_decapitees
# ---------------------------------------------------------------------------

class TestCompterInfectionsDecapitees:

    def test_vide(self, df_vide):
        assert compter_infections_decapitees(df_vide) == 0

    def test_echantillon(self, df_echantillon):
        assert compter_infections_decapitees(df_echantillon) == 1


# ---------------------------------------------------------------------------
# compter_prelev_risque
# ---------------------------------------------------------------------------

class TestCompterPrelevRisque:

    def test_vide(self, df_vide):
        assert compter_prelev_risque(df_vide) == 0

    def test_echantillon(self, df_echantillon):
        assert compter_prelev_risque(df_echantillon) == 1

    def test_sans_colonne_alerte(self):
        df = pd.DataFrame({COL_DEC: ["NÉGATIF : Stérile"]})
        assert compter_prelev_risque(df) == 0


# ---------------------------------------------------------------------------
# stats_par_service
# ---------------------------------------------------------------------------

class TestStatsParService:

    def test_vide_retourne_dataframe_vide(self, df_vide):
        result = stats_par_service(df_vide)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_colonnes_presentes(self, df_echantillon):
        result = stats_par_service(df_echantillon)
        for col in ["Service", "Total ECBU", "Positifs", "Non pertinents", "Taux NP (%)"]:
            assert col in result.columns

    def test_trie_par_taux_np_decroissant(self, df_echantillon):
        result = stats_par_service(df_echantillon)
        if len(result) > 1:
            taux = result["Taux NP (%)"].tolist()
            assert taux == sorted(taux, reverse=True)

    def test_nb_services_correct(self, df_echantillon):
        result = stats_par_service(df_echantillon)
        assert len(result) == 3   # URGENCES, GERIATRIE_A, REANIMATION
