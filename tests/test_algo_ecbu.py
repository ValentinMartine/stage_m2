"""
Tests pour algo_ecbu.py — couverture des règles cliniques R0–R10.
Un test par règle, libellés identiques au SQL (v_algo_avicenne).
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pytest

from algo_ecbu import appliquer_algorithme


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def cas(**kwargs) -> pd.DataFrame:
    """Crée un DataFrame d'une ligne avec valeurs par défaut sûres (NÉGATIF : Stérile)."""
    defaults = {
        "bacteriurie_num":   0,
        "leucocyturie":      0,
        "germe_nom":         "Stérile",
        "nb_especes":        0,
        "est_sonde":         0,
        "est_immunodeprime": 0,
        "est_enceinte":      0,
        "antibio_en_cours":  0,
        "code_genre":        2,
    }
    defaults.update(kwargs)
    return pd.DataFrame([defaults])


def decision(**kwargs) -> str:
    return appliquer_algorithme(cas(**kwargs)).iloc[0]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestAlgorithmeR0R10:

    def test_R0_sterile(self):
        """R0 : Culture stérile + leucocyturie < 10 000 + sans ATB → NÉGATIF : Stérile."""
        assert decision(germe_nom="Stérile", leucocyturie=0, antibio_en_cours=0) == \
               "NÉGATIF : Stérile"

    def test_R1_contamination_nb_especes(self):
        """R1 : ≥ 3 espèces → REJET : Contamination probable."""
        assert decision(
            germe_nom="Escherichia coli", nb_especes=3,
            bacteriurie_num=100000, leucocyturie=50000,
        ) == "REJET : Contamination probable"

    def test_R1_contamination_polymorphe(self):
        """R1 : Flore polymorphe → REJET : Contamination probable."""
        assert decision(
            germe_nom="Flore polymorphe", bacteriurie_num=5000, leucocyturie=10000,
        ) == "REJET : Contamination probable"

    def test_R2_materiel_en_place(self):
        """R2 : Patient sondé + bactériurie > 0 → POSITIF : Matériel."""
        assert decision(
            est_sonde=1, bacteriurie_num=100,
            germe_nom="Escherichia coli", leucocyturie=0,
        ) == "POSITIF : Matériel (pas de seuil)"

    def test_R3_immunodeprime(self):
        """R3 : Immunodéprimé + bactériurie > 0 → POSITIF : Immunodéprimé."""
        assert decision(
            est_immunodeprime=1, bacteriurie_num=500,
            germe_nom="Klebsiella pneumoniae", leucocyturie=0,
        ) == "POSITIF : Immunodéprimé (exception leucocyturie)"

    def test_R4_grossesse(self):
        """R4 : Femme enceinte + bactériurie ≥ 10^3 → POSITIF : Grossesse."""
        assert decision(
            est_enceinte=1, bacteriurie_num=1000,
            germe_nom="Escherichia coli", leucocyturie=10000, code_genre=2,
        ) == "POSITIF : Grossesse (dépistage colonisation)"

    def test_R5_infection_decapitee(self):
        """R5 : ATB + leucocyturie ≥ 10 000 + culture stérile → ALERTE."""
        assert decision(
            antibio_en_cours=1, leucocyturie=50000,
            germe_nom="Stérile", bacteriurie_num=0,
        ) == "ALERTE : Infection décapitée possible (ATB + leucocyturie)"

    def test_R6_leucocyturie_basse(self):
        """R6 : Leucocyturie < 10^4 sans exception → NÉGATIF : Leucocyturie non significative."""
        assert decision(
            leucocyturie=5000, germe_nom="Escherichia coli", bacteriurie_num=100000,
        ) == "NÉGATIF : Leucocyturie non significative"

    def test_R7_germe_prioritaire_ecoli(self):
        """R7 : E. coli + bactériurie ≥ 10^3 + leucocyturie OK → POSITIF : Germe prioritaire."""
        assert decision(
            germe_nom="Escherichia coli", bacteriurie_num=1000, leucocyturie=10000,
        ) == "POSITIF : Germe prioritaire (seuil 10^3)"

    def test_R7_germe_prioritaire_saprophyticus(self):
        """R7 : S. saprophyticus + bactériurie ≥ 10^3 → POSITIF : Germe prioritaire."""
        assert decision(
            germe_nom="Staphylococcus saprophyticus",
            bacteriurie_num=5000, leucocyturie=10000,
        ) == "POSITIF : Germe prioritaire (seuil 10^3)"

    def test_R8_homme_seuil_1000(self):
        """R8 : Homme + germe non-prioritaire + bactériurie ≥ 10^3 → POSITIF : Homme."""
        assert decision(
            code_genre=1, germe_nom="Klebsiella pneumoniae",
            bacteriurie_num=1000, leucocyturie=10000,
        ) == "POSITIF : Homme (seuil 10^3)"

    def test_R9_femme_seuil_10000(self):
        """R9 : Femme + germe non-prioritaire + bactériurie ≥ 10^4 → POSITIF : Femme."""
        assert decision(
            code_genre=2, germe_nom="Proteus mirabilis",
            bacteriurie_num=10000, leucocyturie=10000,
        ) == "POSITIF : Femme (seuil 10^4)"

    def test_R10_sous_seuil_femme(self):
        """R10 : Femme + bactériurie < 10^4 + leucocyturie OK → NÉGATIF : Sous seuil."""
        assert decision(
            code_genre=2, germe_nom="Proteus mirabilis",
            bacteriurie_num=5000, leucocyturie=10000,
        ) == "NÉGATIF : Sous seuil / Colonisation"

    def test_R10_sous_seuil_homme(self):
        """R10 : Homme + bactériurie < 10^3 → NÉGATIF : Sous seuil."""
        assert decision(
            code_genre=1, germe_nom="Proteus mirabilis",
            bacteriurie_num=500, leucocyturie=10000,
        ) == "NÉGATIF : Sous seuil / Colonisation"
