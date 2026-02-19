"""
Tests unitaires pour les fonctions utilitaires du générateur ECBU.
"""
import sys
import os
import re
from datetime import datetime

# Permet d'importer le module depuis la racine du projet
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Imports isolés (sans exécuter le bloc principal du générateur)
from generateur_donnees import (
    generer_ipp,
    generer_date_naissance,
    generer_date_prelevement,
    formater_quantite,
    generer_profil_geriatrie_asb,
    generer_profil_iu_classique,
    generer_profil_contamination,
    generer_profil_sterile,
    generer_profil_sonde_uro,
    generer_profil_reanimation,
    generer_profil_infection_decapitee,
    GERMES_PRIORITAIRES,
    GERMES_AUTRES,
    GERMES_CONTAMINATION,
    MODES_PRELEVEMENT,
    SERVICES,
)

CHAMPS_PROFIL = {
    'sexe', 'cat_age', 'service', 'symptomes', 'germe', 'quantite',
    'leucocyturie', 'mode_prelevement', 'nb_especes', 'est_sonde',
    'est_immunodeprime', 'est_enceinte', 'antibio_en_cours',
}


# =============================================================================
# generer_ipp
# =============================================================================

class TestGenererIpp:
    def test_format(self):
        assert generer_ipp(1) == "800" + "00001"

    def test_zero_padding(self):
        assert generer_ipp(42) == "80000042"

    def test_longueur(self):
        # Toujours 8 caractères : "800" + 5 chiffres
        for i in [1, 99, 9999]:
            assert len(generer_ipp(i)) == 8

    def test_prefixe(self):
        assert generer_ipp(123).startswith("800")


# =============================================================================
# generer_date_naissance
# =============================================================================

class TestGenererDateNaissance:
    def _age_en_ans(self, ddn):
        return (datetime.now() - ddn).days // 365

    def test_bebe(self):
        ddn = generer_date_naissance('bebe')
        age = self._age_en_ans(ddn)
        assert 0 <= age <= 2

    def test_enfant(self):
        ddn = generer_date_naissance('enfant')
        age = self._age_en_ans(ddn)
        assert 3 <= age <= 17

    def test_adulte(self):
        ddn = generer_date_naissance('adulte')
        age = self._age_en_ans(ddn)
        assert 18 <= age <= 74

    def test_vieux(self):
        ddn = generer_date_naissance('vieux')
        age = self._age_en_ans(ddn)
        assert 75 <= age <= 100

    def test_retourne_datetime(self):
        assert isinstance(generer_date_naissance('adulte'), datetime)


# =============================================================================
# generer_date_prelevement
# =============================================================================

class TestGenererDatePrelevement:
    def test_retourne_datetime(self):
        assert isinstance(generer_date_prelevement(), datetime)

    def test_mois_mars_2026(self):
        d = generer_date_prelevement()
        assert d.year == 2026
        assert d.month == 3

    def test_jour_valide(self):
        d = generer_date_prelevement()
        assert 1 <= d.day <= 31

    def test_heure_journee(self):
        d = generer_date_prelevement()
        assert 6 <= d.hour <= 22


# =============================================================================
# formater_quantite
# =============================================================================

class TestFormaterQuantite:
    def test_zero_retourne_zero(self):
        assert formater_quantite(0) == "0"

    def test_retourne_string(self):
        assert isinstance(formater_quantite(1000), str)

    def test_valeur_dans_resultat(self):
        # La valeur numérique doit apparaître dans la chaîne (format normal ou "> x")
        for q in [1000, 10000, 100000]:
            result = formater_quantite(q)
            assert str(q) in result

    def test_format_normal_ou_prefixe(self):
        q = 100000
        result = formater_quantite(q)
        assert result == str(q) or re.match(r"^> \d+$", result)


# =============================================================================
# Profils cliniques — structure et cohérence
# =============================================================================

class TestProfilsStructure:
    """Vérifie que chaque profil renvoie un dict avec les bons champs."""

    def _verifier_profil(self, profil):
        assert isinstance(profil, dict)
        assert CHAMPS_PROFIL == set(profil.keys())

    def test_geriatrie_asb(self):
        self._verifier_profil(generer_profil_geriatrie_asb())

    def test_iu_classique(self):
        self._verifier_profil(generer_profil_iu_classique())

    def test_contamination(self):
        self._verifier_profil(generer_profil_contamination())

    def test_sterile(self):
        self._verifier_profil(generer_profil_sterile())

    def test_sonde_uro(self):
        self._verifier_profil(generer_profil_sonde_uro())

    def test_reanimation(self):
        self._verifier_profil(generer_profil_reanimation())

    def test_infection_decapitee(self):
        self._verifier_profil(generer_profil_infection_decapitee())


class TestProfilsCoherence:
    """Vérifie les invariants cliniques de chaque profil."""

    def test_geriatrie_asb_femme_asymptomatique(self):
        p = generer_profil_geriatrie_asb()
        assert p['sexe'] == 2       # femme
        assert p['symptomes'] == 0  # asymptomatique
        assert p['service'] == 'GERIATRIE_A'
        assert p['germe'] == 'Escherichia coli'
        assert not p['est_sonde']

    def test_sterile_quantite_nulle(self):
        p = generer_profil_sterile()
        assert p['quantite'] == 0
        assert p['germe'] == 'Stérile'
        assert p['nb_especes'] == 0

    def test_contamination_plusieurs_especes(self):
        p = generer_profil_contamination()
        assert p['nb_especes'] >= 3
        assert p['germe'] in GERMES_CONTAMINATION

    def test_sonde_uro_est_sonde(self):
        p = generer_profil_sonde_uro()
        assert p['est_sonde'] is True
        assert p['service'] == 'UROLOGIE_B'
        assert p['mode_prelevement'] in ['SONDE_DEMEURE', 'SONDE_JJ', 'NEPHROSTOMIE']

    def test_reanimation_sonde_sedation(self):
        p = generer_profil_reanimation()
        assert p['est_sonde'] is True
        assert p['symptomes'] == 0   # patient sédaté
        assert p['service'] == 'REANIMATION'

    def test_infection_decapitee_antibio_leucocyturie(self):
        p = generer_profil_infection_decapitee()
        assert p['antibio_en_cours'] is True
        assert p['germe'] == 'Stérile'
        assert p['leucocyturie'] > 0   # leucocyturie persistante malgré ATB
        assert p['symptomes'] == 1

    def test_germe_iu_classique_connu(self):
        tous_germes = GERMES_PRIORITAIRES + GERMES_AUTRES + GERMES_CONTAMINATION + ['Stérile']
        for _ in range(20):
            p = generer_profil_iu_classique()
            assert p['germe'] in tous_germes

    def test_mode_prelevement_valide(self):
        for fn in [generer_profil_geriatrie_asb, generer_profil_sterile,
                   generer_profil_sonde_uro, generer_profil_reanimation]:
            p = fn()
            assert p['mode_prelevement'] in MODES_PRELEVEMENT

    def test_service_valide(self):
        # Les services spéciaux (GYNECOLOGIE) peuvent apparaître dans certains profils
        services_etendus = SERVICES + ['GYNECOLOGIE']
        for fn in [generer_profil_geriatrie_asb, generer_profil_sterile,
                   generer_profil_sonde_uro, generer_profil_reanimation]:
            p = fn()
            assert p['service'] in services_etendus
