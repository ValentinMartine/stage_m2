# Importation de la fonction pour l'analyse de puissance (power analysis) sur deux proportions
from statsmodels.stats.power import zt_ind_solve_power
import statsmodels.api as sm

# Définition des hypothèses attendues pour le test
prop_avant = 0.60  # Estimation : 60% des ECBU partaient en culture avant l'outil
prop_apres = 0.40  # Objectif : descendre à 40% de mises en culture

# Calcul de la taille de l'effet (effect size) avec la formule de Cohen
effect_size = sm.stats.proportion_effectsize(prop_avant, prop_apres)

# Calcul du N nécessaire par groupe (Avant / Après)
n_necessaire = zt_ind_solve_power(
    effect_size=effect_size, 
    alpha=0.05,        # Seuil de significativité p < 0.05
    power=0.80,        # Puissance statistique visée de 80%
    ratio=1.0,         # Autant de jours/patients dans la période 'avant' que 'après'
    alternative='two-sided'
)

print(f"Taille d'échantillon nécessaire par période : {round(n_necessaire)} ECBU")