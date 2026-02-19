-- =============================================================================
-- SCRIPT SQL UNIFIÉ — SAD ECBU Avicenne v2
-- =============================================================================
-- Ce script crée :
--   1. La table de staging (import brut depuis Excel via Python)
--   2. La vue algorithmique avec la logique REMIC/SPILF complète
--
-- Ordre d'exécution : ce script d'abord, puis analyse_ecbu.py pour peupler
-- =============================================================================


-- NETTOYAGE
DROP VIEW  IF EXISTS v_algo_avicenne CASCADE;
DROP TABLE IF EXISTS orbis_export_brut CASCADE;


-- =============================================================================
-- 1. TABLE DE STAGING (réception de l'export Excel)
-- =============================================================================
-- Colonnes alignées sur le mapping du script Python analyse_ecbu.py
-- Les noms correspondent aux colonnes PostgreSQL après renommage.

CREATE TABLE orbis_export_brut (
    id_dossier_x99    VARCHAR(50),       -- IPP (sera hashé dans la vue)
    ddn_pat           TIMESTAMP,         -- Date de naissance
    code_genre        INTEGER,           -- 1 = Homme, 2 = Femme
    unit_func_lbl     VARCHAR(100),      -- Service / Unité fonctionnelle
    dt_prelevement    TIMESTAMP,         -- Date et heure du prélèvement
    mode_prelevement  VARCHAR(50),       -- MILIEU_JET, SONDE_DEMEURE, POCHE, etc.
    leucocyturie      INTEGER,           -- Leucocytes/mL (seuil significatif = 10 000)
    valeur_res_num    VARCHAR(50),       -- Bactériurie (UFC/mL) — peut contenir du texte sale
    germe_nom         VARCHAR(100),      -- Nom du germe identifié
    nb_especes        INTEGER DEFAULT 1, -- Nombre d'espèces bactériennes
    est_symptomatique INTEGER DEFAULT 0, -- 1 = symptômes urinaires, 0 = asymptomatique
    est_sonde         INTEGER DEFAULT 0, -- 1 = matériel en place (sonde/néphro)
    est_immunodeprime INTEGER DEFAULT 0, -- 1 = neutropénie / immunodépression
    est_enceinte      INTEGER DEFAULT 0, -- 1 = femme enceinte
    antibio_en_cours  INTEGER DEFAULT 0, -- 1 = antibiothérapie avant ECBU
    profil_resistance VARCHAR(30) DEFAULT 'Sensible' -- Sensible / BLSE / Carbapenemase / MRSA / Inconnu
);


-- =============================================================================
-- 2. VUE ALGORITHMIQUE — LOGIQUE MÉTIER COMPLÈTE
-- =============================================================================
-- Implémente les règles REMIC 2022 / SPILF 2015 / HAS 2023
-- Hiérarchie de décision (ordre de priorité) :
--   R0. Stérile → NÉGATIF
--   R1. Contamination (≥ 3 espèces ou flore polymorphe/cutanée) → REJET
--   R2. Matériel en place (sondé) + germe > 0 → POSITIF (pas de seuil)
--   R3. Immunodéprimé / neutropénique → POSITIF (culture même sans leucocyturie)
--   R4. Femme enceinte asymptomatique + germe → COLONISATION À TRAITER
--   R5. Infection décapitée (ATB + leucocyturie + stérile) → ALERTE
--   R6. Leucocyturie < 10^4 et pas d'exception → NÉGATIF (reflex culture)
--   R7. Germes prioritaires (E. coli, S. saprophyticus) : seuil ≥ 10^3
--   R8. Autres germes — Homme : seuil ≥ 10^3
--   R9. Autres germes — Femme : seuil ≥ 10^4
--   R10. Sous les seuils → NÉGATIF / COLONISATION

CREATE OR REPLACE VIEW v_algo_avicenne AS
WITH donnees AS (
    SELECT
        *,
        -- Nettoyage de la bactériurie (texte → nombre)
        COALESCE(
            CAST(
                NULLIF(regexp_replace(valeur_res_num, '[^0-9]', '', 'g'), '')
            AS INTEGER),
            0
        ) AS bacteriurie_num
    FROM orbis_export_brut
)
SELECT
    -- Identifiant anonymisé (SHA-256 tronqué avec sel long — PostgreSQL ≥ 11 natif)
    LEFT(encode(sha256((id_dossier_x99 || 'sel_avicenne_2026_ecbu_avicenne_limics')::bytea), 'hex'), 16) AS "ID Anonyme",

    -- Démographie
    CASE
        WHEN code_genre = 1 THEN 'Homme'
        WHEN code_genre = 2 THEN 'Femme'
        ELSE 'Inconnu'
    END AS "Sexe",

    EXTRACT(YEAR FROM age(CURRENT_DATE, ddn_pat))::INT AS "Age",

    -- Contexte
    unit_func_lbl       AS "Service",
    dt_prelevement      AS "Date Prélèvement",
    mode_prelevement    AS "Mode Prélèvement",

    -- Résultats
    leucocyturie        AS "Leucocyturie",
    germe_nom           AS "Bactérie",
    bacteriurie_num     AS "Bactériurie UFC/mL",
    nb_especes          AS "Nb Espèces",

    -- Contexte clinique
    CASE WHEN est_symptomatique = 1 THEN 'Oui' ELSE 'Non' END AS "Symptomatique",
    CASE WHEN est_sonde = 1         THEN 'Oui' ELSE 'Non' END AS "Sondé",
    CASE WHEN est_immunodeprime = 1 THEN 'Oui' ELSE 'Non' END AS "Immunodéprimé",
    CASE WHEN est_enceinte = 1      THEN 'Oui' ELSE 'Non' END AS "Enceinte",
    CASE WHEN antibio_en_cours = 1  THEN 'Oui' ELSE 'Non' END AS "ATB en cours",
    profil_resistance                                           AS "Profil Résistance",

    -- =========================================================================
    -- ALGORITHME DÉCISIONNEL
    -- =========================================================================
    CASE
        -- R0. Culture stérile sans leucocyturie
        WHEN germe_nom ILIKE '%Stérile%' AND leucocyturie < 10000 AND antibio_en_cours = 0
            THEN 'NÉGATIF : Stérile'

        -- R1. Contamination (polymicrobisme ≥ 3 espèces OU flore polymorphe/cutanée)
        WHEN nb_especes >= 3
            OR germe_nom ILIKE '%polymorphe%'
            OR germe_nom ILIKE '%cutanée%'
            THEN 'REJET : Contamination probable'

        -- R2. Matériel en place (sonde, néphrostomie) — pas de seuil de bactériurie
        WHEN est_sonde = 1 AND bacteriurie_num > 0
            THEN 'POSITIF : Matériel (pas de seuil)'

        -- R3. Patient immunodéprimé / neutropénique — culture justifiée même sans leucocyturie
        WHEN est_immunodeprime = 1 AND bacteriurie_num > 0
            THEN 'POSITIF : Immunodéprimé (exception leucocyturie)'

        -- R4. Femme enceinte — dépistage colonisation justifié (risque gravidique)
        WHEN est_enceinte = 1 AND bacteriurie_num >= 1000
            THEN 'POSITIF : Grossesse (dépistage colonisation)'

        -- R5. Infection décapitée — ATB en cours + leucocyturie + culture négative
        WHEN antibio_en_cours = 1 AND leucocyturie >= 10000
            AND (germe_nom ILIKE '%Stérile%' OR bacteriurie_num = 0)
            THEN 'ALERTE : Infection décapitée possible (ATB + leucocyturie)'

        -- R6. Leucocyturie < 10^4 sans exception → pas d'infection probable
        WHEN leucocyturie < 10000
            AND est_immunodeprime = 0
            AND est_sonde = 0
            AND est_enceinte = 0
            THEN 'NÉGATIF : Leucocyturie non significative'

        -- R7. Germes hautement uropathogènes (E. coli, S. saprophyticus) : seuil ≥ 10^3
        WHEN germe_nom IN ('Escherichia coli', 'Staphylococcus saprophyticus')
            AND bacteriurie_num >= 1000
            THEN 'POSITIF : Germe prioritaire (seuil 10^3)'

        -- R8. Autres germes — Homme : seuil ≥ 10^3
        WHEN code_genre = 1 AND bacteriurie_num >= 1000
            THEN 'POSITIF : Homme (seuil 10^3)'

        -- R9. Autres germes — Femme : seuil ≥ 10^4
        WHEN code_genre = 2 AND bacteriurie_num >= 10000
            THEN 'POSITIF : Femme (seuil 10^4)'

        -- R10. Sous les seuils
        ELSE 'NÉGATIF : Sous seuil / Colonisation'
    END AS "Décision Algorithme",

    -- =========================================================================
    -- ALERTE QUALITÉ PRÉLÈVEMENT
    -- =========================================================================
    CASE
        WHEN mode_prelevement IN ('POCHE', 'PENILEX')
            THEN 'Prélèvement à risque de contamination'
        WHEN mode_prelevement = 'SONDE_DEMEURE'
            THEN 'Sonde à demeure — interpréter avec prudence'
        ELSE 'OK'
    END AS "Alerte Prélèvement",

    -- =========================================================================
    -- RECOMMANDATION CLINIQUE
    -- =========================================================================
    CASE
        -- ASB chez personne âgée asymptomatique
        WHEN est_symptomatique = 0
            AND EXTRACT(YEAR FROM age(CURRENT_DATE, ddn_pat))::INT >= 75
            AND bacteriurie_num > 0
            AND est_enceinte = 0
            AND est_sonde = 0
            THEN 'ASB probable — Ne pas traiter (IDSA/SPILF)'

        -- Prescription systématique sans symptômes
        WHEN est_symptomatique = 0
            AND est_enceinte = 0
            AND est_immunodeprime = 0
            AND est_sonde = 0
            AND germe_nom NOT ILIKE '%Stérile%'
            THEN 'Prescription sans symptôme — pertinence à évaluer'

        -- Prélèvement de mauvaise qualité
        WHEN mode_prelevement IN ('POCHE', 'PENILEX')
            AND nb_especes >= 2
            THEN 'Recontrôler sur milieu de jet ou sondage AR'

        ELSE NULL
    END AS "Recommandation"

FROM donnees;
