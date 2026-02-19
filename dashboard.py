"""
Dashboard Streamlit — Pilotage Juste Prescription ECBU
v2 : tendances temporelles, analyse par service, indicateurs de sécurité,
     alertes ASB, qualité prélèvement, credentials .env.

Lancement :
    python -m streamlit run dashboard.py
"""

import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, '.env')

try:
    from dotenv import load_dotenv
    load_dotenv(ENV_PATH)
except ImportError:
    pass

# =============================================================================
# CONFIG
# =============================================================================
st.set_page_config(page_title="Pilotage ECBU — Avicenne", page_icon="🏥", layout="wide")

DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', '')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'postgres')

@st.cache_resource
def init_connection():
    return create_engine(
        f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    )

# =============================================================================
# CHARGEMENT
# =============================================================================
try:
    engine = init_connection()
    df = pd.read_sql("SELECT * FROM v_algo_avicenne", engine)
except Exception as e:
    st.error(f"Connexion impossible : {e}")
    st.info("Vérifiez votre fichier .env et que PostgreSQL est actif.")
    st.stop()

if df.empty:
    st.warning("Aucune donnée dans la vue v_algo_avicenne. Exécutez analyse_ecbu.py d'abord.")
    st.stop()

# =============================================================================
# SIDEBAR — FILTRES
# =============================================================================
st.sidebar.header("🔍 Filtres")

# Services
services = sorted(df['Service'].unique())
choix_services = st.sidebar.multiselect("Service(s)", services, default=services)

# Sexe
sexes = sorted(df['Sexe'].unique())
choix_sexe = st.sidebar.multiselect("Sexe", sexes, default=sexes)

# Tranche d'âge
age_min, age_max = int(df['Age'].min()), int(df['Age'].max())
choix_age = st.sidebar.slider("Tranche d'âge", age_min, age_max, (age_min, age_max))

# Symptomatique
choix_sympt = st.sidebar.multiselect("Symptomatique ?", ['Oui', 'Non'], default=['Oui', 'Non'])

# Application des filtres
mask = (
    df['Service'].isin(choix_services)
    & df['Sexe'].isin(choix_sexe)
    & df['Age'].between(choix_age[0], choix_age[1])
    & df['Symptomatique'].isin(choix_sympt)
)
df_f = df[mask].copy()

# =============================================================================
# EN-TÊTE
# =============================================================================
st.title("🏥 Pilotage Juste Prescription ECBU — Avicenne")
st.caption(f"{len(df_f)} ECBU affichés sur {len(df)} au total")

# =============================================================================
# KPIs PRINCIPAUX
# =============================================================================
total = len(df_f)
col_dec = "Décision Algorithme"

nb_positif = df_f[col_dec].str.contains('POSITIF', case=False, na=False).sum()
nb_negatif = df_f[col_dec].str.contains('NÉGATIF', case=False, na=False).sum()
nb_rejet   = df_f[col_dec].str.contains('REJET', case=False, na=False).sum()
nb_alerte  = df_f[col_dec].str.contains('ALERTE', case=False, na=False).sum()
nb_np = nb_negatif + nb_rejet
taux_np = nb_np / total * 100 if total > 0 else 0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total", total)
c2.metric("Positifs", nb_positif)
c3.metric("Négatifs", nb_negatif)
c4.metric("Rejets", nb_rejet, help="Contamination / flore polymorphe")
c5.metric("Taux non-pertinence", f"{taux_np:.1f}%", delta=f"{nb_np} ECBU",
          delta_color="inverse")

st.divider()

# =============================================================================
# ONGLETS
# =============================================================================
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Vue d'ensemble",
    "📈 Tendances",
    "🏥 Par service",
    "⚠️ Sécurité & Alertes",
    "📋 Données brutes"
])

# ---- TAB 1 : VUE D'ENSEMBLE ----
with tab1:
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Répartition des décisions")
        chart_dec = df_f[col_dec].value_counts().reset_index()
        chart_dec.columns = ['Décision', 'Nombre']
        st.bar_chart(chart_dec, x='Décision', y='Nombre', horizontal=True)

    with col_b:
        st.subheader("Répartition par germe")
        chart_germe = df_f['Bactérie'].value_counts().head(8).reset_index()
        chart_germe.columns = ['Germe', 'Nombre']
        st.bar_chart(chart_germe, x='Germe', y='Nombre', horizontal=True)

    col_c, col_d = st.columns(2)
    with col_c:
        st.subheader("Symptomatique vs Asymptomatique")
        chart_sympt = df_f['Symptomatique'].value_counts().reset_index()
        chart_sympt.columns = ['Symptomatique', 'Nombre']
        st.bar_chart(chart_sympt, x='Symptomatique', y='Nombre')

    with col_d:
        st.subheader("Mode de prélèvement")
        chart_mode = df_f['Mode Prélèvement'].value_counts().reset_index()
        chart_mode.columns = ['Mode', 'Nombre']
        st.bar_chart(chart_mode, x='Mode', y='Nombre', horizontal=True)

# ---- TAB 2 : TENDANCES TEMPORELLES ----
with tab2:
    st.subheader("Évolution temporelle")

    if 'Date Prélèvement' in df_f.columns and df_f['Date Prélèvement'].notna().any():
        df_t = df_f.copy()
        df_t['Date Prélèvement'] = pd.to_datetime(df_t['Date Prélèvement'], errors='coerce')
        df_t['Jour'] = df_t['Date Prélèvement'].dt.date

        # ECBU par jour
        par_jour = df_t.groupby('Jour').agg(
            Total=('Jour', 'size'),
            Non_Pertinents=(col_dec, lambda x: x.str.contains('NÉGATIF|REJET', case=False, na=False).sum())
        ).reset_index()
        par_jour['Taux NP (%)'] = (par_jour['Non_Pertinents'] / par_jour['Total'] * 100).round(1)

        st.line_chart(par_jour.set_index('Jour')[['Total', 'Non_Pertinents']])
        st.caption("Nombre d'ECBU (total vs non pertinents) par jour de prélèvement")

        st.line_chart(par_jour.set_index('Jour')[['Taux NP (%)']])
        st.caption("Taux de non-pertinence quotidien (%)")
    else:
        st.info("Pas de dates de prélèvement disponibles pour les tendances.")

# ---- TAB 3 : PAR SERVICE ----
with tab3:
    st.subheader("Indicateurs par service")

    stats_service = []
    for svc in sorted(df_f['Service'].unique()):
        sub = df_f[df_f['Service'] == svc]
        n = len(sub)
        n_np = sub[col_dec].str.contains('NÉGATIF|REJET', case=False, na=False).sum()
        n_pos = sub[col_dec].str.contains('POSITIF', case=False, na=False).sum()
        n_asympt = (sub['Symptomatique'] == 'Non').sum()
        stats_service.append({
            'Service': svc,
            'Total ECBU': n,
            'Positifs': n_pos,
            'Non pertinents': n_np,
            'Taux NP (%)': round(n_np / n * 100, 1) if n > 0 else 0,
            'Asymptomatiques': n_asympt,
            '% Asymptomatiques': round(n_asympt / n * 100, 1) if n > 0 else 0,
        })

    df_svc = pd.DataFrame(stats_service).sort_values('Taux NP (%)', ascending=False)
    st.dataframe(df_svc, use_container_width=True, hide_index=True)

    st.bar_chart(df_svc.set_index('Service')[['Taux NP (%)', '% Asymptomatiques']])

# ---- TAB 4 : SÉCURITÉ & ALERTES ----
with tab4:
    st.subheader("Indicateurs de sécurité")

    col_s1, col_s2, col_s3, col_s4 = st.columns(4)

    # ASB détectées
    nb_asb = df_f['Recommandation'].str.contains('ASB', case=False, na=False).sum() if 'Recommandation' in df_f.columns else 0
    col_s1.metric("ASB identifiées", nb_asb, help="Bactériuries asymptomatiques chez >75 ans")

    # Infections décapitées
    nb_decap = df_f[col_dec].str.contains('décapitée', case=False, na=False).sum()
    col_s2.metric("Infections décapitées", nb_decap, help="ATB en cours + leucocyturie + culture négative")

    # Immunodéprimés avec infection
    nb_immuno = df_f[col_dec].str.contains('Immunodéprimé', case=False, na=False).sum()
    col_s3.metric("Immunodép. infectés", nb_immuno)

    # Prélèvements à risque
    nb_prelev_risque = (df_f['Alerte Prélèvement'] != 'OK').sum() if 'Alerte Prélèvement' in df_f.columns else 0
    col_s4.metric("Prélèvements à risque", nb_prelev_risque)

    st.divider()

    # Détail des alertes
    if 'Recommandation' in df_f.columns:
        df_reco = df_f[df_f['Recommandation'].notna()][
            ['ID Anonyme', 'Sexe', 'Age', 'Service', 'Bactérie',
             'Symptomatique', col_dec, 'Recommandation']
        ]
        if not df_reco.empty:
            st.subheader("Détail des recommandations cliniques")
            st.dataframe(df_reco, use_container_width=True, hide_index=True)
        else:
            st.success("Aucune alerte clinique sur la sélection courante.")

# ---- TAB 5 : DONNÉES BRUTES ----
with tab5:
    st.subheader("Données complètes")
    st.dataframe(df_f, use_container_width=True, hide_index=True)

    # Export CSV
    csv = df_f.to_csv(index=False).encode('utf-8')
    st.download_button("Télécharger en CSV", csv, "export_ecbu.csv", "text/csv")

# =============================================================================
# FOOTER
# =============================================================================
st.divider()
st.caption("SAD ECBU — Hôpital Avicenne / LIMICS | Algorithme REMIC 2022 / SPILF 2015")
