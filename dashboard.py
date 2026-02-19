"""
Dashboard Streamlit — Pilotage Juste Prescription ECBU
v4 : Wilson CI, authentification optionnelle, donut résistances,
     bouton alerte email, onglet prédiction ML, export PDF.

Lancement :
    python -m streamlit run dashboard.py
"""

import os
from datetime import datetime
from io import BytesIO

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from fpdf import FPDF
from sqlalchemy import create_engine

from kpis_ecbu import (
    taux_non_pertinence,
    compter_asb,
    compter_infections_decapitees,
    compter_prelev_risque,
    stats_par_service,
)

# =============================================================================
# PALETTE — couleurs par décision clinique
# =============================================================================
COULEURS_DECISION = {
    "POSITIF":    "#2ecc71",
    "NÉGATIF":    "#95a5a6",
    "REJET":      "#e67e22",
    "ALERTE":     "#e74c3c",
    "TRAITEMENT": "#3498db",
}

COULEURS_RESISTANCE = {
    "Sensible":      "#2ecc71",
    "BLSE":          "#e67e22",
    "Carbapenemase": "#e74c3c",
    "MRSA":          "#9b59b6",
    "Inconnu":       "#bdc3c7",
}


def couleur_decision(libelle: str) -> str:
    for cle, couleur in COULEURS_DECISION.items():
        if cle in str(libelle).upper():
            return couleur
    return "#bdc3c7"


# =============================================================================
# EXPORT PDF
# =============================================================================

def generer_rapport_pdf(df_f: pd.DataFrame, kpis: dict, df_svc: pd.DataFrame) -> bytes:
    """Génère un rapport PDF de pilotage ECBU avec KPIs et stats par service."""

    class _PDF(FPDF):
        def header(self):
            self.set_font("Helvetica", "B", 13)
            self.cell(0, 8, "Rapport de Pilotage ECBU - Hopital Avicenne",
                      align="C", new_x="LMARGIN", new_y="NEXT")
            self.set_font("Helvetica", "", 8)
            self.set_text_color(120, 120, 120)
            self.cell(
                0, 5,
                f"Genere le {datetime.now().strftime('%d/%m/%Y a %H:%M')} | "
                f"Algorithme REMIC 2022 / SPILF 2015 / HAS 2023",
                align="C", new_x="LMARGIN", new_y="NEXT",
            )
            self.set_text_color(0, 0, 0)
            self.ln(2)
            self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
            self.ln(4)

        def footer(self):
            self.set_y(-14)
            self.set_font("Helvetica", "I", 7)
            self.set_text_color(150, 150, 150)
            self.cell(0, 8, f"SAD ECBU - LIMICS | Page {self.page_no()}", align="C")

    pdf = _PDF()
    pdf.set_margins(15, 18, 15)
    pdf.add_page()

    # KPIs
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 7, "Indicateurs cles", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(1)
    ci_str = (
        f"{kpis['ci_low']:.1f}% - {kpis['ci_high']:.1f}%"
        if "ci_low" in kpis else "N/A"
    )
    kpi_rows = [
        ("Total ECBU analyses",          str(kpis["total"])),
        ("Positifs",                      str(kpis["nb_positif"])),
        ("Negatifs",                      str(kpis["nb_negatif"])),
        ("Rejets (contamination)",        str(kpis["nb_rejet"])),
        ("Alertes cliniques",             str(kpis["nb_alerte"])),
        ("Taux de non-pertinence",        f"{kpis['taux_np']:.1f}%"),
        ("IC 95% Wilson",                 ci_str),
    ]
    cw = [110, 55]
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(220, 235, 255)
    pdf.cell(cw[0], 6, "Indicateur", border=1, fill=True)
    pdf.cell(cw[1], 6, "Valeur", border=1, fill=True, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    for idx, (label, val) in enumerate(kpi_rows):
        fill = idx % 2 == 0
        pdf.set_fill_color(245, 249, 255) if fill else pdf.set_fill_color(255, 255, 255)
        pdf.cell(cw[0], 5, label, border=1, fill=fill)
        pdf.cell(cw[1], 5, val,   border=1, fill=fill, new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)

    # Par service
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, 7, "Statistiques par service", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(1)
    cols_svc   = ["Service", "Total ECBU", "Positifs", "Non pertinents", "Taux NP (%)"]
    widths_svc = [55, 26, 26, 36, 26]
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(220, 235, 255)
    for col, w in zip(cols_svc, widths_svc):
        pdf.cell(w, 6, col, border=1, fill=True, align="C")
    pdf.ln()
    pdf.set_font("Helvetica", "", 8)
    for idx, row in enumerate(df_svc[cols_svc].itertuples(index=False)):
        fill = idx % 2 == 0
        pdf.set_fill_color(245, 249, 255) if fill else pdf.set_fill_color(255, 255, 255)
        for val, w in zip(row, widths_svc):
            pdf.cell(w, 5, str(val), border=1, fill=fill, align="C")
        pdf.ln()
    pdf.ln(4)
    pdf.set_font("Helvetica", "I", 7)
    pdf.set_text_color(120, 120, 120)
    pdf.cell(
        0, 5,
        f"Donnees filtrees : {len(df_f)} ECBU | "
        f"Periode : {df_f['Date Prélèvement'].min() if 'Date Prélèvement' in df_f else 'N/A'}"
        f" - {df_f['Date Prélèvement'].max() if 'Date Prélèvement' in df_f else 'N/A'}",
        new_x="LMARGIN", new_y="NEXT",
    )
    buf = BytesIO()
    buf.write(bytes(pdf.output()))
    return buf.getvalue()


# =============================================================================
# CONNEXION
# =============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH   = os.path.join(SCRIPT_DIR, ".env")

try:
    from dotenv import load_dotenv
    load_dotenv(ENV_PATH)
except ImportError:
    pass

st.set_page_config(
    page_title="Pilotage ECBU — Avicenne",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# AUTHENTIFICATION OPTIONNELLE
# =============================================================================
_SECRETS_PATH = os.path.join(SCRIPT_DIR, ".streamlit", "secrets.toml")
_auth_active  = os.path.isfile(_SECRETS_PATH) and os.path.getsize(_SECRETS_PATH) > 0

if _auth_active:
    try:
        import streamlit_authenticator as stauth

        credentials  = dict(st.secrets.get("credentials", {}))
        cookie_conf  = dict(st.secrets.get("cookie", {}))
        authenticator = stauth.Authenticate(
            credentials,
            cookie_conf.get("name", "ecbu_auth"),
            cookie_conf.get("key", "dev_key"),
            cookie_conf.get("expiry_days", 1),
        )
        authenticator.login()
        _status = st.session_state.get("authentication_status")
        if _status is False:
            st.error("Identifiant ou mot de passe incorrect.")
            st.stop()
        elif _status is None:
            st.warning("Veuillez vous connecter pour accéder au dashboard.")
            st.stop()
        else:
            authenticator.logout(location="sidebar")
    except Exception as _auth_err:
        st.info(f"Authentification désactivée (mode dev) : {_auth_err}")
else:
    st.sidebar.caption("🔓 Mode développement — authentification désactivée")

# =============================================================================
# CHARGEMENT DES DONNÉES
# =============================================================================
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")


@st.cache_resource
def init_connection():
    return create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


try:
    engine = init_connection()
    df = pd.read_sql("SELECT * FROM v_algo_avicenne", engine)
except Exception as e:
    st.error(f"Connexion impossible : {e}")
    st.info("Vérifiez votre fichier .env et que PostgreSQL est actif.")
    st.stop()

if df.empty:
    st.warning("Aucune donnée dans v_algo_avicenne. Exécutez analyse_ecbu.py d'abord.")
    st.stop()

col_dec = "Décision Algorithme"

# =============================================================================
# SIDEBAR — FILTRES
# =============================================================================
st.sidebar.header("🔍 Filtres")

services       = sorted(df["Service"].unique())
choix_services = st.sidebar.multiselect("Service(s)", services, default=services)

sexes     = sorted(df["Sexe"].unique())
choix_sexe = st.sidebar.multiselect("Sexe", sexes, default=sexes)

age_min, age_max = int(df["Age"].min()), int(df["Age"].max())
choix_age = st.sidebar.slider("Tranche d'âge", age_min, age_max, (age_min, age_max))

choix_sympt = st.sidebar.multiselect(
    "Symptomatique ?", ["Oui", "Non"], default=["Oui", "Non"]
)

mask = (
    df["Service"].isin(choix_services)
    & df["Sexe"].isin(choix_sexe)
    & df["Age"].between(choix_age[0], choix_age[1])
    & df["Symptomatique"].isin(choix_sympt)
)
df_f = df[mask].copy()

# =============================================================================
# KPIs — via kpis_ecbu (Wilson CI inclus)
# =============================================================================
kpi_tnp  = taux_non_pertinence(df_f)
total    = kpi_tnp["total"]
nb_np    = kpi_tnp["nb_np"]
taux_np  = kpi_tnp["taux"]
ci_low   = kpi_tnp["ci_low"]
ci_high  = kpi_tnp["ci_high"]

nb_positif = int(df_f[col_dec].str.contains("POSITIF",  case=False, na=False).sum())
nb_negatif = int(df_f[col_dec].str.contains("NÉGATIF",  case=False, na=False).sum())
nb_rejet   = int(df_f[col_dec].str.contains("REJET",    case=False, na=False).sum())
nb_alerte  = int(df_f[col_dec].str.contains("ALERTE",   case=False, na=False).sum())

# =============================================================================
# STATS PAR SERVICE
# =============================================================================
df_svc = stats_par_service(df_f)

# =============================================================================
# SIDEBAR — ALERTES EMAIL
# =============================================================================
st.sidebar.divider()
st.sidebar.subheader("⚠️ Alertes NP")

_seuil_alerte = st.sidebar.slider(
    "Seuil d'alerte (%)", min_value=10, max_value=80,
    value=int(os.getenv("ALERT_NP_SEUIL", "40")), step=5,
)

if st.sidebar.button("Vérifier dépassements"):
    from alertes import verifier_seuil_np
    alertes = verifier_seuil_np(df_f, seuil=_seuil_alerte)
    if alertes:
        st.sidebar.warning(f"{len(alertes)} service(s) en dépassement :")
        for a in alertes:
            st.sidebar.write(f"• **{a['service']}** — {a['taux_np']:.1f}%")
        if st.sidebar.button("Envoyer email d'alerte"):
            from alertes import envoyer_alerte_email
            ok = envoyer_alerte_email(alertes)
            if ok:
                st.sidebar.success("Email envoyé.")
            else:
                st.sidebar.error("Échec envoi — vérifiez les variables ALERT_* dans .env")
    else:
        st.sidebar.success(f"Aucun service au-dessus de {_seuil_alerte}%")

# =============================================================================
# SIDEBAR — EXPORT PDF
# =============================================================================
st.sidebar.divider()
st.sidebar.subheader("Export PDF")
if st.sidebar.button("Générer rapport PDF"):
    kpis_dict = {
        "total":      total,
        "nb_positif": nb_positif,
        "nb_negatif": nb_negatif,
        "nb_rejet":   nb_rejet,
        "nb_alerte":  nb_alerte,
        "taux_np":    taux_np,
        "ci_low":     ci_low,
        "ci_high":    ci_high,
    }
    pdf_bytes = generer_rapport_pdf(df_f, kpis_dict, df_svc)
    st.sidebar.download_button(
        label="Télécharger le rapport",
        data=pdf_bytes,
        file_name=f"rapport_ecbu_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
        mime="application/pdf",
    )

# =============================================================================
# EN-TÊTE
# =============================================================================
st.title("🏥 Pilotage Juste Prescription ECBU — Avicenne")
st.caption(f"{len(df_f)} ECBU affichés sur {len(df)} au total")

# Barre KPIs
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total",               total)
c2.metric("Positifs",            nb_positif,
          delta=f"{nb_positif/total*100:.0f}%" if total else None)
c3.metric("Négatifs",            nb_negatif)
c4.metric("Rejets",              nb_rejet,   help="Contamination / flore polymorphe")
c5.metric("Alertes",             nb_alerte,  help="Infections décapitées, cas critiques")
c6.metric(
    "Taux non-pertinence",
    f"{taux_np:.1f}%",
    delta=f"{nb_np} ECBU",
    delta_color="inverse",
    help=f"IC 95% Wilson : [{ci_low:.1f}% – {ci_high:.1f}%]",
)

st.divider()

# =============================================================================
# ONGLETS
# =============================================================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Vue d'ensemble",
    "📈 Tendances",
    "🏥 Par service",
    "⚠️ Sécurité & Alertes",
    "📋 Données brutes",
    "🤖 Prédiction NP",
])

# ---------------------------------------------------------------------------
# TAB 1 — VUE D'ENSEMBLE
# ---------------------------------------------------------------------------
with tab1:

    col_a, col_b = st.columns(2)

    # Donut décisions
    with col_a:
        st.subheader("Répartition des décisions")
        dec_counts = df_f[col_dec].value_counts().reset_index()
        dec_counts.columns = ["Décision", "Nombre"]
        couleurs = [couleur_decision(d) for d in dec_counts["Décision"]]
        fig_donut = go.Figure(go.Pie(
            labels=dec_counts["Décision"],
            values=dec_counts["Nombre"],
            hole=0.45,
            marker_colors=couleurs,
            textinfo="label+percent",
            hovertemplate="%{label}<br>%{value} ECBU (%{percent})<extra></extra>",
        ))
        fig_donut.update_layout(
            showlegend=False, margin=dict(t=10, b=10, l=10, r=10), height=300
        )
        st.plotly_chart(fig_donut, use_container_width=True)

    # Barres germes
    with col_b:
        st.subheader("Germes les plus fréquents")
        germe_counts = df_f["Bactérie"].value_counts().head(8).reset_index()
        germe_counts.columns = ["Germe", "Nombre"]
        fig_germe = px.bar(
            germe_counts, x="Nombre", y="Germe", orientation="h",
            color="Nombre", color_continuous_scale="Blues", text="Nombre",
        )
        fig_germe.update_traces(textposition="outside")
        fig_germe.update_layout(
            yaxis=dict(autorange="reversed"),
            coloraxis_showscale=False,
            margin=dict(t=10, b=10, l=10, r=40), height=300,
        )
        st.plotly_chart(fig_germe, use_container_width=True)

    col_c, col_d = st.columns(2)

    # Donut symptomatique
    with col_c:
        st.subheader("Symptomatique vs Asymptomatique")
        sympt_counts = df_f["Symptomatique"].value_counts().reset_index()
        sympt_counts.columns = ["Statut", "Nombre"]
        fig_sympt = go.Figure(go.Pie(
            labels=sympt_counts["Statut"],
            values=sympt_counts["Nombre"],
            hole=0.45,
            marker_colors=["#3498db", "#bdc3c7"],
            textinfo="label+percent",
            hovertemplate="%{label}<br>%{value} ECBU (%{percent})<extra></extra>",
        ))
        fig_sympt.update_layout(
            showlegend=False, margin=dict(t=10, b=10, l=10, r=10), height=280
        )
        st.plotly_chart(fig_sympt, use_container_width=True)

    # Barres mode prélèvement
    with col_d:
        st.subheader("Mode de prélèvement")
        mode_counts = df_f["Mode Prélèvement"].value_counts().reset_index()
        mode_counts.columns = ["Mode", "Nombre"]
        fig_mode = px.bar(
            mode_counts, x="Nombre", y="Mode", orientation="h",
            color="Nombre", color_continuous_scale="Oranges", text="Nombre",
        )
        fig_mode.update_traces(textposition="outside")
        fig_mode.update_layout(
            yaxis=dict(autorange="reversed"),
            coloraxis_showscale=False,
            margin=dict(t=10, b=10, l=10, r=40), height=280,
        )
        st.plotly_chart(fig_mode, use_container_width=True)

    # Donut profils de résistance (si colonne présente)
    if "Profil Résistance" in df_f.columns:
        st.subheader("Profils de résistance bactérienne")
        res_counts = df_f["Profil Résistance"].value_counts().reset_index()
        res_counts.columns = ["Profil", "Nombre"]
        res_colors = [COULEURS_RESISTANCE.get(p, "#bdc3c7") for p in res_counts["Profil"]]
        fig_res = go.Figure(go.Pie(
            labels=res_counts["Profil"],
            values=res_counts["Nombre"],
            hole=0.45,
            marker_colors=res_colors,
            textinfo="label+percent",
            hovertemplate="%{label}<br>%{value} ECBU (%{percent})<extra></extra>",
        ))
        fig_res.update_layout(
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.3),
            margin=dict(t=10, b=40, l=10, r=10),
            height=320,
        )
        st.plotly_chart(fig_res, use_container_width=True)

# ---------------------------------------------------------------------------
# TAB 2 — TENDANCES TEMPORELLES
# ---------------------------------------------------------------------------
with tab2:
    st.subheader("Évolution temporelle")

    if "Date Prélèvement" in df_f.columns and df_f["Date Prélèvement"].notna().any():
        df_t = df_f.copy()
        df_t["Date Prélèvement"] = pd.to_datetime(df_t["Date Prélèvement"], errors="coerce")
        df_t["Jour"] = df_t["Date Prélèvement"].dt.date

        par_jour = df_t.groupby("Jour").agg(
            Total=("Jour", "size"),
            Non_Pertinents=(col_dec, lambda x: x.str.contains(
                "NÉGATIF|REJET", case=False, na=False).sum()),
            Positifs=(col_dec, lambda x: x.str.contains(
                "POSITIF", case=False, na=False).sum()),
        ).reset_index()
        par_jour["Taux NP (%)"] = (par_jour["Non_Pertinents"] / par_jour["Total"] * 100).round(1)

        fig_vol = go.Figure()
        fig_vol.add_trace(go.Scatter(
            x=par_jour["Jour"], y=par_jour["Total"],
            name="Total", line=dict(color="#2c3e50", width=2),
            hovertemplate="%{x}<br>Total : %{y}<extra></extra>",
        ))
        fig_vol.add_trace(go.Scatter(
            x=par_jour["Jour"], y=par_jour["Positifs"],
            name="Positifs", line=dict(color="#2ecc71", width=2),
            hovertemplate="%{x}<br>Positifs : %{y}<extra></extra>",
        ))
        fig_vol.add_trace(go.Scatter(
            x=par_jour["Jour"], y=par_jour["Non_Pertinents"],
            name="Non pertinents", line=dict(color="#e67e22", width=2, dash="dot"),
            hovertemplate="%{x}<br>Non pertinents : %{y}<extra></extra>",
        ))
        fig_vol.update_layout(
            xaxis_title="Date", yaxis_title="Nombre d'ECBU",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            hovermode="x unified", height=350, margin=dict(t=30, b=10),
        )
        st.plotly_chart(fig_vol, use_container_width=True)

        fig_np = px.area(
            par_jour, x="Jour", y="Taux NP (%)",
            color_discrete_sequence=["#e74c3c"],
        )
        fig_np.add_hline(
            y=taux_np, line_dash="dash", line_color="#7f8c8d",
            annotation_text=f"Moyenne {taux_np:.1f}%",
            annotation_position="bottom right",
        )
        # IC Wilson en bande de fond
        fig_np.add_hrect(
            y0=ci_low, y1=ci_high, fillcolor="#e74c3c", opacity=0.08,
            line_width=0, annotation_text=f"IC 95%",
            annotation_position="top left",
        )
        fig_np.update_layout(
            height=280, margin=dict(t=10, b=10),
            yaxis_title="Taux NP (%)", xaxis_title="Date",
        )
        st.plotly_chart(fig_np, use_container_width=True)

    else:
        st.info("Pas de dates de prélèvement disponibles pour les tendances.")

# ---------------------------------------------------------------------------
# TAB 3 — PAR SERVICE
# ---------------------------------------------------------------------------
with tab3:
    st.subheader("Indicateurs par service")

    col_left, col_right = st.columns([1, 1])

    with col_left:
        def coloriser_np(val):
            if isinstance(val, (int, float)):
                if val >= 50:
                    return "background-color:#fde8e8; color:#c0392b; font-weight:bold"
                if val >= 30:
                    return "background-color:#fef3cd; color:#856404"
                return "background-color:#d4edda; color:#155724"
            return ""

        styled = (
            df_svc.style
            .applymap(coloriser_np, subset=["Taux NP (%)"])
            .format({"Taux NP (%)": "{:.1f}%", "% Asymptomatiques": "{:.1f}%"})
            .bar(subset=["Total ECBU"], color="#d6eaf8")
        )
        st.dataframe(styled, use_container_width=True, hide_index=True, height=380)

    with col_right:
        st.subheader("Heatmap service × décision")
        pivot = (
            df_f.groupby(["Service", col_dec])
            .size()
            .reset_index(name="n")
            .pivot(index="Service", columns=col_dec, values="n")
            .fillna(0)
            .astype(int)
        )
        fig_heat = px.imshow(
            pivot,
            color_continuous_scale="RdYlGn_r",
            aspect="auto",
            text_auto=True,
            labels=dict(x="Décision", y="Service", color="ECBU"),
        )
        fig_heat.update_layout(
            height=380, margin=dict(t=10, b=10, l=10, r=10),
            coloraxis_showscale=False, xaxis_tickangle=-30,
        )
        st.plotly_chart(fig_heat, use_container_width=True)

    fig_svc = px.bar(
        df_svc.sort_values("Taux NP (%)"),
        x="Service", y=["Taux NP (%)", "% Asymptomatiques"],
        barmode="group",
        color_discrete_map={"Taux NP (%)": "#e74c3c", "% Asymptomatiques": "#3498db"},
        labels={"value": "%", "variable": "Indicateur"},
    )
    fig_svc.update_layout(
        height=320, margin=dict(t=10, b=60),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig_svc, use_container_width=True)

# ---------------------------------------------------------------------------
# TAB 4 — SÉCURITÉ & ALERTES
# ---------------------------------------------------------------------------
with tab4:
    st.subheader("Indicateurs de sécurité")

    col_s1, col_s2, col_s3, col_s4 = st.columns(4)

    nb_asb_v       = compter_asb(df_f)
    nb_decap_v     = compter_infections_decapitees(df_f)
    nb_prelev_v    = compter_prelev_risque(df_f)
    nb_immuno      = int(df_f[col_dec].str.contains("Immunodéprimé", case=False, na=False).sum())

    col_s1.metric("ASB identifiées",       nb_asb_v,
                  help="Bactériuries asymptomatiques chez >75 ans")
    col_s2.metric("Infections décapitées", nb_decap_v,
                  help="ATB en cours + leucocyturie + culture négative")
    col_s3.metric("Immunodép. infectés",   nb_immuno)
    col_s4.metric("Prélèvements à risque", nb_prelev_v)

    st.divider()

    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=taux_np,
        delta={"reference": 40, "increasing": {"color": "#e74c3c"},
               "decreasing": {"color": "#2ecc71"}},
        title={"text": f"Taux de non-pertinence (%) — IC 95% : [{ci_low:.1f}–{ci_high:.1f}%]"},
        gauge={
            "axis": {"range": [0, 100]},
            "bar":  {"color": "#e74c3c" if taux_np >= 50 else
                              "#e67e22" if taux_np >= 30 else "#2ecc71"},
            "steps": [
                {"range": [0,  30], "color": "#d4edda"},
                {"range": [30, 50], "color": "#fef3cd"},
                {"range": [50, 100], "color": "#fde8e8"},
            ],
            "threshold": {"line": {"color": "#7f8c8d", "width": 3},
                          "thickness": 0.75, "value": 40},
        },
        number={"suffix": "%", "font": {"size": 36}},
    ))
    fig_gauge.update_layout(height=280, margin=dict(t=30, b=10, l=40, r=40))
    st.plotly_chart(fig_gauge, use_container_width=True)

    if "Recommandation" in df_f.columns:
        df_reco = df_f[df_f["Recommandation"].notna()][[
            "ID Anonyme", "Sexe", "Age", "Service", "Bactérie",
            "Symptomatique", col_dec, "Recommandation",
        ]]
        if not df_reco.empty:
            st.subheader("Détail des recommandations cliniques")
            st.dataframe(
                df_reco.style.applymap(
                    lambda v: f"color:{couleur_decision(v)}; font-weight:bold",
                    subset=[col_dec],
                ),
                use_container_width=True, hide_index=True,
            )
        else:
            st.success("Aucune alerte clinique sur la sélection courante.")

# ---------------------------------------------------------------------------
# TAB 5 — DONNÉES BRUTES
# ---------------------------------------------------------------------------
with tab5:
    st.subheader("Données complètes")
    styled_raw = df_f.style.applymap(
        lambda v: f"color:{couleur_decision(v)}; font-weight:bold",
        subset=[col_dec],
    )
    st.dataframe(styled_raw, use_container_width=True, hide_index=True)
    csv = df_f.to_csv(index=False).encode("utf-8")
    st.download_button("Télécharger en CSV", csv, "export_ecbu.csv", "text/csv")

# ---------------------------------------------------------------------------
# TAB 6 — PRÉDICTION NP (ML)
# ---------------------------------------------------------------------------
with tab6:
    st.subheader("🤖 Prédiction de non-pertinence — Régression logistique")
    st.caption(
        "⚠️ Modèle entraîné sur données **synthétiques** — "
        "à des fins de recherche et de démonstration uniquement. "
        "Ne pas utiliser pour des décisions cliniques réelles."
    )

    try:
        from modele_prediction import (
            charger_modele, evaluer_modele,
            preparer_features, coefficients_modele, predire_np,
        )
        pipeline = charger_modele()

        X_eval, y_eval, colonnes = preparer_features(df_f)
        if len(df_f) >= 20:
            metriques = evaluer_modele(pipeline, X_eval, y_eval)

            col_m1, col_m2 = st.columns(2)
            with col_m1:
                st.metric("AUC-ROC",  f"{metriques['auc']:.3f}",
                          help="1.0 = parfait, 0.5 = aléatoire")
                st.metric("Accuracy", f"{metriques['accuracy']:.3f}")

            with col_m2:
                cm = metriques["cm"]
                fig_cm = px.imshow(
                    cm, text_auto=True,
                    labels=dict(x="Prédit", y="Réel"),
                    x=["Pertinent", "Non pertinent"],
                    y=["Pertinent", "Non pertinent"],
                    color_continuous_scale="Blues",
                )
                fig_cm.update_layout(
                    height=250, margin=dict(t=10, b=10),
                    coloraxis_showscale=False,
                )
                st.plotly_chart(fig_cm, use_container_width=True)

            # Top features
            df_coef = coefficients_modele(pipeline, colonnes).head(10)
            fig_coef = px.bar(
                df_coef, x="Coefficient", y="Feature", orientation="h",
                color="Coefficient",
                color_continuous_scale="RdBu_r",
                text="Coefficient",
            )
            fig_coef.update_traces(texttemplate="%{text:.3f}", textposition="outside")
            fig_coef.update_layout(
                height=320,
                yaxis=dict(autorange="reversed"),
                margin=dict(t=10, b=10),
                coloraxis_showscale=False,
                xaxis_title="Coefficient (positif → NP)",
            )
            st.subheader("Top 10 features prédictives")
            st.plotly_chart(fig_coef, use_container_width=True)
        else:
            st.info("Sélection trop petite pour évaluer le modèle (minimum 20 ECBU).")

        # Formulaire prédiction individuelle
        st.divider()
        st.subheader("Prédiction individuelle")
        with st.form("form_prediction"):
            pc1, pc2, pc3 = st.columns(3)
            with pc1:
                age_pred       = st.number_input("Âge", 0, 120, 65)
                leuco_pred     = st.number_input("Leucocyturie (/mL)", 0, 1_000_000,
                                                  0, step=1_000)
                bact_pred      = st.number_input("Bactériurie UFC/mL", 0, 10_000_000,
                                                  0, step=1_000)
            with pc2:
                sexe_pred    = st.selectbox("Sexe", ["Femme", "Homme"])
                service_pred = st.selectbox("Service", sorted(df_f["Service"].unique()))
                mode_pred    = st.selectbox(
                    "Mode prélèvement", sorted(df_f["Mode Prélèvement"].unique())
                )
            with pc3:
                sympt_pred  = st.checkbox("Symptomatique")
                sonde_pred  = st.checkbox("Sondé")
                immuno_pred = st.checkbox("Immunodéprimé")
                enceinte_pred = st.checkbox("Enceinte")
                atb_pred    = st.checkbox("ATB en cours")
            submitted = st.form_submit_button("Prédire la pertinence")

        if submitted:
            patient = {
                "Age":                 age_pred,
                "Leucocyturie":        leuco_pred,
                "Bactériurie UFC/mL":  bact_pred,
                "Sexe":                sexe_pred,
                "Service":             service_pred,
                "Mode Prélèvement":    mode_pred,
                "Symptomatique":       "Oui" if sympt_pred  else "Non",
                "Sondé":               "Oui" if sonde_pred  else "Non",
                "Immunodéprimé":       "Oui" if immuno_pred else "Non",
                "Enceinte":            "Oui" if enceinte_pred else "Non",
                "ATB en cours":        "Oui" if atb_pred    else "Non",
            }
            result = predire_np(pipeline, patient)
            if result["prediction"] == 1:
                st.error(
                    f"⚠️ Prescription probablement **non pertinente** "
                    f"(probabilité : {result['probabilite_np']:.1%})"
                )
            else:
                st.success(
                    f"✅ Prescription probablement **pertinente** "
                    f"(probabilité NP : {result['probabilite_np']:.1%})"
                )

    except FileNotFoundError:
        st.info(
            "Modèle non entraîné. Lancez :\n\n"
            "```bash\npython modele_prediction.py --evaluate\n```"
        )
    except Exception as _e:
        st.warning(f"Erreur chargement modèle : {_e}")

# =============================================================================
# FOOTER
# =============================================================================
st.divider()
st.caption(
    "SAD ECBU — Hôpital Avicenne / LIMICS | "
    "Algorithme REMIC 2022 / SPILF 2015 / HAS 2023"
)
