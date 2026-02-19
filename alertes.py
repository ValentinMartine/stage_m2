"""
Module alertes.py — Surveillance du taux de non-pertinence par service
et envoi d'alertes email via SMTP SSL.

Variables d'environnement (.env) :
    ALERT_SMTP_HOST   : serveur SMTP (ex. smtp.gmail.com)
    ALERT_SMTP_PORT   : port SSL (ex. 465)
    ALERT_EMAIL_FROM  : adresse expéditeur
    ALERT_EMAIL_TO    : destinataire(s), séparés par virgule
    ALERT_SMTP_USER   : identifiant SMTP
    ALERT_SMTP_PASS   : mot de passe d'application SMTP
    ALERT_NP_SEUIL    : seuil en % déclenchant l'alerte (défaut 40)
"""

import os
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd

from kpis_ecbu import stats_par_service

DEFAULT_SEUIL = 40.0


def verifier_seuil_np(df: pd.DataFrame, seuil: float = DEFAULT_SEUIL) -> list[dict]:
    """
    Identifie les services dont le taux de non-pertinence dépasse `seuil` (%).

    Args:
        df    : DataFrame issu de v_algo_avicenne.
        seuil : Seuil en pourcentage (défaut 40 %).

    Returns:
        Liste de dicts {service, total, nb_np, taux_np} pour chaque dépassement.
    """
    df_svc = stats_par_service(df)
    return [
        {
            "service": row["Service"],
            "total":   row["Total ECBU"],
            "nb_np":   row["Non pertinents"],
            "taux_np": row["Taux NP (%)"],
        }
        for _, row in df_svc.iterrows()
        if row["Taux NP (%)"] >= seuil
    ]


def _corps_html(alertes: list[dict], seuil: float) -> str:
    """Construit le corps HTML de l'email d'alerte."""
    lignes = "".join(
        f"<tr>"
        f"<td style='padding:4px 10px'>{a['service']}</td>"
        f"<td style='padding:4px 10px;text-align:center'>{a['total']}</td>"
        f"<td style='padding:4px 10px;text-align:center'>{a['nb_np']}</td>"
        f"<td style='padding:4px 10px;text-align:center;"
        f"font-weight:bold;color:#c0392b'>{a['taux_np']:.1f}%</td>"
        f"</tr>"
        for a in alertes
    )
    date_str = datetime.now().strftime("%d/%m/%Y à %H:%M")
    return f"""
<html><body style="font-family:Arial,sans-serif;font-size:14px">
<h2 style="color:#e74c3c">&#9888; Alerte ECBU &#8212; Taux de non-pertinence élevé</h2>
<p>Le {date_str}, <strong>{len(alertes)}</strong> service(s) dépassent
le seuil de <strong>{seuil:.0f}%</strong> :</p>
<table border="1" cellpadding="0" cellspacing="0"
       style="border-collapse:collapse;font-size:13px">
  <thead style="background:#2c3e50;color:white">
    <tr>
      <th style="padding:6px 12px">Service</th>
      <th style="padding:6px 12px">Total ECBU</th>
      <th style="padding:6px 12px">Non pertinents</th>
      <th style="padding:6px 12px">Taux NP</th>
    </tr>
  </thead>
  <tbody>{lignes}</tbody>
</table>
<p style="color:#95a5a6;font-size:11px;margin-top:20px">
SAD ECBU &#8212; Hôpital Avicenne / LIMICS</p>
</body></html>"""


def envoyer_alerte_email(alertes: list[dict], config_smtp: dict | None = None) -> bool:
    """
    Envoie un email récapitulatif pour les services en dépassement de seuil NP.

    Args:
        alertes      : liste retournée par verifier_seuil_np().
        config_smtp  : dict optionnel {smtp_host, smtp_port, user, password,
                       email_from, email_to, seuil}. Si None, lu depuis os.getenv().

    Returns:
        True si envoi réussi ou si aucune alerte, False en cas d'erreur.
    """
    if not alertes:
        return True

    if config_smtp is None:
        config_smtp = {
            "smtp_host":  os.getenv("ALERT_SMTP_HOST", ""),
            "smtp_port":  int(os.getenv("ALERT_SMTP_PORT", "465")),
            "user":       os.getenv("ALERT_SMTP_USER", ""),
            "password":   os.getenv("ALERT_SMTP_PASS", ""),
            "email_from": os.getenv("ALERT_EMAIL_FROM", ""),
            "email_to":   os.getenv("ALERT_EMAIL_TO", ""),
            "seuil":      float(os.getenv("ALERT_NP_SEUIL", str(DEFAULT_SEUIL))),
        }

    if not config_smtp.get("smtp_host") or not config_smtp.get("email_to"):
        print("ALERT_SMTP_HOST ou ALERT_EMAIL_TO non configuré — envoi ignoré.")
        return False

    destinataires = [e.strip() for e in config_smtp["email_to"].split(",")]
    seuil         = config_smtp.get("seuil", DEFAULT_SEUIL)

    msg            = MIMEMultipart("alternative")
    msg["Subject"] = (
        f"[ECBU] \u26a0 Alerte taux NP — {len(alertes)} service(s) en dépassement"
    )
    msg["From"] = config_smtp["email_from"]
    msg["To"]   = ", ".join(destinataires)
    msg.attach(MIMEText(_corps_html(alertes, seuil), "html", "utf-8"))

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(
            config_smtp["smtp_host"], config_smtp["smtp_port"], context=ctx
        ) as srv:
            srv.login(config_smtp["user"], config_smtp["password"])
            srv.sendmail(config_smtp["email_from"], destinataires, msg.as_string())
        return True
    except smtplib.SMTPException as exc:
        print(f"Erreur SMTP : {exc}")
        return False
