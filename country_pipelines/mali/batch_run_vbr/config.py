"""Configuration constants for the batch_run_vbr orchestrator pipeline."""

SERVER_URL = "https://app.openhexa.org"

# Launched runs never send email notifications.
SEND_NOTIFICATION = False

# Default values for the pipeline parameters.
DEFAULT_BATCH_SIZE = 5
DEFAULT_OPENHEXA_CONN = "openhexa-client"
DEFAULT_POLL_INTERVAL = 15

# Terminal run states, as defined by the OpenHEXA PipelineRunStatus enum.
TERMINAL_STATUSES = {"success", "failed", "stopped", "skipped"}

# Parameter code -> caster used to coerce CSV string cells to the type run_vbr expects.
# Mirrors the @parameter(... type=...) declarations in run_vbr/pipeline.py. Any column not
# listed here is passed through as a plain string.
PARAM_TYPES = {
    "model": str,
    "folder": str,
    "mois_start": int,
    "year_start": int,
    "mois_fin": int,
    "year_fin": int,
    "frequence": str,
    "window": int,
    "nb_period_verif": int,
    "prix_verif": int,
    "paym_method_nf": str,
    "proportion_selection_bas_risque": float,
    "proportion_selection_moyen_risque": float,
    "proportion_selection_haut_risque": float,
    "quantity_risk_calculation": str,
    "seuil_max_bas_risk": float,
    "seuil_max_moyen_risk": float,
    "verification_gain_low": int,
    "verification_gain_mod": int,
}
