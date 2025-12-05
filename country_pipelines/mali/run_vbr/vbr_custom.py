import pandas as pd
from openhexa.sdk import current_run

from orgunit import Orgunit
from orgunit import VBR


def get_proportions(p_low: float, p_mod: float, p_high: float) -> dict[str, float]:
    """
    From the probabilities for the low, moderate and high risk centers,
    we contruct a dictionary with the verification probabilities for each risk category.
    (Note that these can be changed depending on the bussiness requirements)

    Parameters
    ----------
    p_low: float
        Probability for a low risk center to be verified
    p_mod: float
        Probability for a moderate risk center to be verified
    p_high: float
        Probability for a high risk center to be verified

    Returns
    -------
    dict[str, float]
        Dictionary with the verification probabilities for each risk category.
    """
    return {
        "low": p_low,
        "moderate": p_mod,
        "high": p_high,
        "uneligible": 1,
    }


def not_enough_visits_in_interval(center: Orgunit, vbr_object: VBR) -> bool:
    """
    Check if there were enough visits in the interval.

    Parameters
    ----------
    center: Orgunit
        Object containing the information from the particular Organizational Unit
    vbr_object: VBR
        Object containing the VBR configuration.

    Returns
    -------
    bool
        True if there are not enough visits in the interval, False otherwise.
    """
    nb_periods_verified = center.quantite_window.loc[
        ~pd.isnull(center.quantite_window.val), vbr_object.period_type
    ].unique()

    if vbr_object.period_type == "month":
        number_verifications = len(nb_periods_verified)
    elif vbr_object.period_type == "quarter":
        number_verifications = len(nb_periods_verified) * 3

    return number_verifications < vbr_object.nb_periods


def eligible_for_vbr(center: Orgunit, vbr_object: VBR) -> bool:
    """
    Determine if the center is eligible for VBR. We check the following conditions:
    1. If there are enough visits in the interval.
    2. Check if the center has risk = "uneligible". (This was filled in the initialization pipeline)

    Parameters
    ----------
    center: Orgunit
        Object containing the information from the particular Organizational Unit
    vbr_object: VBR
        Object containing the VBR configuration.

    Returns
    -------
    bool
        True if the center is eligible for VBR, False otherwise.
    """
    if not_enough_visits_in_interval(center, vbr_object):
        return False
    elif center.quantite_window[~pd.isnull(center.quantite_window.val)].shape[0] == 0:
        raise ValueError("You should not be here")
    else:
        return True


def categorize_quantity(center: Orgunit, vbr_object: VBR) -> None:
    """
    Categorize the quantity risk of the center.

    Parameters
    ----------
    center: Orgunit
        Object containing the information from the particular Organizational Unit
    vbr_object: VBR
        Object containing the VBR configuration.
    """
    if vbr_object.quantity_risk_calculation == "ecart_median":
        categorize_quantity_ecart(center, vbr_object, "median")
    elif vbr_object.quantity_risk_calculation == "ecart_moyen":
        categorize_quantity_ecart(center, vbr_object, "mean")
    elif vbr_object.quantity_risk_calculation == "verifgain":
        categorize_quantity_verifgain(center, vbr_object)
    else:
        current_run.log_error(
            f"Quantity risk method {vbr_object.quantity_risk_calculation} not recognized."
        )
        raise ValueError(
            f"Quantity risk method {vbr_object.quantity_risk_calculation} not recognized."
        )


def categorize_quantity_ecart(center: Orgunit, vbr_object: VBR, method: str) -> None:
    """
    Categorize the quantity risk of the center using the difference between
    the declared and validated values (the "ecart" method).

    Parameters
    ----------
    center: Orgunit
        Object containing the information from the particular Organizational Unit
    vbr_object: VBR
        Object containing the VBR configuration.
    method: str
        Method to use for the calculation ("median" or "mean").
    """
    if method == "median":
        indicator = center.ecart_median_window
    elif method == "mean":
        indicator = center.ecart_moyen_window

    if indicator > vbr_object.seuil_max_moyen_risk:
        center.risk_quantite = "high"
    elif indicator > vbr_object.seuil_max_bas_risk:
        center.risk_quantite = "moderate"
    elif indicator >= 0:
        center.risk_quantite = "low"
    else:
        center.risk_quantite = "weird"


def categorize_quantity_verifgain(center: Orgunit, vbr_object: VBR) -> None:
    """
    Categorize the quantity risk of the center using the difference in money
    spent when visiting the center and not visiting it (the "verifgain" method).

    Parameters
    ----------
    center: Orgunit
        Object containing the information from the particular Organizational Unit
    vbr_object: VBR
        Object containing the VBR configuration.
    """
    if center.benefice_vbr_window > vbr_object.verification_gain_low:
        center.risk_quantite = "low"
    elif center.benefice_vbr_window > vbr_object.verification_gain_mod:
        center.risk_quantite = "moderate"
    else:
        center.risk_quantite = "high"
