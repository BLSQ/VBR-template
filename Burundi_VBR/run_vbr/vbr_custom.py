import pandas as pd
from openhexa.sdk import current_run
from RBV_package import dates


def get_proportions(p_low, p_mod, p_high):
    """
    From the probabilities for the low, moderate and high risk centers,
    we calculate the probabilities for other risk categories.
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
    dict
        Dictionary with the verification probabilities for each risk category.
    """
    return {
        "low": p_low,
        "moderate_1": (p_high - p_low) / 4 + p_low,
        "moderate_2": 2 * (p_high - p_low) / 4 + p_low,
        "moderate_3": 3 * (p_high - p_low) / 4 + p_low,
        "moderate": p_mod,
        "high": p_high,
        "uneligible": 1,
    }


def not_enough_visits_in_interval(center):
    """
    Check if there were enough visits in the interval.

    Parameters
    ----------
    center: Orgunit object.
        Object containing the information from the particular Organizational Unit

    Returns
    -------
    bool
        True if there are not enough visits in the interval, False otherwise.
    """
    center.nb_periods_verified = center.quantite_window[
        ~pd.isnull(center.quantite_window.val)
    ].month.unique()
    return len(center.nb_periods_verified) < center.nb_periods


def visited_since(center, months_since_last_visit):
    """
    Check that the center has been visited in the pertinent period.

    Parameters
    ----------
    center: Orgunit object.
        Object containing the information from the particular Organizational Unit
    months_since_last_visit: int
        Maximum number of months since the last visit.

    Returns
    -------
    bool
        True if the center has been visited in the pertinent period, False otherwise.
    """
    verified_last_period = center.quantite_window[~pd.isnull(center.quantite_window.val)][
        "month"
    ].max() >= str(dates.months_before(center.month, months_since_last_visit))
    return verified_last_period


def eligible_for_vbr(center, months_since_last_visit=3, min_subside=50):
    """
    Determine if the center is eligible for VBR.
    We check the following conditions:
    1. If there are enough visits in the interval.
    2. Check if the center has risk = "uneligible". (This was filled in the initialization pipeline)
    3. Check if the center has been visited recently enough.
    4. Check that (for the pertinent period) there were valid visits.
    5. Check that the money that the center would get without verification is above a certain threshold.

    Parameters
    ----------
    center: Orgunit object.
        Object containing the information from the particular Organizational Unit
    months_since_last_visit: int
        Maximum number of months since the last visit.
    min_subside: int
        Minimum amount of money that the center should get without verification.

    Returns
    -------
    bool
        True if the center is eligible for VBR, False otherwise.
    """
    if not_enough_visits_in_interval(center):
        return False
    elif center.risk == "uneligible":
        return False
    elif not visited_since(center, months_since_last_visit):
        return False
    elif center.quantite_window[~pd.isnull(center.quantite_window.val)].shape[0] == 0:
        return False
    elif center.quantite_window.subside_sans_verification.sum() <= min_subside:
        return False
    else:
        return True


def categorize_quality(center):
    """
    Use the quality informations to determine the risk of the center.

    Parameters
    ----------
    center : OrgUnit object.
        Object containing the information from the particular Organizational Unit
    """
    current_run.log_info("Categorizing quality risk is not defined yet!")


def categorize_quantity(center, seuil_gain_median, seuil_max_bas_risk, seuil_max_moyen_risk):
    """
    Categorize the quantity risk of the center.

    Parameters
    ----------
    center: Orgunit object.
        Object containing the information from the particular Organizational Unit
    seuil_gain_median: int
        Median verification gain from which the center is considered at high risk (euros). It is inputed by the user.
    seuil_max_bas_risk: float
        Maximum value of the weighted_ecart_dec_val for which the center is considered at low risk.
    seuil_max_moyen_risk: float
        Maximum value of the weighted_ecart_dec_val for which the center is considered at medium risk.
    """
    if eligible_for_vbr(center):
        center.get_ecart_median()
        center.get_diff_subsidies_decval_median()

        if center.diff_subsidies_decval_median > seuil_gain_median:
            center.risk_gain_median = "high"
        else:
            center.risk_gain_median = "low"

        nb_risk_moindre = len(
            [
                ecart
                for ecart in center.ecart_median_per_service.ecart_median.values
                if ecart >= seuil_max_bas_risk
            ]
        )
        center.nb_services_risky = nb_risk_moindre

        if center.ecart_median_per_service.ecart_median.max() <= seuil_max_bas_risk:
            center.risk_quantite = "low"
        elif center.ecart_median_per_service.ecart_median.max() >= seuil_max_moyen_risk:
            center.risk_quantite = "high"
        elif nb_risk_moindre == 1:
            center.risk_quantite = "moderate_1"
        elif nb_risk_moindre < 4:
            center.risk_quantite = "moderate_2"
        else:
            center.risk_quantite = "moderate_3"
    else:
        center.risk_quantite = "uneligible"
        center.ecart_median = pd.NA
        center.ecart_median_per_service = pd.NA
        center.diff_subsidies_decval_median = pd.NA
        center.risk = "uneligible"
        center.risk_gain_median = "uneligible"


def assign_taux_validation_per_zs(group):
    """
    Calculate the verification gains for each Organizational Unit (OU) in the group.
    In order to calculate them, we will use a taux. This taux will be the median taux per zone of sante and risk.
    (You have an Organizational Unit. You will group it with the OUs in the same Zone de Sante and with the same risk
    The taux for the non-verified center is the median of that group).

    Parameters
    -----------
    group :  GroupOrgUnits.
        List of OrgUnits. Contains the information about the verifications for a particular area.
    """

    def mediane(g: list) -> float:
        """
        Calculate the median of a list of numbers.

        Parameters
        ----------
        g : list
            List of numbers.

        Returns
        -------
        float
            Median of the list.
        """
        lenght_g = len(g)
        if lenght_g > 1:
            pair = lenght_g % 2 == 0
            sorted_numbers = sorted(g)
            if pair:
                mediane = sum(sorted_numbers[lenght_g // 2 : (lenght_g // 2) + 2]) / 2
            else:
                mediane = sorted_numbers[lenght_g // 2]
        elif lenght_g == 1:
            mediane = g[0]
        else:
            mediane = pd.NA
        return mediane

    taux_val_zs = {}
    for ou in group.members:
        taux_val_zs.setdefault(f"{ou.identifier_verification[2]}-{ou.risk}", []).append(
            ou.taux_validation
        )

    for zs, _ in taux_val_zs.items():
        taux_val_zs[zs] = mediane(taux_val_zs[zs])

    for ou in group.members:
        if f"{ou.identifier_verification[2]}-{ou.risk}" in taux_val_zs:
            ou.get_gain_verif_for_period_verif(
                taux_val_zs[f"{ou.identifier_verification[2]}-{ou.risk}"]
            )
        else:
            ou.get_gain_verif_for_period_verif(ou.taux_validation)
