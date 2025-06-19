import pandas as pd
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
    elif center.category_centre == "pca":
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
    quality_scores = {"high": [], "mod": [], "low": []}
    list_indicators = list(center.qualite_window["indicator"].unique())

    for indicator in list_indicators:
        evolution = center.qualite_window[center.qualite_window.indicator == indicator]["score"]

        if len(evolution) >= 2:
            trend = list(evolution.pct_change().values)[-1]
            score = list(evolution.values)[-1]
            risk = set_quality_for_indicator(score, trend)
            quality_scores.setdefault(risk, []).append(indicator)
        elif len(evolution) == 1:
            score = list(evolution.values)[-1]
            quality_scores.setdefault("high", []).append(indicator)
        else:
            score = pd.NA
            quality_scores.setdefault("high", []).append(indicator)

        center.indicator_scores[indicator] = score

    set_main_quality_indicators(center)

    center.quality_high_risk = "--".join(quality_scores["high"])
    center.quality_mod_risk = "--".join(quality_scores["mod"])
    center.quality_low_risk = "--".join(quality_scores["low"])

    _categorize_quality_risk(center)


def set_main_quality_indicators(center):
    """ "
    Set the main quality indicators for the center.

    Parameters
    ----------
    center : OrgUnit object.
        Object containing the information from the particular Organizational Unit
    """
    if "Organisation générale" in center.indicator_scores:
        center.general_quality = center.indicator_scores["Organisation générale"]
    else:
        center.general_quality = 0

    if "Hygiène et Stérilisation" in center.indicator_scores:
        center.hygiene = center.indicator_scores["Hygiène et Stérilisation"]
    else:
        center.hygiene = 0

    if "Gestion financière" in center.indicator_scores:
        center.finance = center.indicator_scores["Gestion financière"]
    else:
        center.finance = 0


def set_quality_for_indicator(score, trend, seuil_high=0.5, seuil_mod=0.75):
    """
    Based on a score and in a trend, determine if the quality risk is high, moderate or low

    Parameters
    -----------
    score: float
        The score of the particular quality indicator
    trend: float
        How the score has been evolving.
    seuil_high: int, default 50
        The threshold for high risk
    seuil_mod: int, default 76
        The threshold for moderate risk

    Returns
    ---------
    str:
        It has the quality risk for the center.
    """
    if score < seuil_high:
        return "high"
    elif score < seuil_mod:
        if trend > 0:
            return "moderate"
        else:
            return "high"
    else:
        if trend > -0.1:
            return "low"
        else:
            return "moderate"


def _categorize_quality_risk(center):
    """
    Using the main quality indicators for the center, set the quality risk.

    Parameters
    ----------
    center : OrgUnit object.
        Object containing the information from the particular Organizational Unit
    """
    if center.general_quality < 50 or (
        center.general_quality < 80 and (center.hygiene < 75 or center.finance < 75)
    ):
        center.risk_quality = "high"
    elif (center.general_quality >= 80 and (center.hygiene < 75 or center.finance < 75)) or (
        center.general_quality < 80 and center.hygiene >= 75 and center.finance >= 75
    ):
        center.risk_quality = "moderate"
    else:
        center.risk_quality = "low"


def categorize_quantity(
    center,
    seuil_gain_median,
    seuil_max_bas_risk,
    seuil_max_moyen_risk,
    quantity_risk_calculation,
    verification_gain_low,
    verification_gain_mod,
):
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
    if (
        quantity_risk_calculation == "standard"
        or quantity_risk_calculation == "ecartmedgen"
        or quantity_risk_calculation == "ecartavggen"
    ):
        if center.diff_subsidies_decval_median > seuil_gain_median:
            center.risk_gain_median = "high"
        else:
            center.risk_gain_median = "low"
            if quantity_risk_calculation == "standard":
                score = center.ecart_median
            elif quantity_risk_calculation == "ecartmedgen":
                score = center.ecart_median_gen
            elif quantity_risk_calculation == "ecartavggen":
                score = center.ecart_avg_gen

            if score > seuil_max_moyen_risk:
                center.risk_quantite = "high"
            elif score > seuil_max_bas_risk:
                center.risk_quantite = "moderate"
            else:
                center.risk_quantite = "low"
    else:
        dict_threholds = get_thresholds(-verification_gain_low, -verification_gain_mod)

        if pd.isna(center.benefice_vbr):
            center.risk = "high"
        elif center.benefice_vbr < dict_threholds["low"]:
            center.risk_quantite = "low"
        elif center.benefice_vbr <= dict_threholds["moderate"]:
            center.risk_quantite = "moderate"
        else:
            center.risk_quantite = "high"


def get_thresholds(verification_gain_low, verification_gain_mod):
    """
    Define a dictionary with the thresholds for the quantity risk categories. (low, moderate, moderate, moderate, high)

    Parameters
    ----------
    verification_gain_low: float
        Threshold for the low risk category.
    verification_gain_mod: float
        Threshold for the moderate risk categories.

    Returns
    -------
    dict
        Dictionary with the thresholds for the quantity risk categories.
    """
    dict_threholds = {
        "low": verification_gain_low,
        "moderate": verification_gain_low + (verification_gain_mod - verification_gain_low) / 2,
        "high": verification_gain_mod,
    }
    return dict_threholds


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
