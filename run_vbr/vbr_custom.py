import pandas as pd
from openhexa.sdk import current_run, workspace
from RBV_package import rbv_environment


def get_proportions(p_low, p_mod, p_high):
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
    center.nb_periods_verified = center.quantite_window[
        ~pd.isnull(center.quantite_window.val)
    ].month.unique()
    center.nb_periods_not_verified = center.quantite_window[
        pd.isnull(center.quantite_window.val)
    ].month.unique()
    return len(center.nb_periods_verified) < center.nb_periods


def visited_since(center, months_since_last_visit):
    center.verified_last_period = center.quantite_window[~pd.isnull(center.quantite_window.val)][
        "month"
    ].max() >= str(rbv_environment.months_before(center.month, months_since_last_visit))
    return center.verified_last_period


def eligible_for_VBR(center, months_since_last_visit=3, min_subside=50):
    print(center.id, center.name, center.identifier_verification)
    if not_enough_visits_in_interval(center):
        print("Not enough visits in interval")
        return False
    elif center.risk == "uneligible":
        print("Uneligible since start")
        return False
    elif not visited_since(center, months_since_last_visit):
        print("Not visited since")
        return False
    elif center.quantite_window[~pd.isnull(center.quantite_window.val)].shape[0] == 0:
        print("No valid quantities")
        return False
    elif center.quantite_window.subside_sans_verification.sum() <= min_subside:
        print("Subside sans verification too low")
        return False
    else:
        print("Eligible")
        return True


def categorize_quality(center, consider_quality):
    if consider_quality:

        def quality_risk_rules(score, trend):
            pass

        current_run.log_info("Categorizing quality risk is not defined yet!")
    else:
        center.indicator_scores = {}
        center.quality_high_risk = ""
        center.quality_mod_risk = ""
        center.quality_low_risk = ""
        center.risk_quality = "low"


def categorize_quantity(center, seuil_gain_median, seuil_max_bas_risk, seuil_max_moyen_risk):
    if eligible_for_VBR(center):
        center.get_ecart_median()
        center.get_ecart_median_per_service()
        if center.gain_median > seuil_gain_median:
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
        center.nb_services_plus_grand_5_pct = nb_risk_moindre
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
        center.gain_median = pd.NA
        center.risk = "uneligible"
        center.risk_gain_median = "uneligible"
