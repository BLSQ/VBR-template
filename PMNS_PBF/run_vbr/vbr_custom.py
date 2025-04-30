import pandas as pd
from openhexa.sdk import current_run, workspace
from RBV_package import rbv_environment


def get_proportions(p_low, p_mod, p_high):
    return {
        "low": p_low,
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
    center.verified_last_period = center.quantite_window[
        ~pd.isnull(center.quantite_window.val)
    ]["month"].max() >= str(
        rbv_environment.months_before(center.month, months_since_last_visit)
    )
    return center.verified_last_period


def eligible_for_VBR(center, months_since_last_visit, min_subside):
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


def categorize_quantity(
    self, gain_median_seuil, seuil_max_bas_risk, seuil_max_moyen_risk
):
    if eligible_for_VBR(self, months_since_last_visit=3, min_subside=50):
        self.get_ecart_median()
        self.get_ecart_median_per_service()
        self.get_gain_median_par_periode()
        if self.gain_median > gain_median_seuil:
            self.risk_gain_median = "high"
            self.risk_quantite = "high"
        else:
            self.risk_gain_median = "low"
            score = self.ecart_median
            if score > seuil_max_moyen_risk:
                self.risk_quantite = "high"
            elif score > seuil_max_bas_risk:
                self.risk_quantite = "moderate"
            else:
                self.risk_quantite = "low"
    else:
        self.risk_quantite = "uneligible"
        self.ecart_median = pd.NA
        self.risk_gain_median = "high"
        self.gain_median = pd.NA
        self.risk = "uneligible"


def categorize_quality(self, consider_quality):
    if consider_quality:

        def quality_risk_rules(score, trend):
            bad = 50
            mod = 75
            if score < bad:
                return "high"
            elif score < mod:
                if trend > 0:
                    return "mod"
                else:
                    return "high"
            else:
                if trend > -0.1:
                    return "low"
                else:
                    return "mod"

        def pmns_categorize_risk(hygiene, general, finance):
            if general < 50 or (general < 80 and (hygiene < 75 or finance < 75)):
                self.risk_quality = "high"
            elif (general >= 80 and (hygiene < 75 or finance < 75)) or (
                general < 80 and hygiene >= 75 and finance >= 75
            ):
                self.risk_quality = "moderate"
            else:
                self.risk_quality = "low"

        indicators = list(self.qualite.indicator.unique())
        quality_scores = {"high": [], "mod": [], "low": []}
        self.indicator_scores = {}
        self.general_quality, self.hygiene, self.finance = 0, 0, 0
        for i in indicators:
            evolution = self.qualite_window[self.qualite_window.indicator == i]["score"]
            if len(evolution) >= 2:
                trend = list(evolution.pct_change().values)[-1]
                score = list(evolution.values)[-1]
                risk = quality_risk_rules(score, trend)
                quality_scores.setdefault(risk, []).append(i)
                print(i, trend, list(evolution.values)[-2:])
            elif len(evolution) == 1:
                score = list(evolution.values)[-1]
                quality_scores.setdefault("high", []).append(i)
            else:
                score = pd.NA
                quality_scores.setdefault("high", []).append(i)
            self.indicator_scores[i] = score
            if not pd.isnull(score):
                if i == "Organisation générale":
                    self.general_quality = score
                elif i == "Hygiène et Stérilisation":
                    self.hygiene = score
                elif i == "Gestion financière":
                    self.finance = score
        self.quality_high_risk = "--".join(quality_scores["high"])
        self.quality_mod_risk = "--".join(quality_scores["mod"])
        self.quality_low_risk = "--".join(quality_scores["low"])
        pmns_categorize_risk(self.hygiene, self.general_quality, self.finance)
    else:
        self.quality_high_risk = ""
        self.quality_mod_risk = ""
        self.quality_low_risk = ""
        self.risk_quality = "low"
        self.indicator_scores = {}
