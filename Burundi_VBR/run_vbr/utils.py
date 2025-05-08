import pandas as pd
import random, json
from difflib import get_close_matches
from openhexa.sdk import current_run


class Group_Orgunits:
    def __init__(self, name, qualite_indicators):
        self.qualite_indicators = qualite_indicators
        self.name = name
        self.members = []

    def set_cout_verification(self, cout_verification_centre):
        self.cout_verification_centre = cout_verification_centre

    def add_ou(self, ou):
        self.members.append(ou)

    def set_proportions(self, p_low, p_mod, p_high):
        self.p_low, self.p_mod, self.p_high = p_low, p_mod, p_high

    def get_verification_list(self):
        self.verification = pd.DataFrame(
            columns=[
                "period",
                "level_2_uid",
                "level_2_name",
                "level_3_uid",
                "level_3_name",
                "level_4_uid",
                "level_4_name",
                "level_5_uid",
                "level_5_name",
                "verified",
                "gain_verif_median_precedent",
                "gain_verif_actuel",
                "benefice_net_verification",
                "gain_perte_subside_taux_val",
                "taux_validation",
                "subside_dec_period_verif",
                "subside_val_period_verif",
                "subside_period_verif",
                "ecart_median",
                "categorie_risque",
                "indicateurs_qualite_risque_eleve",
                "indicateurs_qualite_risque_mod",
                "indicateurs_qualite_risque_faible",
            ]
            + self.qualite_indicators
        )
        proportions = {
            "low": self.p_low,
            "moderate": self.p_mod,
            "high": self.p_high,
            "uneligible": 1,
        }
        for ou in self.members:
            ou.set_verification(random.uniform(0, 1) <= proportions[ou.risk])
            if pd.isnull(ou.gain_verif_period_verif):
                benefice = pd.NA
            else:
                benefice = ou.gain_verif_period_verif - self.cout_verification_centre
            new_row = (
                [ou.period]
                + ou.identifier_verification
                + [ou.is_verified]
                + [
                    ou.gain_median,
                    ou.gain_verif_period_verif,
                    benefice,
                    ou.diff_methode_paiement,
                    ou.taux_validation,
                    ou.subside_dec_period_verif,
                    ou.subside_val_period_verif,
                    ou.subside_period_verif,
                    ou.ecart_median,
                    ou.risk,
                    ou.quality_high_risk,
                    ou.quality_mod_risk,
                    ou.quality_low_risk,
                ]
                + [ou.indicator_scores.get(i, pd.NA) for i in self.qualite_indicators]
            )
            try:
                self.verification.loc[self.verification.shape[0]] = new_row
            except:
                print("Catch error", len(new_row), new_row)
        return self.verification

    def get_detailled_list_dx(self):
        df = pd.DataFrame(
            columns=[
                "period",
                "level_5_uid",
                "level_5_name",
                "dx_name",
                "non_verified",
                "taux_validation",
                "categorie_centre",
                "nb_mois_non_vérifiés",
            ]
        )
        for ou in self.members:
            for dx in ou.dx_list:
                taux_validation = ou.quantite_window[ou.quantite_window.dx_name == dx][
                    "taux_validation"
                ].median()
                if pd.isnull(taux_validation):
                    taux_validation = ou.taux_validation
                non_verified = hot_encode(not ou.is_verified)
                ou_uid = ou.id
                ou_name = ou.name
                quarter = ou.quarter
                category = ou.category_centre
                new_row = [
                    quarter,
                    ou_uid,
                    ou_name,
                    dx,
                    non_verified,
                    taux_validation,
                    category,
                    len(ou.nb_periods_not_verified),
                ]
                df.loc[df.shape[0]] = new_row
        return df

    def get_statistics(self, period):
        stats = pd.DataFrame(
            columns=[
                "province",
                "periode",
                "#centres",
                "#risque élevé",
                "#risque modéré",
                "#risque faible",
                "# vérifiés",
                "cout vérif (VBR)",
                "cout vérif (syst)",
                "subsides santé (VBR)",
                "subsides santé (syst)",
                "cout total (VBR)",
                "cout total (syst)",
                "cout verif sur cout total (VBR)",
                "cout verif sur cout total (syst)",
                "#centres lese par taux validation",
                "#centres favorise par taux validation",
                "Total subsides sous évalués",
                "Total subsides sur-évalués",
                "perte médiane pour centres non-vérifiés",
                "gain médian pour centres vérifiés",
                "#_scores_qualite_risqués (centres vérifiés)",
                "#_scores_qualite_risqués (centres non-vérifiés)",
                "#_scores_qualite_non-risqués (centres vérifiés)",
                "#_scores_qualite_non-risqués (centres non-vérifiés)",
            ]
        )
        self.nb_centers = len(self.members)
        self.nb_centers_verified = self.verification[
            self.verification.verified == True
        ].shape[0]
        self.high = len(
            [
                ou.id
                for ou in self.members
                if ou.risk == "high" or ou.risk == "uneligible"
            ]
        )
        self.mod = len([ou.id for ou in self.members if ou.risk == "moderate"])
        self.low = len([ou.id for ou in self.members if ou.risk == "low"])
        self.cost_verification_vbr = (
            self.cout_verification_centre * self.nb_centers_verified
        )
        self.cost_verification_syst = self.cout_verification_centre * self.nb_centers
        self.benefice_net_unverified = self.verification[
            self.verification.verified == False
        ]["benefice_net_verification"].mean()
        self.benefice_net_verified = self.verification[
            self.verification.verified == True
        ]["benefice_net_verification"].mean()
        self.nb_centre_leses_method_paym = len(
            self.verification[
                (self.verification.verified == False)
                & (self.verification["gain_perte_subside_taux_val"] < 0)
            ]["level_5_uid"].unique()
        )
        self.nb_centre_favorises_method_paym = len(
            self.verification[
                (self.verification.verified == False)
                & (self.verification["gain_perte_subside_taux_val"] > 0)
            ]["level_5_uid"].unique()
        )
        self.subs_total_leses_method_paym = self.verification[
            (self.verification.verified == False)
            & (self.verification["gain_perte_subside_taux_val"] < 0)
        ]["gain_perte_subside_taux_val"].sum()
        self.subs_total_favorises_method_paym = self.verification[
            (self.verification.verified == False)
            & (self.verification["gain_perte_subside_taux_val"] > 0)
        ]["gain_perte_subside_taux_val"].sum()
        self.qualite_indicator_risque_eleve_unverified = (
            self.verification[self.verification.verified == False][
                "indicateurs_qualite_risque_eleve"
            ]
            .map(lambda x: len(x.split("--")))
            .mean()
        )
        self.qualite_indicator_risque_eleve_verified = (
            self.verification[self.verification.verified == True][
                "indicateurs_qualite_risque_eleve"
            ]
            .map(lambda x: len(x.split("--")))
            .mean()
        )
        self.qualite_indicator_risque_faible_unverified = (
            self.verification[self.verification.verified == False][
                "indicateurs_qualite_risque_faible"
            ]
            .map(lambda x: len(x.split("--")))
            .mean()
        )
        self.qualite_indicator_risque_faible_verified = (
            self.verification[self.verification.verified == True][
                "indicateurs_qualite_risque_faible"
            ]
            .map(lambda x: len(x.split("--")))
            .mean()
        )
        self.subsides_vbr = (
            self.verification[self.verification.verified == True][
                "subside_val_period_verif"
            ].sum()
            + self.verification[self.verification.verified == False][
                "subside_period_verif"
            ].sum()
        )
        self.subsides_syst = self.verification["subside_val_period_verif"].sum()
        self.cout_total_vbr = self.subsides_vbr + self.cost_verification_vbr
        self.cout_total_syst = self.subsides_syst + self.cost_verification_syst
        self.ratio_vbr = self.cost_verification_vbr / self.cout_total_vbr
        self.ratio_syst = self.cost_verification_syst / self.cout_total_syst
        new_row = [
            self.name,
            period,
            self.nb_centers,
            self.high,
            self.mod,
            self.low,
            self.nb_centers_verified,
            self.cost_verification_vbr,
            self.cost_verification_syst,
            self.subsides_vbr,
            self.subsides_syst,
            self.cout_total_vbr,
            self.cout_total_syst,
            self.ratio_vbr,
            self.ratio_syst,
            self.nb_centre_leses_method_paym,
            self.nb_centre_favorises_method_paym,
            self.subs_total_leses_method_paym,
            self.subs_total_favorises_method_paym,
            (-1) * self.benefice_net_unverified,
            self.benefice_net_verified,
            self.qualite_indicator_risque_eleve_verified,
            self.qualite_indicator_risque_eleve_unverified,
            self.qualite_indicator_risque_faible_verified,
            self.qualite_indicator_risque_faible_unverified,
        ]
        try:
            stats.loc[0] = new_row
        except:
            print("catch error", len(new_row), new_row)

        return stats


class Orgunit:
    def __init__(self, ou_id, quantite, qualite, qualite_indicators, uneligible_vbr):
        self.qualite_indicators = qualite_indicators
        self.start = quantite.month.min()
        if uneligible_vbr:
            self.risk = "uneligible"
            self.category_centre = "pca"
        else:
            self.risk = "unknown"
            self.category_centre = "pma"
        self.end = quantite.month.max()
        self.dx_list = list(quantite.dx_name.unique())
        self.quantite = quantite.sort_values(
            by=["level_5_uid", "dx_name", "quarter", "month"]
        )
        self.qualite = qualite.sort_values(
            by=["level_5_uid", "DE_name_officiel", "quarter"]
        ).drop_duplicates(["level_5_uid", "DE_name_officiel", "quarter"])
        self.quantite["month"] = self.quantite["month"].astype(str)
        self.qualite["month"] = self.qualite["month"].astype(str)
        self.id = ou_id
        self.identifier_verification = list(
            self.quantite[
                [
                    "level_2_uid",
                    "level_2_name",
                    "level_3_uid",
                    "level_3_name",
                    "level_4_uid",
                    "level_4_name",
                    "level_5_uid",
                    "level_5_name",
                ]
            ].values[0]
        )
        self.name = quantite.level_5_name.unique()[0]

    def set_verification(self, is_verified):
        self.is_verified = is_verified

    def eligible_for_VBR(self):
        if self.risk == "uneligible":
            return False
        self.nb_periods_verified = self.quantite_window[
            ~pd.isnull(self.quantite_window.val)
        ].month.unique()
        self.nb_periods_not_verified = self.quantite_window[
            self.quantite_window["non-vérifié"] == 1
        ].month.unique()

        if self.quantite_window[~pd.isnull(self.quantite_window.val)].shape[0] == 0:
            return False
        elif self.quantite_window.subside_sans_verification.sum() <= 50:
            print(
                self.name,
                "low preceeding subside:",
                self.quantite_window.subside_sans_verification.sum(),
            )
            return False
        self.verified_last_period = self.quantite_window[
            ~pd.isnull(self.quantite_window.val)
        ]["month"].max() >= str(months_before(self.month, 3))
        return (
            len(self.nb_periods_verified) >= self.nb_periods
            and self.verified_last_period
        )

    def set_frequence(self, freq):
        if freq == "trimestre":
            self.period_type = "quarter"
        else:
            self.period_type = "month"

    def set_month_verification(self, period):
        self.period = str(period)
        if self.period_type == "quarter":
            self.quarter = period
            self.month = str(quarter_to_months(period))
        else:
            self.month = period
            self.quarter = str(month_to_quarter(period))

    def set_window(self, window):
        self.window = max([window, 3])
        if self.period_type == "quarter":
            self.range = [
                str(elem)
                for elem in get_date_series(
                    str(months_before(self.month, self.window + 2)),
                    str(months_before(self.month, 3)),
                    "month",
                )
            ]
        else:
            self.range = [
                str(elem)
                for elem in get_date_series(
                    str(months_before(self.month, self.window)),
                    str(months_before(self.month, 1)),
                    "month",
                )
            ]
        self.quantite_window = self.quantite[self.quantite["month"].isin(self.range)]
        self.qualite_window = self.qualite[self.qualite["month"].isin(self.range)]

    def set_nb_verif_min_per_window(self, nb_periods):
        self.nb_periods = nb_periods

    def get_gain_verif_for_period_verif(self, taux_validation):
        quantite_period_verif = self.quantite[
            self.quantite[self.period_type] == self.period
        ]
        if quantite_period_verif.shape[0] > 0:
            self.subside_dec_period_verif = quantite_period_verif[
                "subside_sans_verification"
            ].sum()
            self.subside_val_period_verif = quantite_period_verif[
                "subside_avec_verification"
            ].sum()
            if taux_validation < 1:
                quantite_period_verif["subside_sans_verification_method_dpdt"] = (
                    taux_validation * quantite_period_verif["subside_sans_verification"]
                )
                quantite_period_verif["gain_verif_method_dpdt"] = (
                    quantite_period_verif["subside_sans_verification_method_dpdt"]
                    - quantite_period_verif["subside_avec_verification"]
                )
                self.subside_period_verif = quantite_period_verif[
                    "subside_sans_verification_method_dpdt"
                ].sum()
                self.diff_methode_paiement = (
                    self.subside_period_verif - self.subside_val_period_verif
                )
                self.gain_verif_period_verif = quantite_period_verif[
                    "gain_verif_method_dpdt"
                ].sum()
            else:
                self.diff_methode_paiement = 0
                self.subside_period_verif = self.subside_dec_period_verif
                self.gain_verif_period_verif = quantite_period_verif["gain_verif"].sum()

        else:
            self.gain_verif_period_verif = pd.NA
            self.subside_dec_period_verif = pd.NA
            self.subside_period_verif = pd.NA
            self.subside_val_period_verif = pd.NA
            self.diff_methode_paiement = pd.NA

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

            indicators = list(self.qualite.DE_name_officiel.unique())
            quality_scores = {"high": [], "mod": [], "low": []}
            self.indicator_scores = {}
            self.general_quality, self.hygiene, self.finance = 0, 0, 0
            for i in indicators:
                evolution = self.qualite_window[
                    self.qualite_window.DE_name_officiel == i
                ]["value"]
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

    def categorize_risk(
        self, gain_median_seuil, seuil_max_bas_risk, seuil_max_moyen_risk
    ):
        if self.eligible_for_VBR():
            self.get_ecart_median()
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

    def mix_risks(self, use_quality_for_risk):
        if use_quality_for_risk:
            risks = [self.risk_gain_median, self.risk_quantite, self.risk_quality]
        else:
            risks = [self.risk_gain_median, self.risk_quantite]
        if "uneligible" in risks:
            self.risk = "uneligible"
        elif "high" in risks:
            self.risk = "high"
        elif "moderate" in risks:
            self.risk = "moderate"
        else:
            self.risk = "low"

    def get_ecart_median(self):
        self.ecart_median = (
            self.quantite_window.groupby("dx_name", as_index=False)["ecart_dec_val"]
            .median()["ecart_dec_val"]
            .median()
        )

    def get_taux_validation_median(self):
        self.taux_validation = (
            self.quantite_window.groupby("dx_name", as_index=False)["taux_validation"]
            .median()["taux_validation"]
            .median()
        )

    def get_gain_median_par_periode(self):
        self.gain_median = (
            self.quantite_window.groupby(self.period_type, as_index=False)["gain_verif"]
            .sum()["gain_verif"]
            .median()
        )


def add_parents(df, parents):
    filtered_parents = {key: parents[key] for key in df["ou"] if key in parents}
    # Transform the `parents` dictionary into a DataFrame
    parents_df = pd.DataFrame.from_dict(filtered_parents, orient="index").reset_index()

    # Rename the index column to match the "ou" column
    parents_df.rename(
        columns={
            "index": "ou",
            "level_2_id": "level_2_uid",
            "level_3_id": "level_3_uid",
            "level_4_id": "level_4_uid",
            "level_5_id": "level_5_uid",
            "name": "level_5_name",
        },
        inplace=True,
    )

    # Join the DataFrame with the parents DataFrame on the "ou" column
    result_df = df.merge(parents_df, on="ou", how="left")
    return result_df


def hot_encode(condition):
    if condition:
        return 1
    else:
        return 0


def month_to_quarter(num):
    """
    Input:
    num (int) : a given month (e.g. 201808 )
    Returns: (str) the quarter corresponding to the given month (e.g. 2018Q3)
    """
    num = int(num)
    y = num // 100
    m = num % 100
    return str(y) + "Q" + str((m - 1) // 3 + 1)


def quarter_to_months(name):
    """
    Input:
    name (str) : a given quarter (e.g. 2018Q3)
    Returns: (int) the third month of the quarter (e.g. 201809)
    """
    year, quarter = str(name).split("Q")
    return int(year) * 100 + int(quarter) * 3


def months_before(date, lag):
    """
    Input:
    - date (int) : a given month (e.g. 201804)
    - lag (int) : number of months before (e.g. 6)

    Returns: a month (int) corresponding to the period that is "lag" months before "date"
    e.g. : 201710
    """
    date = int(date)
    year = date // 100
    m = date % 100
    lag_years = lag // 12
    year -= lag_years
    lag = lag - 12 * lag_years
    diff = m - lag
    if diff > 0:
        return year * 100 + m - lag
    else:
        year -= 1
        m = 12 + diff
        return year * 100 + m


def get_month(mois, year):
    return year * 100 + mois


def period_to_quarter(p):
    p = int(p)
    year = p // 100
    quarter = ((p % 100) - 1) // 3 + 1
    return f"{year}Q{quarter}"


def get_date_series(start, end, type):
    from openhexa.toolbox.dhis2.periods import Month, Quarter

    """
    Input:
    - start (int) : a given starting month (e.g. 201811)
    - end (int) : a given ending month (e.g. 201811)

    Returns: a list of consecutive months (int) starting with "start" and ending
    with "end"
    """
    if type == "quarter":
        q1 = Quarter(start)
        q2 = Quarter(end)
        range = q1.get_range(q2)
    else:
        m1 = Month(start)
        m2 = Month(end)
        range = m1.get_range(m2)
    return range


def last_quarter(year, quarter):
    if quarter == 1:
        return year - 1, 4
    else:
        return year, quarter - 1


def create_short_name(name, suffixe):
    abrv = {"non vérifié": "NVRF", "taux validation": "TV"}
    short_name = ""
    name_decomposed = name.split()
    for i, word in enumerate(name_decomposed):
        # Limit to 6 letters per word
        limited_word = word[:6]
        short_name += limited_word[0].upper()  # First letter capitalized
        short_name += limited_word[1:6].lower()  # First six letters in lowercase
    return short_name[: 50 - len(abrv[suffixe]) - 1] + "-" + abrv[suffixe]


def get_DE_ids_from_DS(dhis, ds_id):
    ids = [
        elem["dataElement"]["id"]
        for elem in dhis.api.get(f"29/dataSets/{ds_id}.json").json()["dataSetElements"]
    ]
    dataElements = {
        dhis.api.get(f"29/dataElements/{id}.json").json()["name"]: id for id in ids
    }
    return dataElements


def create_dict_dx_name_type_ids(dataElements, suffixe):
    taux_validation_dx = {}
    for dx in dataElements:
        dx_decomposed = dx.split("-")
        dx_name = "-".join(dx_decomposed[:-1]).strip()
        data_type = dx_decomposed[-1].strip()
        if data_type == suffixe:
            taux_validation_dx.setdefault(dx_name, dataElements[dx])
    filename = f"{suffixe}_dx.json"
    with open(filename, "w") as json_file:
        json.dump(
            taux_validation_dx, json_file, indent=4
        )  # `indent` is optional for pretty printing
    return taux_validation_dx


def closest_string(target, strings):
    return get_close_matches(target, strings, n=1)


def check_presence_dx_name_dict_dx_type(dic, dx_names):
    for dx in dx_names:
        if dx.strip() not in dic:
            print(dx, closest_string(dx, list(dic.keys())))


def check_add_missing_DEs_to_DHIS2(
    dhis, dx_names, verif_dx, suffixe, aggregation_type, data_type, test=True
):
    added = 0
    for dx in dx_names:
        if dx not in verif_dx:
            if not test:
                success = add_DE_to_DHIS2(
                    dhis, dx, suffixe, aggregation_type, data_type
                )
                if success:
                    current_run.log_info(dx, suffixe, "added to DHIS2")
                    added += 1
            print("suffixe", dx, closest_string(dx, list(verif_dx.keys())))
        else:
            print(dx, suffixe, "already in DHIS2")
    return added


def add_DE_to_DHIS2(dhis, dx_name, suffixe, aggregation_type, data_type):
    name = dx_name + " - " + suffixe
    short_name = create_short_name(dx_name, suffixe)
    print(name, short_name)
    dataElement = {
        "aggregationType": aggregation_type,
        "domainType": "AGGREGATE",
        "valueType": data_type,
        "zeroIsSignificant": "true",
        "name": name,
        "shortName": short_name,
    }
    r = dhis.api.post("29/dataElements", json=dataElement)
    return r.json()["response"]["importCount"]["imported"] == 1


import requests


def run_analytics(base_url: str, username: str, password: str) -> str:
    """Update analytics tables."""
    with requests.Session() as s:
        s.auth = requests.auth.HTTPBasicAuth(username, password)
        r = s.post(f"{base_url}/api/resourceTables/analytics")
        r.raise_for_status()
        return r.json()["response"]["id"]


def last_analytics_update(base_url: str, username: str, password: str) -> str:
    """Get date of latest analytics update."""
    with requests.Session() as s:
        s.auth = requests.auth.HTTPBasicAuth(username, password)
        r = s.get(f"{base_url}/api/system/info")
        r.raise_for_status()
        return r.json()["lastAnalyticsTableSuccess"][:10]


def analytics_ready(job_id: str, base_url: str, username: str, password: str) -> bool:
    """Check if analytics tables are ready."""
    with requests.Session() as s:
        s.auth = requests.auth.HTTPBasicAuth(username, password)
        r = s.get(f"{base_url}/api/system/tasks/ANALYTICS_TABLE/{job_id}")
        r.raise_for_status()
    for task in r.json():
        if task.get("completed"):
            return True
    return False


def add_higher_levels_and_names(dhis, data):
    lou = {ou["id"]: ou for ou in dhis.meta.organisation_units()}
    res = [
        {
            "dx": row.dx,
            "dx_name": row.dx_name,
            "period": int(row.pe),
            "level_5_uid": row["ou"],
            "level_5_name": lou[row["ou"]].get("name"),
            "level_4_uid": lou[row["ou"]]["path"].strip("/").split("/")[3],
            "level_4_name": lou[lou[row["ou"]]["path"].strip("/").split("/")[3]].get(
                "name"
            ),
            "level_3_uid": lou[row["ou"]]["path"].strip("/").split("/")[2],
            "level_3_name": lou[lou[row["ou"]]["path"].strip("/").split("/")[2]].get(
                "name"
            ),
            "level_2_uid": lou[row["ou"]]["path"].strip("/").split("/")[1],
            "level_2_name": lou[lou[row["ou"]]["path"].strip("/").split("/")[1]].get(
                "name"
            ),
            "value": int(float(row.value)),
        }
        for i, row in data.iterrows()
        if not row.isnull().any()
    ]
    return pd.DataFrame(res)


def period_to_quarter(p):
    p = int(p)
    year = p // 100
    quarter = ((p % 100) - 1) // 3 + 1
    return f"{year}Q{quarter}"


def get_date_series(start, end, type):
    from openhexa.toolbox.dhis2.periods import Month, Quarter

    """
    Input:
    - start (int) : a given starting month (e.g. 201811)
    - end (int) : a given ending month (e.g. 201811)

    Returns: a list of consecutive months (int) starting with "start" and ending
    with "end"
    """
    if type == "quarter":
        q1 = Quarter(start)
        q2 = Quarter(end)
        range = q1.get_range(q2)
    else:
        m1 = Month(start)
        m2 = Month(end)
        range = m1.get_range(m2)
    return range


def calcul_ecarts(q):
    q["ecart_dec_ver"] = q.apply(
        lambda x: abs(x.dec - x.ver) / x.ver if x.ver != 0 else x.dec,
        axis=1,
    )
    q["ecart_ver_val"] = q.apply(
        lambda x: abs(x.ver - x.val) / x.ver if x.ver != 0 else 0,
        axis=1,
    )
    q["taux_validation"] = q.apply(
        lambda x: min([1, 1 - (x.dec - x.val) / x.dec]) if x.dec != 0 else pd.NA,
        axis=1,
    )
    q["ecart_dec_val"] = 0.4 * q["ecart_dec_ver"] + 0.6 * q["ecart_ver_val"]
    return q
