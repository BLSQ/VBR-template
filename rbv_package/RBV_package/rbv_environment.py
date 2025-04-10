import pandas as pd
import random, json
from difflib import get_close_matches
from openhexa.toolbox.dhis2.periods import period_from_string, Period
from openhexa.sdk import current_run
from openhexa.sdk import workspace
import requests
import os
import copy
from typing import List
import csv


def deserialize(content):
    """
    :param content: A JSON API document already
    :returns: The JSON API document parsed
    """
    if "errors" in content:
        return content

    if "data" not in content:
        raise AttributeError("This is not a JSON API document")

    # be nondestructive with provided content
    content = copy.deepcopy(content)

    if "included" in content:
        included = _parse_included(content["included"])
    else:
        included = {}
    if isinstance(content["data"], dict):
        return _resolve(_flat(content["data"]), included, set())
    elif isinstance(content["data"], list):
        result = []
        for obj in content["data"]:
            result.append(_resolve(_flat(obj), included, set()))
        return result
    else:
        return None


def _resolve(data, included, resolved, deep=True):
    if not isinstance(data, dict):
        return data
    keys = data.keys()
    if keys == {"type", "id"} or keys == {"type", "id", "meta"}:
        type_id = data["type"], data["id"]
        meta = data.get("meta")
        resolved_item = included.get(type_id, data)
        resolved_item = resolved_item.copy()
        if type_id not in resolved:
            data = _resolve(resolved_item, included, resolved | {type_id})
        if meta is not None:
            data = data.copy()
            data.update(meta=meta)
        return data
    for key, value in data.items():
        if isinstance(value, dict):
            data[key] = _resolve(value, included, resolved)
        elif isinstance(value, list):
            if deep:
                data[key] = [_resolve(item, included, resolved, False) for item in value]
        else:
            data[key] = value
    return data


def _parse_included(included):
    result = {}
    for include in included:
        result[(include["type"], include["id"])] = _flat(include)
    return result


def _flat(obj):
    obj.pop("links", None)
    obj.update(obj.pop("attributes", {}))
    if "relationships" in obj:
        for relationship, item in obj.pop("relationships").items():
            data = item.get("data")
            links = item.get("links")
            if data is not None:
                obj[relationship] = data
            elif links:
                obj[relationship] = item
            else:
                obj[relationship] = None
    return obj


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
    q["weighted_ecart_dec_val"] = 0.4 * q["ecart_dec_ver"] + 0.6 * q["ecart_ver_val"]
    q["ecart_dec_val"] = q.apply(
        lambda x: abs(x.dec - x.val) / x.ver if x.ver != 0 else 0,
        axis=1,
    )
    return q


def get_org_unit_ids(dhis, group_id):
    org_units = set()
    for page in dhis.api.get_paged(
        f"organisationUnitGroups/{group_id}",
        params={
            "fields": "organisationUnits",
            "pageSize": 10,
        },
    ):
        org_units = org_units.union({ou_id["id"] for ou_id in page["organisationUnits"]})
    return org_units


def get_org_unit_ids_from_hesabu(contract_group, hesabu_package, dhis):
    ou_groups = [
        (g["id"], g["name"]) for g in hesabu_package["orgUnitGroups"] if g["id"] != contract_group
    ]
    ous = set()
    for group_id, group_name in ou_groups:
        ous = ous.union(get_org_unit_ids(dhis, group_id))
    return ous


def fetch_data_values(dhis, deg_external_reference, org_unit_ids, periods, activities, package_id):
    for monthly_period in periods:
        if os.path.exists(f"{workspace.files_path}/packages/{package_id}/{monthly_period}.csv"):
            current_run.log_info(
                f"Data for package {package_id} for {monthly_period} already fetched"
            )
            continue
        chunks = {}
        values = []
        nb_org_unit_treated = 0
        for i in range(1, len(org_unit_ids) + 1):
            chunks.setdefault(i // 10, []).append(org_unit_ids[i - 1])
        for i in chunks:
            data_values = {}
            param_ou = "".join([f"&orgUnit={ou}" for ou in chunks[i]])
            url = f"dataValueSets.json?dataElementGroup={deg_external_reference}{param_ou}&period={monthly_period}"
            res = dhis.api.get(url)
            # data_values.exten
            if "dataValues" in res:
                data_values = res["dataValues"]
            else:
                continue
            for org_unit_id in chunks[i]:
                for activity in activities:
                    current_value = {
                        "period": monthly_period,
                        "org_unit_id": org_unit_id,
                        "activity_name": activity["name"],
                        "activity_code": activity["code"],
                    }
                    some_values = False
                    for code in activity.get("inputMappings").keys():
                        input_mapping = activity.get("inputMappings").get(code)
                        selected_values = [
                            dv
                            for dv in data_values
                            if dv["orgUnit"] == org_unit_id
                            and str(dv["period"]) == str(monthly_period)
                            and dv["dataElement"] == input_mapping["externalReference"]
                        ]
                        if len(selected_values) > 0:
                            # print(code, monthly_period, org_unit_id, len(selected_values), selected_values[0]["value"] if len(selected_values) >0 else None)
                            try:
                                current_value[code] = selected_values[0]["value"]
                                some_values = True
                            except:
                                print(
                                    "Error",
                                    code,
                                    monthly_period,
                                    org_unit_id,
                                    len(selected_values),
                                    selected_values[0],
                                )

                    if some_values:
                        values.append(current_value)
            nb_org_unit_treated += 10
            if nb_org_unit_treated % 100 == 0:
                current_run.log_info(f"{nb_org_unit_treated} org units treated")
        values_df = pd.DataFrame(values)
        if values_df.shape[0] > 0:
            if not os.path.exists(f"{workspace.files_path}/packages/{package_id}"):
                os.makedirs(f"{workspace.files_path}/packages/{package_id}")
            values_df.to_csv(
                f"{workspace.files_path}/packages/{package_id}/{monthly_period}.csv",
                index=False,
            )
            current_run.log_info(
                f"Data ({len(values_df)}) for package {package_id} for {monthly_period} treated"
            )


class Group_Orgunits:
    def __init__(self, name, qualite_indicators):
        self.qualite_indicators = qualite_indicators
        self.name = name
        self.members = []

    def set_cout_verification(self, cout_verification_centre):
        self.cout_verification_centre = cout_verification_centre

    def add_ou(self, ou):
        self.members.append(ou)

    def set_proportions(self, proportions):
        self.proportions = proportions

    def get_verification_list(self):
        # Modify the number of risk categories and their proportion freely :)
        self.verification = pd.DataFrame(
            columns=[
                "period",
                "ou",
                "level_2_uid",
                "level_2_name",
                "level_3_uid",
                "level_3_name",
                "level_4_uid",
                "level_4_name",
                "level_5_uid",
                "level_5_name",
                "level_6_uid",
                "level_6_name",
                "verified",
                "gain_verif_median_precedent",
                "gain_verif_actuel",
                "benefice_net_verification",
                "gain_perte_subside_taux_val",
                "taux_validation",
                "subside_dec_period_verif",
                "subside_val_period_verif",
                "subside_period_dec_taux_validation",
                "ecart_median",
                "categorie_risque",
                "indicateurs_qualite_risque_eleve",
                "indicateurs_qualite_risque_mod",
                "indicateurs_qualite_risque_faible",
            ]
            + self.qualite_indicators
        )
        ## CUSTOM - Modify the proportion of each risk category freely :)

        for ou in self.members:
            ou.set_verification(random.uniform(0, 1) <= self.proportions[ou.risk])
            if pd.isnull(ou.gain_verif_period_verif):
                benefice = pd.NA
            else:
                benefice = ou.gain_verif_period_verif - self.cout_verification_centre
            new_row = (
                [ou.period]
                + [ou.id]
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
                    ou.subside_period_dec_taux_validation,
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
                "ou",
                "name",
                "service",
                "non_verified",
                "taux_validation",
                "categorie_centre",
                "nb_mois_non_vérifiés",
            ]
        )
        for ou in self.members:
            for dx in ou.dx_list:
                taux_validation = ou.quantite_window[ou.quantite_window.service == dx][
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

    def save_orgunits_to_csv(self, filepath: str):
        # Get attributes from the first object

        orgunits = self.members

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        # Write to CSV
        res = pd.DataFrame()
        for obj in orgunits:
            row = {
                attr: getattr(obj, attr)
                for attr in dir(obj)
                if not attr.startswith("_")
                and not callable(getattr(obj, attr))
                and isinstance(getattr(obj, attr), (int, float, str))
            }
            res = pd.concat([res, pd.DataFrame([row], index=[0])], ignore_index=True)
        res.to_csv(filepath, index=False)

        print(f"Saved {len(orgunits)} Orgunit(s) to '{filepath}'.")

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
        self.nb_centers_verified = self.verification[self.verification.verified == True].shape[0]
        self.high = len(
            [ou.id for ou in self.members if ou.risk == "high" or ou.risk == "uneligible"]
        )
        ## CUSTOM - Modify the assignation based on levels of risks :)
        self.mod = len([ou.id for ou in self.members if "moderate" in ou.risk])
        self.low = len([ou.id for ou in self.members if ou.risk == "low"])
        self.cost_verification_vbr = self.cout_verification_centre * self.nb_centers_verified
        self.cost_verification_syst = self.cout_verification_centre * self.nb_centers
        self.benefice_net_unverified = self.verification[self.verification.verified == False][
            "benefice_net_verification"
        ].mean()
        self.benefice_net_verified = self.verification[self.verification.verified == True][
            "benefice_net_verification"
        ].mean()
        self.nb_centre_leses_method_paym = len(
            self.verification[
                (self.verification.verified == False)
                & (self.verification["gain_perte_subside_taux_val"] < 0)
            ]["ou"].unique()
        )
        self.nb_centre_favorises_method_paym = len(
            self.verification[
                (self.verification.verified == False)
                & (self.verification["gain_perte_subside_taux_val"] > 0)
            ]["ou"].unique()
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
            self.verification[self.verification.verified == True]["subside_val_period_verif"].sum()
            + self.verification[self.verification.verified == False][
                "subside_period_dec_taux_validation"
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
        if "level_6_uid" not in quantite.columns:
            quantite["level_6_uid"] = pd.NA
            quantite["level_6_name"] = pd.NA
            qualite["level_6_uid"] = pd.NA
            qualite["level_6_name"] = pd.NA

        self.start = quantite.month.min()
        if uneligible_vbr:
            self.risk = "uneligible"
            self.category_centre = "pca"
        else:
            self.risk = "unknown"
            self.category_centre = "pma"
        self.end = quantite.month.max()
        self.dx_list = list(quantite.service.unique())
        self.quantite = quantite.sort_values(by=["ou", "service", "quarter", "month"])
        self.qualite = qualite.sort_values(by=["ou", "indicator", "quarter"]).drop_duplicates(
            ["ou", "indicator", "quarter"]
        )
        self.quantite["month"] = self.quantite["month"].astype(str)
        self.qualite["month"] = self.qualite["month"].astype(str)
        self.id = ou_id
        self.level = self.quantite.level.unique()[0]
        self.name = self.quantite[f"level_{self.level}_uid"].unique()[0]
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
                    "level_6_uid",
                    "level_6_name",
                ]
            ].values[0]
        )

    def set_verification(self, is_verified):
        self.is_verified = is_verified

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
        quantite_period_verif_total = self.quantite[
            self.quantite[self.period_type] == self.period
        ].copy()
        if quantite_period_verif_total.shape[0] > 0:
            (
                self.subside_dec_period_verif,
                self.subside_val_period_verif,
                self.subside_period_dec_taux_validation,
                self.gain_verif_period_verif,
                self.diff_methode_paiement,
            ) = 0, 0, 0, 0, 0
            for dx in quantite_period_verif_total.service.unique():
                quantite_period_verif = quantite_period_verif_total[
                    quantite_period_verif_total.service == dx
                ].copy()
                if len(quantite_period_verif) > 0:
                    self.subside_dec_period_verif += quantite_period_verif[
                        "subside_sans_verification"
                    ].sum()
                    self.subside_val_period_verif += quantite_period_verif[
                        "subside_avec_verification"
                    ].sum()
                    if taux_validation < 1:
                        quantite_period_verif["subside_sans_verification_method_dpdt"] = (
                            quantite_period_verif.apply(
                                lambda x: x["subside_sans_verification"]
                                * list(
                                    self.taux_validation_par_service[
                                        self.taux_validation_par_service.service == dx
                                    ]["taux_validation"].values
                                )[0]
                                if len(
                                    self.taux_validation_par_service[
                                        self.taux_validation_par_service.service == dx
                                    ]
                                )
                                > 0
                                else x.subside_sans_verification
                                * self.taux_validation_par_service["taux_validation"].mean(),
                                axis=1,
                            )
                        )
                    else:
                        quantite_period_verif["subside_sans_verification_method_dpdt"] = (
                            quantite_period_verif["subside_sans_verification"]
                        )
                    quantite_period_verif["gain_verif_method_dpdt"] = (
                        quantite_period_verif["subside_sans_verification_method_dpdt"]
                        - quantite_period_verif["subside_avec_verification"]
                    )

                    self.subside_period_dec_taux_validation += quantite_period_verif[
                        "subside_sans_verification_method_dpdt"
                    ].sum()
                    self.diff_methode_paiement = (
                        self.subside_period_dec_taux_validation - self.subside_val_period_verif
                    )
                    self.gain_verif_period_verif += quantite_period_verif[
                        "gain_verif_method_dpdt"
                    ].sum()
                else:
                    self.diff_methode_paiement += 0
                    self.subside_period_dec_taux_validation += self.subside_dec_period_verif
                    self.gain_verif_period_verif += quantite_period_verif["gain_verif"].sum()

        else:
            self.gain_verif_period_verif = pd.NA
            self.subside_dec_period_verif = pd.NA
            self.subside_period_dec_taux_validation = pd.NA
            self.subside_val_period_verif = pd.NA
            self.diff_methode_paiement = pd.NA

    def mix_risks(self, use_quality_for_risk):
        if use_quality_for_risk:
            risks = [self.risk_gain_median, self.risk_quantite, self.risk_quality]
        else:
            risks = [self.risk_gain_median, self.risk_quantite]
        if "uneligible" in risks:
            self.risk = "uneligible"
        elif "high" in risks:
            self.risk = "high"
        elif "moderate_1" in risks:
            self.risk = "moderate_1"
        elif "moderate_2" in risks:
            self.risk = "moderate_2"
        elif "moderate_3" in risks:
            self.risk = "moderate_3"
        elif "moderate" in risks:
            self.risk = "moderate"
        else:
            self.risk = "low"

    def get_ecart_median_per_service(self):
        self.ecart_median_per_service = (
            self.quantite_window.groupby("service", as_index=False)["weighted_ecart_dec_val"]
            .median()
            .rename(columns={"weighted_ecart_dec_val": "ecart_median"})
        )
        self.gain_median = (
            self.quantite_window.groupby(self.period_type, as_index=False)["gain_verif"]
            .sum()["gain_verif"]
            .median()
        )

    def get_ecart_median(self):
        self.ecart_median = (
            self.quantite_window.groupby("service", as_index=False)["weighted_ecart_dec_val"]
            .median()["weighted_ecart_dec_val"]
            .median()
        )

    def get_taux_validation_median(self):
        self.taux_validation = (
            self.quantite_window.groupby("service", as_index=False)["taux_validation"]
            .median()["taux_validation"]
            .median()
        )
        self.taux_validation_par_service = self.quantite_window.groupby("service", as_index=False)[
            "taux_validation"
        ].median()

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
            "level_4_name": lou[lou[row["ou"]]["path"].strip("/").split("/")[3]].get("name"),
            "level_3_uid": lou[row["ou"]]["path"].strip("/").split("/")[2],
            "level_3_name": lou[lou[row["ou"]]["path"].strip("/").split("/")[2]].get("name"),
            "level_2_uid": lou[row["ou"]]["path"].strip("/").split("/")[1],
            "level_2_name": lou[lou[row["ou"]]["path"].strip("/").split("/")[1]].get("name"),
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


def last_quarter(year, quarter):
    if quarter == 1:
        return year - 1, 4
    else:
        return year, quarter - 1
