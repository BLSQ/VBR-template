import pandas as pd
from openhexa.sdk import current_run
import os

import config


def fetch_data_values(
    dhis, deg_external_reference, org_unit_ids, periods, activities, package_id, path
):
    """
    Get the datavalues from DHIS2

    Parameters
    ----------
    dhis: object
        Connection to the DHIS2 instance.
    deg_external_reference: str
        It will help us to find the data values we are interested in.
    org_unit_ids: list
        The IDs of the organizational units we are interested in.
    periods: list
        The periods we are interested in.
    activities: list
        The activities we are interested in.
    package_id: str
        The ID of the package we are interested in.
    path: str
        The path where the packages are stored.
    """
    for quarter, list_months in periods.items():
        if os.path.exists(f"{path}/{package_id}/{quarter}.csv"):
            current_run.log_info(f"Data for package {package_id} for {quarter} already fetched")
            continue

        chunks = {}
        values_validated = []
        values_declared = []
        nb_org_unit_treated = 0

        for i in range(1, len(org_unit_ids) + 1):
            chunks.setdefault(i // 10, []).append(org_unit_ids[i - 1])

        for i, _ in chunks.items():
            param_ou = "".join(
                [f"&orgUnit={ou}" for ou in chunks[i] if ou not in config.list_of_ous_not_to_see]
            )

            values_validated = get_values_for_ous(
                dhis,
                deg_external_reference,
                param_ou,
                quarter,
                activities,
                values_validated,
                chunks[i],
            )
            for month in list_months:
                values_declared = get_values_for_ous(
                    dhis,
                    deg_external_reference,
                    param_ou,
                    month,
                    activities,
                    values_declared,
                    chunks[i],
                )

            nb_org_unit_treated += 10
            if nb_org_unit_treated % 100 == 0:
                current_run.log_info(f"{nb_org_unit_treated} org units treated")

        values_validated = pd.DataFrame(values_validated)
        values_declared = pd.DataFrame(values_declared)

        if values_validated.shape[0] > 0:
            values_declared["declare"] = pd.to_numeric(values_declared["declare"], errors="coerce")
            values_declared_sum = values_declared.groupby(
                ["activity_name", "org_unit_id", "activity_code"], as_index=False
            )["declare"].sum()
            values = pd.merge(
                values_declared_sum,
                values_validated,
                on=["activity_name", "org_unit_id", "activity_code"],
                how="outer",
            )
            values["period"] = values["period"].fillna(quarter)
            values["declare"] = values["declare"].fillna(0)
            values["valide"] = values["valide"].fillna(0)
            if not os.path.exists(f"{path}/{package_id}"):
                os.makedirs(f"{path}/{package_id}")
            values.to_csv(
                f"{path}/{package_id}/{quarter}.csv",
                index=False,
            )
            current_run.log_info(
                f"Data ({len(values)}) for package {package_id} for {quarter} treated"
            )


def get_monts_in_quarter(quarter_str):
    """
    From a quarter, get the three months in that quarter.

    Parameters
    ----------
    quarter_str: str
        A given quarter in the format 'YYYYQn' (e.g. '2024Q1').

    Returns
    -------
    list
        A list of strings representing the three months in the quarter (e.g. ['202401', '202402', '202403']).
    """
    year = int(quarter_str[:4])
    q = int(quarter_str[-1])
    start_month = (q - 1) * 3 + 1
    return [f"{year}{month:02d}" for month in range(start_month, start_month + 3)]


def get_values_for_activity(period, org_unit_id, activity, data_values, values):
    """
    For a particular activity (a medical service that is being provided), organizational unit, and period, get its value from the
    data values list.

    Parameters
    ----------
    period: str
        The period for which we want to get the values.
    org_unit_id: str
        The ID of the organizational unit for which we want to get the values.
    activity: dict
        The activity for which we want to get the values.
    data_values: list
        The list of dataelements that we have fetch from DHIS2.
    values: list
        The list of already extracted values. We will append the new one.

    Returns
    -------
    list
        A list of dictionaries containing the values for the given activity, organizational unit, and period.
    """
    current_value = {
        "period": period,
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
            and str(dv["period"]) == str(period)
            and dv["dataElement"] == input_mapping["externalReference"]
        ]
        if len(selected_values) > 0:
            current_value[code] = selected_values[0]["value"]
            some_values = True

    if some_values:
        values.append(current_value)

    return values


def get_values_for_ous(
    dhis, deg_external_reference, group_ou, period, activities, values_validated, chunk
):
    """
    For a particular dataElementGroup, group of ou's, and period
    Get the values for a particular list of activities.

    Parameters
    ----------
    dhis: object
        Connection to the DHIS2 instance.
    deg_external_reference: str
        The ID of the dataElementGroup we are interested in.
    group_ou: str
        The group of organizational units we are interested in.
    period: str
        The period for which we want to get the values.
    activities: list
        The list of activities for which we want to get the values.
    values_validated: list
        The list of already extracted values. We will append the new one.
    chunk: list
        A chunk of organizational units to process.

    Returns
    -------
    list
        The validated values with the new one appended.

    """
    url_validated = (
        f"dataValueSets.json?dataElementGroup={deg_external_reference}{group_ou}&period={period}"
    )
    res_validated = dhis.api.get(url_validated)

    if "dataValues" in res_validated:
        data_values = res_validated["dataValues"]
    else:
        return values_validated

    for org_unit_id in chunk:
        for activity in activities:
            values_validated = get_values_for_activity(
                period, org_unit_id, activity, data_values, values_validated
            )

    return values_validated


def calcul_ecarts(q):
    """
    Calculate the relations between the declared, verified and validated values.

    Parameters
    ----------
    q : pd.DataFrame
        DataFrame containing the quantitative information for the particular Organizational Unit

    Returns
    -------
    q: pd.DataFrame
        The same DataFrame with the new columns added.
    """
    q["taux_validation"] = q.apply(
        lambda x: min([1, 1 - (x.dec - x.val) / x.dec]) if x.dec != 0 else pd.NA,
        axis=1,
    )
    q["ecart_dec_val"] = q.apply(
        lambda x: abs(x.dec - x.val) / x.dec if x.dec != 0 else 0,
        axis=1,
    )
    q["weighted_ecart_dec_val"] = q["ecart_dec_val"]
    return q
