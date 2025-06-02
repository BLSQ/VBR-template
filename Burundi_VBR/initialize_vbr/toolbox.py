import pandas as pd
from openhexa.sdk import current_run
from openhexa.sdk import workspace
import os
import copy


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
    code_coc: dict, optional
        A dictionary containing the CategoryOptionCombo for some codes.
        If it is empty or the code is not in the dictionary, we do not demand a CategoryOptionCombo.
    """
    for monthly_period in periods:
        if os.path.exists(f"{path}/{package_id}/{monthly_period}.csv"):
            current_run.log_info(
                f"Data for package {package_id} for {monthly_period} already fetched"
            )
            continue
        chunks = {}
        values = []
        nb_org_unit_treated = 0
        for i in range(1, len(org_unit_ids) + 1):
            chunks.setdefault(i // 10, []).append(org_unit_ids[i - 1])
        for i, _ in chunks.items():
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
                        if "categoryOptionCombo" in input_mapping.keys():
                            selected_values = [
                                dv
                                for dv in data_values
                                if dv["orgUnit"] == org_unit_id
                                and str(dv["period"]) == str(monthly_period)
                                and dv["dataElement"] == input_mapping["externalReference"]
                                and dv["categoryOptionCombo"]
                                == input_mapping["categoryOptionCombo"]
                            ]
                        else:
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
            if not os.path.exists(f"{path}/{package_id}"):
                os.makedirs(f"{path}/{package_id}")
            values_df.to_csv(
                f"{path}/{package_id}/{monthly_period}.csv",
                index=False,
            )
            current_run.log_info(
                f"Data ({len(values_df)}) for package {package_id} for {monthly_period} treated"
            )
