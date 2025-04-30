import pandas as pd
from openhexa.sdk import current_run
from openhexa.sdk import workspace
import os
import copy


def deserialize(content):
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


def get_org_unit_ids(dhis, group_id):
    """
    Get the IDs of the organizational Units that we need to look at for the concrete group.

    Parameters
    ----------
    DHIS2: object
        Connection to the DHIS2 instance.
    group_id: str
        Key to find the IDs of the organizational units that we need to look at.

    Returns
    -------
    Set of IDs of the organizational units that we need to look at.
    """
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
    """
    Get the IDs of the organizational Units that we need to look at

    Parameters
    ----------
    contract_group: str
        The ID of the contract group that we are interested in.
    hesabu_package: dict
        Keys to find the IDs of the organizational units that we need to look at.
    DHIS2: object
        Connection to the DHIS2 instance.

    Returns
    -------
    Set of IDs of the organizational units that we need to look at.
    """
    ou_groups = [
        (g["id"], g["name"]) for g in hesabu_package["orgUnitGroups"] if g["id"] != contract_group
    ]
    ous = set()
    for group_id, group_name in ou_groups:
        ous = ous.union(get_org_unit_ids(dhis, group_id))
    return ous


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
