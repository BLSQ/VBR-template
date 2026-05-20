from openhexa.sdk import (
    current_run,
    pipeline,
    workspace,
    parameter,
    DHIS2Connection,
    File,
)
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import (
    get_organisation_units,
)
import pandas as pd
import regex as re
import os
from pathlib import Path
import json
import requests
from datetime import datetime

import config


@pipeline("push-vbr", timeout=20000)
@parameter(
    "dhis_con",
    type=DHIS2Connection,
    help="Connection to DHIS2",
    default="pbf-burundi",
    required=True,
)
@parameter(
    "file_to_push",
    name="File with the information to push",
    type=File,
    required=True,
)
@parameter(
    "dry_run_taux",
    name="Dry Run Taux",
    type=bool,
    default=True,
    help="If False, we will actually push the taux data to DHIS2",
)
@parameter(
    "dry_run_ver",
    name="Dry Run Verification",
    type=bool,
    default=True,
    help="If False, we will actually push the verification data to DHIS2",
)
@parameter("add_gasc", name="Push also verification for GASC", type=bool, default=False)
@parameter("add_other_ous", name="Push verification for OUs not in list", type=bool, default=True)
def push_vbr(dhis_con, file_to_push, dry_run_taux, dry_run_ver, add_gasc, add_other_ous):
    """
    Pipeline to push the taux de validation and center validation to DHIS2.
    """
    output_path, data, dt = get_data(file_to_push)
    # output_path, data, dt = get_data_mod()
    dhis = get_dhis2(dhis_con)
    check_data_full(data)
    data_taux, data_ver = divide_data_taux_ver(data)
    if add_other_ous:
        all_ous = get_full_list_of_ous()
        data_ver = add_other_ous_to_data(data_ver, all_ous)
    if add_gasc:
        org_units = get_organisation_units(dhis).to_pandas()
        data_ver = add_gasc_children(data_ver, org_units)
    services = get_service_codes()
    data_taux = merge_service_codes(data_taux, services)
    check_data_taux(data_taux)
    data_taux_list = prepare_taux_for_dhis(data_taux, output_path, dry_run_taux, dt)
    data_ver_list = prepare_ver_for_dhis(data_ver, output_path, dry_run_ver, dt)
    push_to_dhis2(
        dhis, data_taux_list, data_ver_list, dry_run_taux, dry_run_ver, output_path, dt=dt
    )


def divide_data_taux_ver(data):
    """Divide the data into two DataFrames: one for the taux de validation
    and one for the verification information.

    Returns
    -------
    data_taux: pd.DataFrame
        A DataFrame containing the data for the taux de validation.
    data_ver: pd.DataFrame
        A DataFrame containing the data for the verification information.
    """
    data_taux = data[["ou_id", "period", "service", "Taux of validation"]].copy()
    data_ver = data[["ou_id", "period", "bool verified"]].copy().drop_duplicates()
    data_ver["bool verified"] = data_ver["bool verified"].astype(int).astype(str)
    # We have {"Verified": 0, "Non Verified": 1}
    return data_taux, data_ver


def get_full_list_of_ous():
    """Get the full list of organisational units in the program from the rows that have
    verified and validated data.

    Returns
    -------
    list
        The list of organisational units that should have verification and validation data in DHIS2.
    """
    quant_file = pd.read_csv(f"{workspace.files_path}/{config.path_quantity_data}")
    ous_quant = quant_file["ou"].unique().tolist()
    return ous_quant


def add_other_ous_to_data(data, all_ous):
    """Add the missing ous as non-verified centers.

    Returns
    --------
    pd.DataFrame
        The original data with the missing ous added as non-verified centers.
    """
    present_ous = data["ou_id"].unique().tolist()
    missing_ous = list(set(all_ous) - set(present_ous))
    current_run.log_info(
        f"Adding {len(missing_ous)} missing ous to the data as non-verified centers."
    )
    missing_ous_df = pd.DataFrame(missing_ous, columns=["ou_id"])
    missing_ous_df["bool verified"] = "0"
    missing_ous_df["period"] = data["period"].unique()[0]
    full_data = pd.concat([data, missing_ous_df], axis=0)
    return full_data


def add_gasc_children(data, org_units):
    """For all the organisation units in the data, addd the verification information for the children GASC.

    Returns
    --------
    pd.DataFrame
        The original data with the GASC children data added.
    """
    current_run.log_info("Adding GASC children to the data")
    gasc_ous = org_units[org_units["name"].str.contains(config.mark_gasc)]
    data_gasc = data.merge(gasc_ous, how="inner", left_on="ou_id", right_on="level_5_id")
    data_gasc = (
        data_gasc[
            [
                "period",
                "level_6_id",
                "bool verified",
            ]
        ]
        .rename(columns={"level_6_id": "ou_id"})
        .drop_duplicates()
    )
    current_run.log_info(f"Added {len(data_gasc)} rows for GASC children")
    full_data = pd.concat([data, data_gasc], axis=0)
    return full_data


def get_period_list(period):
    """
    From a period in the format YYYYQX / YYYYMM, get a list of periods in the format YYYYMM.
    (if the format is already YYYYMM we just put it in a list and return it)

    Parameters
    ----------
    period: str
        A period in the format YYYYQX / YYYYMM

    Returns
    -------
    list
        List of periods in the format YYYYMM.
    """
    list_periods = []
    if "Q" in str(period):
        year = period[:4]
        quarter = int(period[5])
        for month in range(quarter * 3 - 2, quarter * 3 + 1):
            list_periods.append(f"{year}{month:02d}")
            current_run.log_info(f"List of periods: {list_periods}")
    else:
        list_periods.append(period)

    return list_periods


def get_data_mod():
    """
    Get the verification data for the specified periods from the workspace.

    Parameters
    ----------
    file: File
        The file with the data

    Returns
    -------
    output_path: str
        The place where the output data will be saved
    data: pd.DataFrame
        The data to push to DHIS2

    """
    file_name = "model___ext_2005-prov___national-prd___202603-service.csv"
    pipeline_folder = "run_vbr"
    file_path = "/home/leyregarrido/01_github_repos/VBR-template/country_pipelines/burundi/push_vbr/model___ext_2005-prov___national-prd___202603-service.csv"
    target_folder = "temp"
    pattern = r"model___.+-prov___.+-prd___.+-service\.csv$"
    if not re.match(pattern, file_name):
        current_run.log_error(
            f"The file name {file_name} does not match the expected pattern {pattern}"
            "I will not use it."
        )
        raise ValueError(f"The file name {file_name} does not match the expected pattern {pattern}")

    if pipeline_folder != "run_vbr":
        current_run.log_error(
            f"The file {file_name} is not in the expected folder 'run_vbr'. It is in {pipeline_folder}. I will not use it."
        )
        raise ValueError(
            f"The file {file_name} is not in the expected folder 'run_vbr'. It is in {pipeline_folder}"
        )

    df = pd.read_csv(file_path)

    output_path = f"{workspace.files_path}/pipelines/push_vbr/data_to_push/{target_folder}"
    os.makedirs(output_path, exist_ok=True)

    df.to_csv(f"{output_path}/{file_name}", index=False)
    current_run.log_info(f"Output path: {output_path}")
    dt = datetime.now().strftime("%Y_%m_%d_%H%M")

    return output_path, df, dt


def get_data(file: File):
    """
    Get the verification data for the specified periods from the workspace.

    Parameters
    ----------
    file: File
        The file with the data

    Returns
    -------
    output_path: str
        The place where the output data will be saved
    data: pd.DataFrame
        The data to push to DHIS2

    """
    file_path = Path(file.path)
    file_name = file.name
    current_run.log_info(f"Getting data for file: {file}")

    target_folder = file_path.parents[2].name
    pipeline_folder = file_path.parents[3].name

    pattern = r"model___.+-prov___.+-prd___.+-service\.csv$"
    if not re.match(pattern, file_name):
        current_run.log_error(
            f"The file name {file_name} does not match the expected pattern {pattern}"
            "I will not use it."
        )
        raise ValueError(f"The file name {file_name} does not match the expected pattern {pattern}")

    if pipeline_folder != "run_vbr":
        current_run.log_error(
            f"The file {file_name} is not in the expected folder 'run_vbr'. It is in {pipeline_folder}. I will not use it."
        )
        raise ValueError(
            f"The file {file_name} is not in the expected folder 'run_vbr'. It is in {pipeline_folder}"
        )

    df = pd.read_csv(file_path)

    output_path = f"{workspace.files_path}/pipelines/push_vbr/data_to_push/{target_folder}"
    os.makedirs(output_path, exist_ok=True)

    df.to_csv(f"{output_path}/{file_name}", index=False)
    current_run.log_info(f"Output path: {output_path}")
    dt = datetime.now().strftime("%Y_%m_%d_%H%M")

    return output_path, df, dt


def get_dhis2(con_oh):
    """Start the connection to the DHIS2 instance.

    Parameters
    ----------
    con_OH:
        Connection to the DHIS2 instance.

    Returns
    -------
    DHIS2 instance
        The DHIS2 instance.
    """
    return DHIS2(con_oh)


def get_service_codes():
    """
    Get the service codes from the configuration file.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the service codes and their corresponding names.
    """
    base_path = f"{workspace.files_path}/pipelines/push_vbr/config/"
    json_file_name = base_path + "taux_validation_dx.json"
    with open(json_file_name, "r") as json_file:
        codes_services_dict = json.load(json_file)
    codes_services = pd.DataFrame(
        list(codes_services_dict.items()), columns=["service", "service_code"]
    )
    return codes_services


def merge_service_codes(data, services):
    """
    Merge the service codes with the data.

    Parameters
    ----------
    data: pd.DataFrame
        The data to merge with the service codes.
    services: pd.DataFrame
        The service codes to merge with the data.

    Returns
    -------
    pd.DataFrame
        The merged DataFrame containing the service codes.
    """
    data = data.merge(services, how="left", on="service")
    missing_services = data[data["service_code"].isna()]["service"].unique()
    if len(missing_services) > 0:
        current_run.log_warning(
            f"The following services are not in the service codes: {missing_services}. I will not push them."
        )
        data = data[~data["service_code"].isna()]
    else:
        current_run.log_info("All services are in the service codes.")
    return data


def check_data_taux(data):
    """
    Check if the data is valid and ready to be pushed to DHIS2.

    Parameters
    ----------
    data: pd.DataFrame
        The data to check.
    """
    ser_nan_taux = data["Taux of validation"].isna()
    if ser_nan_taux.any():
        current_run.log_warning(
            f"You have rows with NaN in the Taux of validation column: {data[ser_nan_taux]}"
        )


def check_data_full(data):
    """
    Check if the data is valid and ready to be pushed to DHIS2.

    Parameters
    ----------
    data: pd.DataFrame
        The data to check.
    """
    inconsistent_ous = data.groupby(["ou_id", "period"])["bool verified"].nunique()
    inconsistent_ous = inconsistent_ous[inconsistent_ous > 1]
    if inconsistent_ous.any():
        current_run.log_warning(f"There are incosistent ous: {inconsistent_ous.index.tolist()}")

    ser_org_unit_id_nan = data["ou_id"].isna()
    if ser_org_unit_id_nan.any():
        current_run.log_warning(
            f"You have rows with NaN in the ou_id column: {data[ser_org_unit_id_nan]}"
        )

    duplicate_rows = data[data.duplicated(subset=["ou_id", "service", "period"], keep=False)]
    if not duplicate_rows.empty:
        current_run.log_warning(f"You have duplicate rows: {duplicate_rows}")


def prepare_taux_for_dhis(data, output_path, dry_run, dt):
    """
    Prepare the taux data for DHIS2.

    Parameters
    ----------
    data: pd.DataFrame
        The data to prepare.
    output_path: str
        The base output path
    dry_run: bool
        If True, we will not actually push the data to DHIS2.
    dt: str
        Current date and time string for file naming.

    Returns
    -------
    list
        A list of dictionaries containing the data to post to DHIS2.
    """
    values_to_post_taux = []
    periods_to_post = data["period"].unique()
    dict_periods = {period: get_period_list(period) for period in periods_to_post}

    for index, row in data.iterrows():
        dataElement = row["service_code"]
        orgUnit = row["ou_id"]
        value = str(row["Taux of validation"])
        for period in dict_periods[row["period"]]:
            values_to_post_taux.append(
                {
                    "dataElement": dataElement,
                    "period": str(period),
                    "orgUnit": orgUnit,
                    "categoryOptionCombo": config.categoryOptionCombo,
                    "value": value,
                }
            )

    output_folder = f"{output_path}/pushed_data"
    os.makedirs(output_folder, exist_ok=True)

    df_taux = pd.DataFrame(values_to_post_taux)
    df_taux.to_csv(f"{output_folder}/taux_data_dry_run_{dry_run}_{dt}.csv", index=False)

    return values_to_post_taux


def prepare_ver_for_dhis(data, output_path, dry_run, dt):
    """
    Prepare the center verification data for DHIS2.

    Parameters
    ----------
    data: pd.DataFrame
        The data to prepare.
    output_path: str
        The base output path
    dry_run: bool
        If True, we will not actually push the data to DHIS2.
    dt: str
        Current date and time string for file naming.

    Returns
    -------
    list
        A list of dictionaries containing the data to post to DHIS2.
    """
    values_to_post_ver = []
    periods_to_post = data["period"].unique()
    dict_periods = {period: get_period_list(period) for period in periods_to_post}
    for index, row in data.iterrows():
        orgUnit = row["ou_id"]
        value = str(row["bool verified"])
        for period in dict_periods[row["period"]]:
            values_to_post_ver.append(
                {
                    "dataElement": config.ver_code,
                    "period": str(period),
                    "orgUnit": orgUnit,
                    "categoryOptionCombo": config.categoryOptionCombo,
                    "value": value,
                }
            )

    output_folder = f"{output_path}/pushed_data"
    os.makedirs(output_folder, exist_ok=True)

    df_ver = pd.DataFrame(values_to_post_ver)
    df_ver.to_csv(f"{output_folder}/ver_data_dry_run_{dry_run}_{dt}.csv", index=False)

    return values_to_post_ver


def flip_binary(value):
    if value == 1:
        return 0
    elif value == 0:
        return 1
    else:
        return value


def push_to_dhis2(
    dhis,
    data_taux,
    data_ver,
    dry_run_taux,
    dry_run_ver,
    output_path,
    import_strategy="CREATE_AND_UPDATE",
    max_post=1000,
    dt=None,
):
    """
    Push the data to DHIS2.
    """
    current_run.log_info(
        f"Pushing verification data len: {len(data_ver)} to DHIS2. Dry-run: {dry_run_ver}"
    )
    summary_ver = push_data_elements(
        dhis2_client=dhis,
        data_elements_list=data_ver,
        strategy=import_strategy,
        dry_run=dry_run_ver,
        max_post=max_post,
    )
    msg = f"Analytics extracts summary for verification: {summary_ver['import_counts']}"
    current_run.log_info(msg)

    current_run.log_info(
        f"Pushing taux data len: {len(data_taux)} to DHIS2.Dry-run: {dry_run_taux}"
    )
    summary_taux = push_data_elements(
        dhis2_client=dhis,
        data_elements_list=data_taux,
        strategy=import_strategy,
        dry_run=dry_run_taux,
        max_post=max_post,
    )
    msg = f"Analytics extracts summary for taux: {summary_taux['import_counts']}"
    current_run.log_info(msg)

    summary = pd.DataFrame(
        [
            {"type": "summary_ver", "dry_run": dry_run_ver, **summary_ver.get("import_counts", {})},
            {
                "type": "summary_taux",
                "dry_run": dry_run_taux,
                **summary_taux.get("import_counts", {}),
            },
        ]
    )
    folder = f"{output_path}/pushed_data"
    os.makedirs(folder, exist_ok=True)
    summary.to_csv(f"{folder}/summary_push_{dt}.csv", index=False)


def push_data_elements(
    dhis2_client: DHIS2,
    data_elements_list: list,
    dry_run: bool,
    strategy: str = "CREATE_AND_UPDATE",
    max_post: int = 1000,
) -> dict:
    """dry_run: Set to true to get an import summary without actually importing data (DHIS2).

    Returns
    -------
        dict: A summary dictionary containing import counts and errors.
    """
    # max_post instead of MAX_POST_DATA_VALUES
    summary = {
        "import_counts": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
        "import_options": {},
        "ERRORS": [],
    }

    total_datapoints = len(data_elements_list)
    count = 0

    for chunk in config.split_list(data_elements_list, max_post):
        count = count + 1
        try:
            r = dhis2_client.api.session.post(
                f"{dhis2_client.api.url}/dataValueSets",
                json={"dataValues": chunk},
                params={
                    "dryRun": dry_run,
                    "importStrategy": strategy,
                    "preheatCache": True,
                    "skipAudit": True,
                    "strictPeriods": True,
                    "strictDataElements": True,
                    "strictCategoryOptionCombos": True,
                    "reportMode": "ERRORS",
                },  # speed!
            )
            r.raise_for_status()

            try:
                response_json = r.json()
                status = response_json.get("status")
                response = response_json.get("response").get("importCount")
            except json.JSONDecodeError as e:
                summary["ERRORS"].append(
                    f"Response JSON decoding failed: {e}"
                )  # period: {chunk_period}")
                response_json = None
                status = None
                response = None

            if status not in ("SUCCESS", "OK") and response:
                summary["ERRORS"].append(response)

            if response:
                for key in ["imported", "updated", "ignored", "deleted"]:
                    summary["import_counts"][key] += response.get(key, 0)

        except requests.exceptions.RequestException as e:
            try:
                response = r.json().get("response")
            except (ValueError, AttributeError):
                response = None

            if response:
                for key in ["imported", "updated", "ignored", "deleted"]:
                    summary["import_counts"][key] += response["importCount"][key]

            error_response = config.get_response_value_errors(response, chunk=chunk)
            summary["ERRORS"].append({"error": e, "response": error_response})

        if (count * max_post) % 10000 == 0:
            current_run.log_info(
                f"{count * max_post} / {total_datapoints} data points pushed summary: {summary['import_counts']}"
            )

    return summary


if __name__ == "__main__":
    push_vbr()
