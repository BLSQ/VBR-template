from openhexa.sdk import (
    current_run,
    pipeline,
    workspace,
    parameter,
    DHIS2Connection,
)
from openhexa.toolbox.dhis2 import DHIS2
import pandas as pd
import regex as re
import os
import json
import requests

import config


@pipeline("rdc-push-ver", name="rdc_push_ver", timeout=20000)
@parameter(
    "dhis_con",
    type=DHIS2Connection,
    help="Connection to DHIS2",
    default="pbf-pmns-rdc",
    required=True,
)
@parameter("folder", name="Folder", type=str, default="data_to_push")
@parameter(
    "periods",
    name="Periods",
    help="Periods to push to DHIS2",
    type=str,
    default=["2025Q1", "2025Q2"],
    multiple=True,
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
    name="Dry Run Verificaiton",
    type=bool,
    default=True,
    help="If False, we will actually push the verification data to DHIS2",
)
def rdc_push_ver(dhis_con, folder, periods, dry_run_taux, dry_run_ver):
    """
    Pipeline to push the taux de validation and center validation to DHIS2.
    """
    data = get_data(folder, periods)
    dhis = get_dhis2(dhis_con)
    services = get_service_codes()
    data_with_services = merge_service_codes(data, services)
    done = check_data(data_with_services)
    data_taux = prepare_taux_for_dhis(data_with_services, done)
    data_ver = prepare_ver_for_dhis(data_with_services, done)
    summary_taux = push_to_dhis2(dhis, data_taux, data_ver, dry_run_taux, dry_run_ver)


def get_period_list(period):
    """
    From a period in the format YYYYQX, get a list of periods in the format YYYYMM.

    Parameters
    ----------
    period: str
        A period in the format YYYYQX

    Returns
    -------
    list
        List of periods in the format YYYYMM.
    """
    list_periods = []

    year = period[:4]
    quarter = int(period[5])
    for month in range(quarter * 3 - 2, quarter * 3 + 1):
        list_periods.append(f"{year}{month:02d}")

    current_run.log_info(f"List of periods: {list_periods}")
    return list_periods


@rdc_push_ver.task
def get_data(folder, periods):
    """
    Get the verification data for the specified periods from the workspace.

    Parameters
    ----------
    folder: str
        The folder where the data is stored.
    periods: list
        The periods for which to retrieve the data.

    Returns
    -------
    pd.DataFrame or None
        A DataFrame containing the verification data for the specified periods,
        or None if no data is found.

    """
    current_run.log_info(f"Getting data for periods: {periods} and the folder: {folder}")
    files_ok = []
    files_not_ok = []
    base_path = f"{workspace.files_path}/pipelines/push_vbr/{folder}/"

    for filename in os.listdir(base_path):
        for period in periods:
            pattern = rf"prov___(.+)-prd___{period}-model__.+\.csv$"
            match = re.search(pattern, filename)
            if match:
                province = match.group(1)
                file_path = os.path.join(base_path, filename)
                df = pd.read_csv(file_path)
                df["province"] = province
                files_ok.append(df)
                break
        else:
            files_not_ok.append(filename)

    if len(files_not_ok) > 0:
        current_run.log_warning(
            f"There are files in the folder {folder} not matching the pattern: {files_not_ok}"
        )

    if len(files_ok) > 0:
        df = pd.concat(files_ok, ignore_index=True)
        current_run.log_info("Data has been successfully retrieved.")
        current_run.log_info(f"Number of rows: {len(df)}")
        return df
    else:
        current_run.log_error("No data found for the specified periods.")
        return None


@rdc_push_ver.task
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


@rdc_push_ver.task
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


@rdc_push_ver.task
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


@rdc_push_ver.task
def check_data(data):
    """
    Check if the data is valid and ready to be pushed to DHIS2.

    Parameters
    ----------
    data: pd.DataFrame
        The data to check.

    Returns
    -------
    bool
        True if the data is valid, False otherwise.
    """
    inconsistent_ous = data.groupby("ou_id")["bool verified"].nunique()
    inconsistent_ous = inconsistent_ous[inconsistent_ous > 1]
    if inconsistent_ous.any():
        current_run.log_warning(f"There are incosistent ous: {inconsistent_ous.index.tolist()}")

    ser_org_unit_id_nan = data["ou_id"].isna()
    if ser_org_unit_id_nan.any():
        current_run.log_warning(
            f"You have rows with NaN in the ou_id column: {data[ser_org_unit_id_nan]}"
        )

    ser_nan_taux = data["Taux of validation"].isna()
    if ser_nan_taux.any():
        current_run.log_warning(
            f"You have rows with NaN in the Taux of validation column: {data[ser_nan_taux]}"
        )

    duplicate_rows = data[data.duplicated(subset=["ou_id", "service_code", "period"], keep=False)]
    if not duplicate_rows.empty:
        current_run.log_warning(f"You have duplicate rows: {duplicate_rows}")

    return True


@rdc_push_ver.task
def prepare_taux_for_dhis(data, done):
    """
    Prepare the taux data for DHIS2.

    Parameters
    ----------
    data: pd.DataFrame
        The data to prepare.
    done: bool
        Used to stop this task from starting before we have checked the data

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

    return values_to_post_taux


@rdc_push_ver.task
def prepare_ver_for_dhis(data, done):
    """
    Prepare the center verification data for DHIS2.

    Parameters
    ----------
    data: pd.DataFrame
        The data to prepare.
    done: bool
        Used to stop this task from starting before we have checked the data

    Returns
    -------
    list
        A list of dictionaries containing the data to post to DHIS2.
    """
    data["bool verified"] = data["bool verified"].astype(int).astype(str)
    # We have {"Verified": 0, "Non Verified": 1}
    values_to_post_ver = []
    periods_to_post = data["period"].unique()
    dict_periods = {period: get_period_list(period) for period in periods_to_post}

    data_ver = data[["ou_id", "period", "ver_to_post"]].copy().drop_duplicates()

    for index, row in data_ver.iterrows():
        orgUnit = row["ou_id"]
        value = str(row["ver_to_post"])
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

    return values_to_post_ver


def flip_binary(value):
    if value == 1:
        return 0
    elif value == 0:
        return 1
    else:
        return value


@rdc_push_ver.task
def push_to_dhis2(
    dhis,
    data_taux,
    data_ver,
    dry_run_taux,
    dry_run_ver,
    import_strategy="CREATE_AND_UPDATE",
    max_post=1000,
):
    """
    Push the data to DHIS2.
    """
    current_run.log_info(
        f"Pushing verification data len: {len(data_ver)} to DHIS2. Dry-run: {dry_run_ver}"
    )
    summary = push_data_elements(
        dhis2_client=dhis,
        data_elements_list=data_ver,
        strategy=import_strategy,
        dry_run=dry_run_ver,
        max_post=max_post,
    )
    msg = f"Analytics extracts summary for verification: {summary['import_counts']}"
    current_run.log_info(msg)

    current_run.log_info(
        f"Pushing taux data len: {len(data_taux)} to DHIS2.Dry-run: {dry_run_taux}"
    )
    summary = push_data_elements(
        dhis2_client=dhis,
        data_elements_list=data_taux,
        strategy=import_strategy,
        dry_run=dry_run_taux,
        max_post=max_post,
    )
    msg = f"Analytics extracts summary for taux: {summary['import_counts']}"
    current_run.log_info(msg)


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
                },  # speed!
            )
            r.raise_for_status()

            try:
                response_json = r.json()
                status = response_json.get("status")
                response = response_json.get("importCount")
            except json.JSONDecodeError as e:
                summary["ERRORS"].append(
                    f"Response JSON decoding failed: {e}"
                )  # period: {chunk_period}")
                response_json = None
                status = None
                response = None

            if status != "SUCCESS" and response:
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
    rdc_push_ver()
