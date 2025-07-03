"""Template for newly generated pipelines."""

import json
import os
import pickle
import warnings
from pandas.errors import SettingWithCopyWarning

import pandas as pd
import requests
from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2

from RBV_package import data_extraction, dates
from RBV_package import rbv_environment as rbv

import config

warnings.filterwarnings("ignore", category=SettingWithCopyWarning)


@pipeline("buu-init-vbr", name="BUU_init_VBR")
@parameter(
    "dhis_con",
    type=DHIS2Connection,
    help="Connection to DHIS2",
    default="pbf-pmns-rdc",
    required=True,
)
@parameter("hesabu_con", type=str, help="Connection to hesabu", default="hesabu", required=True)
@parameter(
    "program_id",
    type=str,
    help="Program ID for contracts",
    default="iGORRTJbNOk",
    required=True,
)
@parameter(
    "period",
    name="period",
    type=str,
    help="Period for the verification (either yyyymm eg 202406 or yyyyQt eg 2024Q2)",
    required=True,
    default="202506",
)
@parameter(
    "model_name",
    name="Name of the model",
    type=str,
    default="model",
    required=True,
)
@parameter(
    "window",
    type=int,
    help="Number of months to consider",
    required=True,
    default=12,
)
@parameter("selection_provinces", type=bool, default=True)
@parameter("extract", name="Extraire les données", type=bool, default=True)
@parameter(
    "extract_verification",
    name="Extract is_verified",
    help="Extract the verification information from DHIS2",
    type=bool,
    default=False,
)
def buu_init_vbr(
    dhis_con,
    hesabu_con,
    program_id,
    period,
    extract,
    model_name,
    window,
    selection_provinces,
    extract_verification,
):
    """Pipeline to extract the necessary data.

    We use Hesabu and DHIS2 to obtain information about different health centers.
    We obtain both quantity and quality data, and we store them in the files quantity_data.csv and quality_data.csv.
    """
    dhis = get_dhis2(dhis_con)
    hesabu = get_hesabu(hesabu_con)
    hesabu_params = get_hesabu_vbr_setup()
    packages_hesabu = get_hesabu_package_ids(hesabu_params)
    contract_group_id = get_contract_group_unit_id(hesabu_params)
    packages = fetch_hesabu_package(hesabu, packages_hesabu)
    periods = get_periods(period, window)
    contracts = fetch_contracts(dhis, program_id, model_name)
    done_packages = get_package_values(dhis, periods, packages, contract_group_id, extract)
    quant = prepare_quantity_data(
        done_packages,
        periods,
        packages,
        contracts,
        hesabu_params,
        extract,
        model_name,
        dhis,
        extract_verification,
    )
    qual = prepare_quality_data(
        done_packages, periods, packages, hesabu_params, extract, model_name
    )
    save_simulation_environment(quant, qual, hesabu_params, model_name, selection_provinces)


@buu_init_vbr.task
def prepare_quantity_data(
    done,
    periods,
    packages,
    contracts,
    hesabu_params,
    extract,
    model_name,
    dhis,
    extract_verification,
):
    """Create a CSV file with the quantity data.
    (1) We combine all of the data from the packages cvs's that have quantity data.
    (2) We merge it with the data in the contracts.csv file.
    (3) We select the columns that we are interested in.
    (4) We perform some calculations on the data, to have all of the information we will need.
    (5) We save the data in a CSV file.

    Parameters
    ----------
    done: bool
        Indicates that the process get_package_values has finished.
    periods: list
        The periods to be considered.
    packages: dict
        A dictionary containing the codes/names/etc that we are going to want to extract from DHIS2.
    contracts: pd.DataFrame
        A DataFrame containing the information about the data elements we want to extract.
    hesabu_params: dict
        Contains the information we want to extract from Hesabu.
    extract: bool
        If True, extract the data. If False, we don't do anything. (We assume that the pertinent data was already extracted).
    model_name: str
        The name of the model. We will use it for the name of the csv file.

    Returns
    -------
    data: pd.DataFrame
        A DataFrame containing the quantity data.
    """
    if extract:
        data = pd.DataFrame()
        for id in packages:
            if hesabu_params["packages"][str(id)] == "quantite" and os.path.exists(
                f"{workspace.files_path}/pipelines/initialize_vbr/packages/{id}"
            ):
                for p in periods:
                    if os.path.exists(
                        f"{workspace.files_path}/pipelines/initialize_vbr/packages/{id}/{p}.csv"
                    ):
                        df = pd.read_csv(
                            f"{workspace.files_path}/pipelines/initialize_vbr/packages/{id}/{p}.csv"
                        )
                        data = pd.concat([data, df], ignore_index=True)
                        # data contains the actual data for the package identified with id.
                        # It has the data for all of the periods.

        # We are now dropping the rows where the dec = 0 or NaN.
        data = data[data["declaree"].notna() & (data["declaree"] != 0)]
        data = data.merge(contracts, on="org_unit_id")
        data = data[
            list(hesabu_params["quantite_attributes"].keys())
            + list(hesabu_params["contracts_attributes"].keys())
        ]
        data.rename(
            columns=hesabu_params["contracts_attributes"],
            inplace=True,
        )
        data.rename(
            columns=hesabu_params["quantite_attributes"],
            inplace=True,
        )
        data.contract_end_date = data.contract_end_date.astype(int)
        data = data[(data.contract_end_date >= data.month) & (~data.type_ou.isna())]

        if extract_verification:
            verification_list = []
            list_periods = data["month"].unique()
            for period in list_periods:
                verification_period = get_verification_information(dhis, data, period)
                verification_list.append(verification_period)

            if len(verification_list) > 0:
                verification = pd.concat(verification_list, ignore_index=True)
                data = add_verification_information(data, verification)

        data["gain_verif"] = (data["dec"] - data["val"]) * data["tarif"]
        data["subside_sans_verification"] = data["dec"] * data["tarif"]
        data["subside_avec_verification"] = data["val"] * data["tarif"]
        data["quarter"] = data["month"].map(dates.month_to_quarter)
        data = rbv.calcul_ecarts(data)

        if extract_verification:
            mark_strange_verification(data, model_name)
            data = format_data_for_verified_centers(data)

        data.to_csv(
            f"{workspace.files_path}/pipelines/initialize_vbr/data/quantity_data/quantity_data_{model_name}.csv",
            index=False,
        )
    else:
        data = pd.read_csv(
            f"{workspace.files_path}/pipelines/initialize_vbr/data/quantity_data/quantity_data_{model_name}.csv"
        )
    return data


def mark_strange_verification(data, model):
    """
    Save in a CSV weird verification values.
    (1) The center was not verified, but there are verified or validated values.
    (2) The verification fields holds something that is not a 0 or a 1.
    (3) There are centers that for the same period have different verification values.

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame containing the quantity data.
    model: str
        The name of the model. We will use it for the name of the csv file.
    """
    weird_data = []
    data_ver_val_not_verified = data[
        (data["is_verified"] == 1) & ((data["ver"] != 0) | (data["val"] != 0))
    ]
    data_incorrect_is_verified_value = data[~data["is_verified"].isin([0, 1])]
    verification_conflicts = data.groupby(["ou", "month"])["is_verified"].nunique().reset_index()
    conflicting_groups = verification_conflicts[verification_conflicts["is_verified"] > 1]

    if data_ver_val_not_verified.shape[0] > 0:
        current_run.log_info(
            f"There are services that have not been verified but that have verified or validated values: {data_ver_val_not_verified.shape[0]} rows"
        )
        weird_data.append(data_ver_val_not_verified)
    if data_incorrect_is_verified_value.shape[0] > 0:
        current_run.log_info(
            f"There are services that have an incorrect value for is_verified: {data_incorrect_is_verified_value.shape[0]} rows"
        )
        weird_data.append(data_incorrect_is_verified_value)
    if conflicting_groups.shape[0] > 0:
        current_run.log_info(
            f"There are centers that have 0 and 1 verification for the same period: {conflicting_groups.shape[0]} rows"
        )
        weird_data.append(conflicting_groups)

    if len(weird_data) > 0:
        weird_data_df = pd.concat(weird_data, ignore_index=True)
        weird_data_df.to_csv(
            f"{workspace.files_path}/pipelines/initialize_vbr/data/quantity_data/strange_verif_{model}.csv",
            index=False,
        )
        current_run.log_info(
            f"Weird verification data saved in {workspace.files_path}/pipelines/initialize_vbr/data/weird_verification.csv"
        )


def get_verification_information(dhis, data, period):
    """
    Extract the is_verified bool from DHIS2.

    Parameters
    ----------
    dhis: DHIS2
        The DHIS2 instance.
    data: pd.DataFrame
        The DataFrame containing the quantity data.
    period: str
        The relevant period.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the information about the verification of centers.
        If no information is found, an empty DataFrame is returned.
    """
    list_ous = list(data[data["month"] == period]["ou"].unique())
    period = [str(period)]
    response = dhis.data_value_sets.get(
        datasets=config.ds_is_verified,
        periods=period,
        org_units=list_ous,
    )
    if len(response) == 0:
        current_run.log_info(f"No information about the center verification for {period}")
        return pd.DataFrame()
    else:
        response = pd.DataFrame(response)
        reponse_selected = response[
            (response["categoryOptionCombo"] == config.coc_is_verified)
            & (response["dataElement"] == config.de_is_verified)
        ]
        if len(reponse_selected) == 0:
            current_run.log_info(
                f"No information about the center verification for {period} and {config.coc_is_verified}"
            )
            return pd.DataFrame()
        else:
            current_run.log_info(f"Information about the center verification for {period} found")
            return reponse_selected


def add_verification_information(data, verification):
    """
    Add the information about the verification of centers to the data DataFrame.

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame containing the quantity data.
    verification: pd.DataFrame
        The DataFrame containing the information about the verification of centers.
    period: str
        The relevant period.

    Returns
    -------
    data: pd.DataFrame
        The DataFrame containing the quantity data with the verification information added.
    """
    verification["period"] = verification["period"].astype(int)
    data = data.merge(
        verification[["orgUnit", "period", "value"]],
        left_on=["ou", "month"],
        right_on=["orgUnit", "period"],
        how="left",
    )
    data = data.drop(columns=["orgUnit", "period"])
    data = data.rename(columns={"value": "is_verified"})
    data["is_verified"] = data["is_verified"].fillna(0)
    data["is_verified"] = data["is_verified"].astype(int)
    # 0 means that the center was verified.
    return data


def format_data_for_verified_centers(data):
    """
    For verified centers, we don't want to take into account the data.

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame containing the quantity data.

    Returns
    -------
    data: pd.DataFrame
        The DataFrame containing the quantity data with the verification information formatted.
    """
    ser_verified_centers = data["is_verified"] == 1
    for col in config.null_indicators_verified_centers:
        data.loc[ser_verified_centers, col] = None
    return data


@buu_init_vbr.task
def prepare_quality_data(done, periods, packages, hesabu_params, extract, model_name):
    """Create a CSV file with the quality data.
    (1) We combine all of the data from the packages cvs's that have quality data.
    (2) We select the columns that we are interested in.
    (3) We perform some calculations on the data, to have all of the information we will need.
    (4) We save the data in a CSV file.

    Parameters
    ----------
    done: bool
        Indicates that the process get_package_values has finished.
    periods: list
        The periods to be considered.
    packages: dict
        A dictionary containing the codes/names/etc that we are going to want to extract from DHIS2.
    hesabu_params: dict
        Contains the information we want to extract from Hesabu.
    extract: bool
        If True, extract the data. If False, we don't do anything. (We assume that the pertinent data was already extracted).
        model_name: str
        The name of the model. We will use it for the name of the csv file.

    Returns
    -------
    data: pd.DataFrame
        A DataFrame containing the quality data.
    """
    if extract:
        data = pd.DataFrame()
        for id in packages:
            if hesabu_params["packages"][str(id)] == "qualite" and os.path.exists(
                f"{workspace.files_path}/pipelines/initialize_vbr/packages/{id}"
            ):
                for p, q in [(p, dates.month_to_quarter(str(p))) for p in periods]:
                    if os.path.exists(
                        f"{workspace.files_path}/pipelines/initialize_vbr/packages/{id}/{q}.csv"
                    ):
                        df = pd.read_csv(
                            f"{workspace.files_path}/pipelines/initialize_vbr/packages/{id}/{q}.csv"
                        )
                        df["month"] = str(p)
                        data = pd.concat([data, df], ignore_index=True)
        data = data[list(hesabu_params["qualite_attributes"].keys()) + ["month"]]
        data.rename(
            columns=hesabu_params["qualite_attributes"],
            inplace=True,
        )
        data["score"] = data["num"] / data["denom"]
        data["month_final"] = data["quarter"].map(dates.quarter_to_months)
        data.to_csv(
            f"{workspace.files_path}/pipelines/initialize_vbr/data/quality_data/quality_data_{model_name}.csv",
            index=False,
        )
    else:
        data = pd.read_csv(
            f"{workspace.files_path}/pipelines/initialize_vbr/data/quality_data/quality_data_{model_name}.csv"
        )
    return data


@buu_init_vbr.task
def save_simulation_environment(quant, qual, hesabu_params, model_name, selection_provinces):
    """We save the simulation in a pickle file. We will then access this pickle file in the second pipeline.

    Parameters
    ----------
    quant: pd.DataFrame
        The quantity data.
    qual: pd.DataFrame
        The quality data.
    hesabu_params: dict
        Contains the information we want to extract from Hesabu.
    model_name: str
        The name of the model. We will use it for the name of the pickle file.
    selection_provinces: bool
        If True, we will select the provinces. This was inputed by the user.

    Returns
    -------
    list:
        A list of regions.
    """
    regions = []
    orgunits = quant["ou"].unique()
    quality_indicators = sorted(qual["indicator"].unique())
    if (
        selection_provinces
        and "selection_provinces" in hesabu_params
        and len(hesabu_params["selection_provinces"]) > 0
    ):
        for province in hesabu_params["selection_provinces"]:
            group_of_ou = rbv.GroupOrgUnits(
                province,
                quality_indicators,
            )
            nb_tot = 0
            for ou in orgunits:
                temp = quant[(quant.ou == ou) & (quant.level_2_name == province)]
                # temp has the quantitative information for the province and organizational unit we are interested in.
                if len(temp) > 0:
                    group_of_ou.add_ou(
                        rbv.Orgunit(
                            ou,
                            temp,
                            qual[(qual.ou == ou)],
                            quality_indicators,
                            temp.type_ou.values[0]
                            in hesabu_params["type_ou_verification_systematique"],
                        )
                    )
                    nb_tot += 1

            current_run.log_info(f"Province= {province}. Number of orgunits= {nb_tot}")
            regions.append(group_of_ou)

    else:
        group_of_ou = rbv.GroupOrgUnits(
            "national",
            quality_indicators,
        )
        nb_tot = 0
        for ou in orgunits:
            temp = quant[(quant.ou == ou)]
            # temp has the quantitative information for the province and organizational unit we are interested in.
            if len(temp) > 0:
                group_of_ou.add_ou(
                    rbv.Orgunit(
                        ou,
                        temp,
                        qual[(qual.ou == ou)],
                        quality_indicators,
                        temp.type_ou.values[0]
                        in hesabu_params["type_ou_verification_systematique"],
                    )
                )
                nb_tot += 1
        current_run.log_info(f"number of orgunits= {nb_tot}")
        regions.append(group_of_ou)

    if not os.path.exists(
        f"{workspace.files_path}/pipelines/initialize_vbr/initialization_simulation"
    ):
        os.makedirs(f"{workspace.files_path}/pipelines/initialize_vbr/initialization_simulation")
    with open(
        f"{workspace.files_path}/pipelines/initialize_vbr/initialization_simulation/{model_name}.pickle",
        "wb",
    ) as file:
        # Serialize and save the object to the file
        pickle.dump(regions, file)
    current_run.log_info(
        f"Fichier d'initialisation sauvé: {workspace.files_path}/pipelines/initialize_vbr/initialization_simulation/{model_name}.pickle"
    )
    return regions


@buu_init_vbr.task
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


@buu_init_vbr.task
def get_hesabu_vbr_setup():
    """Open the JSON file with the Hesabu setup.

    Returns
    -------
    Dict:
        Contains the information we want to extract from Hesabu.
    """
    return json.load(
        open(f"{workspace.files_path}/pipelines/initialize_vbr/hesabu/hesabu_vbr_setup.json")
    )


@buu_init_vbr.task
def get_hesabu_package_ids(hesabu_setup):
    """Get the package IDs from the Hesabu setup.

    Parameters
    ----------
    hesabu_setup: dict
        The Hesabu setup.

    Returns
    -------
    list:
        The package IDs.
    """
    return [int(id) for id in hesabu_setup["packages"]]


@buu_init_vbr.task
def get_contract_group_unit_id(hesa_setup):
    """Get the contract ID from the Hesabu setup.

    Parameters
    ----------
    hesabu_setup: dict
        The Hesabu setup.

    Returns
    -------
    str:
        The contract ID.
    """
    return hesa_setup["main_contract_id"]


@buu_init_vbr.task
def get_hesabu(con_hesabu):
    """Start the connection to the Hesabu instance.

    Parameters
    ----------
    con_hesabu:
        Connection to the Hesabu instance.

    Returns
    -------
    Hesabu instance
        The Hesabu instance.
    """
    return workspace.get_connection(con_hesabu)


@buu_init_vbr.task
def fetch_hesabu_package(con_hesabu, package_ids):
    """Using the Hesabu connection, get the codes for the information that we need from each of the packages.
    You have a list of package IDs. They correspond to the different informations that we are going to want to extract from DHIS2.
    These packages contain different informations. We go into the Hesabu page to get the codes/names/etc of those informations.

    Parameters
    ----------
    con_hesabu:
        Connection to the Hesabu instance.
    package_ids: list
        The IDs of the informations that we want to extract from DHIS2.

    Returns
    -------
    dict:
        A dictionary containing the codes/names/etc that we are going to want to extract from DHIS2.

    """
    headers = {
        "Accept": "application/vnd.api+json;version=2",
        "Accept-Language": "en-US",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "X-Token": con_hesabu.token,
        "X-Dhis2UserId": con_hesabu.dhis2_user_id,
    }
    hesabu_packages = {}
    for package_id in package_ids:
        url = f"{con_hesabu.url}/{package_id}"
        response = requests.get(url, headers=headers)
        hesabu_payload = response.json()
        hesabu_package = data_extraction.deserialize(hesabu_payload)

        activities = []
        freq = hesabu_package["frequency"]
        for activity in hesabu_package["projectActivities"]:
            some_ext = [
                mapping for mapping in activity["inputMappings"] if "externalReference" in mapping
            ]
            if len(activity["inputMappings"]) > 0 and len(some_ext) > 0:
                new_activity = {
                    "name": activity["name"],
                    "id": activity["id"],
                    "code": activity["code"],
                    "inputMappings": {},
                }

                for mapping in activity["inputMappings"]:
                    if "externalReference" in mapping:
                        new_activity["inputMappings"][mapping["input"]["code"]] = {
                            "externalReference": mapping["externalReference"],
                            "name": mapping["name"],
                        }

                activities.append(new_activity)

            activities = sorted(activities, key=lambda x: x["code"])

        hesabu_package["activities"] = activities
        hesabu_packages[package_id] = {"content": hesabu_package, "freq": freq}
    return hesabu_packages


@buu_init_vbr.task
def get_package_values(dhis, periods, hesabu_packages, contract_group, extract):
    """Create a CSV file for each of the packages and each of the periods.
    Each of the packages contains a kind of information (for example, the xxxx).
    In order to fill the CVSs files, we use the codes/names/etc that we extracted from Hesabu.
    Then, we go to DHIS2 and get the data for each of those codes/names/etc.
    We create a CSV file for each of the packages and each of the periods.

    Parameters
    ----------
    dhis:
        Connection to the DHIS2 instance.
    periods: list
        The periods to be considered.
    hesabu_packages: dict
        A dictionary containing the codes/names/etc that we are going to want to extract from DHIS2.
    contract_group: str
        The contract ID.
    extract: bool
        If True, extract the data. If False, we don't do anything. (We assume that the pertinent data was already extracted).

    Returns
    -------
    bool:
        We indicate that the process has finished.
    """
    if extract:
        for package_id in hesabu_packages:
            # We iterate over each of the packages we want to extract.
            package = hesabu_packages[package_id]["content"]
            freq = hesabu_packages[package_id]["freq"]
            if freq != "monthly":
                periods_adapted = list({dates.month_to_quarter(str(pe)) for pe in periods})
                current_run.log_info(f"Adapted periods: {periods_adapted}")
            else:
                periods_adapted = periods
            package_name = package["name"]
            deg_external_reference = package["degExternalReference"]
            org_unit_ids = data_extraction.get_org_unit_ids_from_hesabu(
                contract_group, package, dhis
            )
            # Here, we input the contract ID, the package that we are interested in and the DHIS2 connection.
            # We output a set of IDs of the organization units that we need to look at.
            org_unit_ids = list(org_unit_ids)
            current_run.log_info(
                f"Fetching data for package {package_name} for {len(org_unit_ids)} org units"
            )
            if len(org_unit_ids) > 0:
                data_extraction.fetch_data_values(
                    dhis,
                    deg_external_reference,
                    org_unit_ids,
                    periods_adapted,
                    package["activities"],
                    package_id,
                    f"{workspace.files_path}/pipelines/initialize_vbr/packages",
                )
                # Here, we input the DHIS2 connection, the degree of external reference, the list of IDs of the organization units,
                # the periods we are interested in, the activities we are interested in and the package ID.
                # We output a CSV file for each of the packages and each of the periods.

    return True


@buu_init_vbr.task
def fetch_contracts(dhis, contract_program_id, model_name):
    """Using the DHIS2 connection and the ID of the contract, get the description of the data elements.

    Parameters
    ----------
    dhis:
        Connection to the DHIS2 instance.
    contract_program_id: str
        The ID of the program  in DHIS2 that we want to extract the data elements from.
        (The data in DHIS2 is organized by contracts.
        We specify the ID of the contract that relates to VBR in the pertinent country)
    model_name: str
        The name of the model. We will use it for the name of the csv file.

    Returns
    -------
    records_df: pd.DataFrame
        A DataFrame containing the information about the data elements we want to extract.
    """
    program = dhis.api.get(
        f"programs/{contract_program_id}.json?fields=id,name,programStages[:all,programStageDataElements[dataElement[id,code,name,optionSet[options[code,name]]]"
    )
    data_elements = program["programStages"][0]["programStageDataElements"]
    data_value_headers = {}
    for de in data_elements:
        data_value_headers[de["dataElement"]["id"]] = de

    data = dhis.api.get(
        f"sqlViews/QNKOsX4EGEk/data.json?var=programId:{contract_program_id}&paging=false"
    )
    headers = data["listGrid"]["headers"]

    records = []
    rows = data["listGrid"]["rows"]
    for row in rows:
        record = {}
        index = 0
        for header in headers:
            value = row[index]
            record[header["name"]] = value
            index = index + 1

        data_values = json.loads(record["data_values"])
        for data_value in data_values:
            field_name = data_value_headers[data_value]["dataElement"]["code"]
            record[field_name] = data_values[data_value]["value"]
            record["last_modified_at"] = data_values[data_value]["lastUpdated"]

        del record["data_values"]

        record["start_period"] = record["contract_start_date"].replace("-", "")[0:6]
        record["end_period"] = record["contract_end_date"].replace("-", "")[0:6]

        records.append(record)

    records_df = pd.DataFrame(records)
    records_df.to_csv(
        f"{workspace.files_path}/pipelines/initialize_vbr/data/contracts/contracts_{model_name}.csv",
        index=False,
    )
    return records_df


@buu_init_vbr.task
def get_periods(period, window):
    """Get the periods.

    Parameters
    ----------
    period: str
        The end of the period to be considered. Is inputed by the user.
    window: int
        The number of months to be considered. Is inputed by the user.

    Returns
    -------
    list:
        The periods to be considered.
    """
    frequency = get_period_type(period)
    start, end = get_start_end(period, window, frequency)
    current_run.log_info(f"Periods considered: {start} to {end}")
    return dates.get_date_series(start, end, frequency)


def get_period_type(period):
    """Decide if the period is a month or a quarter.

    Parameters
    ----------
    period: str
        The end of the period to be considered. Is inputed by the user.

    Returns
    -------
    str:
        The type of the period. Either "month" or "quarter".
    """
    if "Q" in period:
        return "quarter"
    return "month"


def get_start_end(period, window, frequency):
    """Get the periods.

    Parameters
    ----------
    period: str
        The end of the period to be considered. Is inputed by the user.
    window: int
        The number of months to be considered. Is inputed by the user.
    frequency: str
        The type of the period. Either "month" or "quarter".

    Returns
    -------
    str:
        The start of the period to be considered.
    str:
        The end of the period to be considered.
    """
    if frequency == "quarter":
        year = int(period[:4])
        quarter = int(period[-1])
        end = year * 100 + quarter * 3
        start = dates.months_before(end, window)
        start = dates.month_to_quarter(start)
        end = dates.month_to_quarter(end)
    else:
        end = int(period)
        start = dates.months_before(end, window)
    return str(start), str(end)


if __name__ == "__main__":
    buu_init_vbr()
