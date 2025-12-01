"""Template for newly generated pipelines."""

import json
import os
import pickle
import warnings
from pandas.errors import SettingWithCopyWarning

import pandas as pd
import polars as pl
import requests
from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2

from RBV_package import data_extraction, dates
from RBV_package import rbv_environment as rbv

import config
import toolbox

warnings.filterwarnings("ignore", category=SettingWithCopyWarning)


@pipeline("buu-init-vbr", name="BUU_init_VBR")
@parameter(
    "period",
    name="Period",
    type=str,
    help="Last month to extract data for. Format YYYYMM or YYYYQt",
    required=True,
    default="202506",
)
@parameter(
    "window",
    type=int,
    name="Number of months",
    help="Number of past months to consider",
    required=True,
    default=9,
)
@parameter(
    "model_name",
    name="Name of the initialization file",
    help="It will be used to name the csv files and the pickle file. Choose something meaningful that you can remember",
    type=str,
    default="test",
    required=True,
)
@parameter(
    "selection_provinces",
    name="Select some provinces",
    help="Save the data only for the provinces indicated in the config file",
    type=bool,
    default=True,
)
@parameter(
    "extract",
    name="Extract all the data",
    help="Extract all of the data, even if it is already present in the workspace",
    type=bool,
    default=True,
)
@parameter(
    "dhis_con",
    type=DHIS2Connection,
    name="Connection to DHIS2 - do not change",
    default="pbf-pmns-rdc",
    required=True,
)
@parameter(
    "hesabu_con",
    type=str,
    name="Connection to hesabu - do not change",
    default="hesabu",
    required=True,
)
@parameter(
    "program_id",
    type=str,
    name="Program ID for contracts - do not change",
    default="iGORRTJbNOk",
    required=True,
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
    ou_list = get_package_values(dhis, periods, packages, contract_group_id, extract)
    verification = get_actual_verification(dhis, periods, ou_list)
    quant = prepare_quantity_data(
        ou_list,
        periods,
        packages,
        contracts,
        hesabu_params,
        extract,
        model_name,
        verification,
    )
    qual = prepare_quality_data(ou_list, periods, packages, hesabu_params, extract, model_name)
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
    verification: pl.DataFrame,
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
    verification: df
        It has the information on whether the centers have been verified or not.

    Returns
    -------
    data: pd.DataFrame
        A DataFrame containing the quantity data.
    """
    if extract:
        current_run.log_info("Preparing the quantity data dataframe...")
        list_all_data = []
        for id in packages:
            if hesabu_params["packages"][str(id)] == "quantite" and os.path.exists(
                f"{workspace.files_path}/pipelines/initialize_vbr/packages/{id}"
            ):
                for p in periods:
                    if os.path.exists(
                        f"{workspace.files_path}/pipelines/initialize_vbr/packages/{id}/{p}.csv"
                    ):
                        df = pl.read_csv(
                            f"{workspace.files_path}/pipelines/initialize_vbr/packages/{id}/{p}.csv"
                        )
                        df = df.join(verification, on=["org_unit_id", "period"], how="left")
                        list_all_data.append(df)
                        # data contains the actual data for the package identified with id.
                        # It has the data for all of the periods.

        # We are now dropping the rows where the dec = 0 or NaN.
        data = pl.concat(list_all_data, how="diagonal").to_pandas()
        data["dhis2_is_not_verified"] = data["dhis2_is_not_verified"].fillna(0).astype(int)
        # There are still some NaNs -- which makes sense, there are OUs for which we have pushed no data.
        data = data[data["declaree"].notna() & (data["declaree"] != 0)]
        data = data.merge(contracts, on="org_unit_id")
        data = data[
            list(hesabu_params["quantite_attributes"].keys())
            + list(hesabu_params["contracts_attributes"].keys())
            + ["dhis2_is_not_verified"]
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

        data["gain_verif"] = (data["dec"] - data["val"]) * data["tarif"]
        data["subside_sans_verification"] = data["dec"] * data["tarif"]
        data["subside_avec_verification"] = data["val"] * data["tarif"]
        data["quarter"] = data["month"].map(dates.month_to_quarter)
        data = rbv.calcul_ecarts(data)

        mark_strange_verification(data, model_name)
        data = format_data_for_verified_centers(data)
        data = data.drop_duplicates()
        # There are duplicate rows -- in Hesabu, we have some services assigned to two packages.

        output_path = f"{workspace.files_path}/pipelines/initialize_vbr/data/quantity_data"
        os.makedirs(output_path, exist_ok=True)
        data.to_csv(
            f"{output_path}/quantity_data_{model_name}.csv",
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
    current_run.log_info(
        "Looking at the coherence of the verification information extracted from DHIS2..."
    )
    weird_data = []
    data_ver_val_not_verified = data[
        (data["dhis2_is_not_verified"] == 1) & ((data["ver"] != 0) | (data["val"] != 0))
    ]
    data_incorrect_is_verified_value = data[~data["dhis2_is_not_verified"].isin([0, 1])]
    verification_conflicts = (
        data.groupby(["ou", "month"])["dhis2_is_not_verified"].nunique().reset_index()
    )
    conflicting_groups = verification_conflicts[verification_conflicts["dhis2_is_not_verified"] > 1]

    if data_ver_val_not_verified.shape[0] > 0:
        current_run.log_info(
            f"There are services that have not been verified but that have verified or validated values: {data_ver_val_not_verified.shape[0]} rows"
        )
        weird_data.append(data_ver_val_not_verified)
    if data_incorrect_is_verified_value.shape[0] > 0:
        current_run.log_info(
            f"There are services that have an incorrect value for is_not_verified: {data_incorrect_is_verified_value.shape[0]} rows"
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


@buu_init_vbr.task
def get_actual_verification(dhis: DHIS2, periods: list, ou_list: list) -> pl.DataFrame:
    """
    Get the verification information from DHIS2 for the relevant org units and the relevant periods

    Returns
    ------
    pl.DataFrame:
        Contains a row per OU and period, with information about the verification
        (false for verified, true for not verified)
        If the OU and period are missing, the center was verified
    """
    current_run.log_info(
        "Fetching the verification information for the relevant org units and periods..."
    )
    param_pe = "".join([f"&period={pe}" for pe in periods])
    chunks = {}
    nb_org_unit_treated = 0
    verification_rows = []

    for i in range(1, len(ou_list) + 1):
        chunks.setdefault(i // 10, []).append(ou_list[i - 1])

    for i, chunk in chunks.items():
        param_ou = "".join([f"&orgUnit={ou}" for ou in chunk])
        url = f"dataValueSets.json?dataSet={config.ds_is_verified}{param_ou}{param_pe}"
        res = dhis.api.get(url)

        for dv in res.get("dataValues", []):
            if dv["dataElement"] == config.de_is_verified:
                verification_rows.append(
                    {
                        "org_unit_id": dv["orgUnit"],
                        "period": dv["period"],
                        "dhis2_is_not_verified": dv["value"],
                    }
                )

        nb_org_unit_treated += 10
        if nb_org_unit_treated % 1000 == 0:
            current_run.log_info(
                f"I have fetched the verification information for {nb_org_unit_treated} orgUnits"
            )

    verification = pl.DataFrame(verification_rows)
    if verification.height > 0:
        verification = verification.with_columns(
            [
                pl.col("period").cast(pl.Int64),
                pl.col("dhis2_is_not_verified").fill_null(0).cast(pl.Int64),
            ]
        )

    else:
        verification = pl.DataFrame(
            schema={
                "org_unit_id": pl.Utf8,
                "period": pl.Int64,
                "dhis2_is_not_verified": pl.Int64,
            }
        )

    return verification


def format_data_for_verified_centers(data) -> pd.DataFrame:
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
    ser_verified_centers = data["dhis2_is_not_verified"] == 1
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
        output_path = f"{workspace.files_path}/pipelines/initialize_vbr/data/quality_data"
        os.makedirs(output_path, exist_ok=True)
        data.to_csv(
            f"{output_path}/quality_data_{model_name}.csv",
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

    output_path = f"{workspace.files_path}/pipelines/initialize_vbr/initialization_simulation"
    os.makedirs(output_path, exist_ok=True)

    with open(
        f"{output_path}/{model_name}.pickle",
        "wb",
    ) as file:
        # Serialize and save the object to the file
        pickle.dump(regions, file)
    current_run.log_info(f"Fichier d'initialisation sauvé: {output_path}/{model_name}.pickle")
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

    Returns
    -------
    full_list_ous:
        A list with all of the organizational Units we need to extract data for.
    """
    full_list_ous = []
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
            full_list_ous.extend(org_unit_ids)
            current_run.log_info(
                f"Fetching data for package {package_name} for {len(org_unit_ids)} org units"
            )
            if len(org_unit_ids) > 0:
                toolbox.fetch_data_values(
                    dhis,
                    deg_external_reference,
                    org_unit_ids,
                    periods_adapted,
                    package["activities"],
                    package_id,
                    f"{workspace.files_path}/pipelines/initialize_vbr/packages",
                    extract,
                )
                # Here, we input the DHIS2 connection, the degree of external reference, the list of IDs of the organization units,
                # the periods we are interested in, the activities we are interested in and the package ID.
                # We output a CSV file for each of the packages and each of the periods.

    else:
        # We only need the organizational units
        for package_id in hesabu_packages:
            package = hesabu_packages[package_id]["content"]
            org_unit_ids = data_extraction.get_org_unit_ids_from_hesabu(
                contract_group, package, dhis
            )
            org_unit_ids = list(org_unit_ids)
            full_list_ous.extend(org_unit_ids)
    full_list_ous = list(set(full_list_ous))
    return full_list_ous


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
    output_path = f"{workspace.files_path}/pipelines/initialize_vbr/data/contracts"
    os.makedirs(output_path, exist_ok=True)
    records_df.to_csv(
        f"{output_path}/contracts_{model_name}.csv",
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
    return toolbox.get_date_series(start, end, frequency)


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
