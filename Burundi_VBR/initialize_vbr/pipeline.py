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

import toolbox
import config

warnings.filterwarnings("ignore", category=SettingWithCopyWarning)


@pipeline("BUU_init_VBR")
@parameter(
    "dhis_con",
    type=DHIS2Connection,
    help="Connection to DHIS2",
    default="pbf-burundi",
    required=True,
)
@parameter("hesabu_con", type=str, help="Connection to hesabu", default="hesabu", required=True)
@parameter(
    "program_id",
    type=str,
    help="Program ID for contracts",
    default="okgPNNENgtD",
    required=True,
)
@parameter(
    "period",
    name="period",
    type=str,
    help="Period for the verification (either yyyymm eg 202406 or yyyyQt eg 2024Q2)",
    required=True,
    default="202501",
)
@parameter("model_name", name="Name of the model", type=str, default="model", required=True)
@parameter(
    "window",
    type=int,
    help="Number of months to consider",
    required=True,
    default=24,
)
@parameter("selection_provinces", type=bool, default=False)
@parameter(
    "clean_data",
    help="If true, weird quantite values will be discarded in the pickle file",
    type=bool,
    default=False,
)
@parameter("extract", name="Extraire les données", type=bool, default=True)
def buu_init_vbr(
    dhis_con,
    hesabu_con,
    program_id,
    period,
    extract,
    model_name,
    window,
    selection_provinces,
    clean_data,
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
    adaptaged_packages = adapt_hesabu_packages(packages)
    periods = get_periods(period, window)
    contracts = fetch_contracts(dhis, program_id, model_name)
    done = get_package_values(dhis, periods, adaptaged_packages, contract_group_id, extract)
    quant_full = prepare_quantity_data(
        done, periods, packages, contracts, hesabu_params, extract, model_name
    )
    quant = clean_quant_data(quant_full, clean_data, model_name)
    qual = prepare_quality_data(done, periods, packages, hesabu_params, extract, model_name)
    save_simulation_environment(quant, qual, hesabu_params, model_name, selection_provinces)


@buu_init_vbr.task
def adapt_hesabu_packages(packages):
    """
    We add the new declared values per payment mode to the hesabu packages.

    Parameters
    ----------
    packages: dict
        A dictionary containing the codes/names/etc that we are going to want to extract from DHIS2.

    Returns
    -------
    packages: dict
        The packages with the new declared values per payment mode.
    """
    for package_id in packages:
        for index, activity in enumerate(packages[package_id]["content"]["activities"]):
            if "declaree_indiv" in activity["inputMappings"].keys():
                externalReference = packages[package_id]["content"]["activities"][index][
                    "inputMappings"
                ]["declaree_indiv"]["externalReference"].split(".")[0]
                name = packages[package_id]["content"]["activities"][index]["inputMappings"][
                    "declaree_indiv"
                ]["name"]

                for dec, coc in config.category_options_per_dec.items():
                    packages[package_id]["content"]["activities"][index]["inputMappings"][dec] = {
                        "externalReference": externalReference,
                        "name": name,
                        "categoryOptionCombo": coc,
                    }

    return packages


@buu_init_vbr.task
def prepare_quantity_data(done, periods, packages, contracts, hesabu_params, extract, model_name):
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
        data["dec_fbp"] = data["dec"] - data["dec_cam"] - data["dec_mfp"]
        data["ver_fbp"] = data["ver"] - data["dec_cam"] - data["dec_mfp"]
        data["val_fbp"] = data["val"] - data["dec_cam"] - data["dec_mfp"]
        data = data[data["dec"].notna() & (data["dec"] != 0)]
        data.contract_end_date = data.contract_end_date.astype(int)
        data = data[(data.contract_end_date >= data.month) & (~data.type_ou.isna())]

        data["gain_verif"] = (data["dec_fbp"] - data["val_fbp"]) * data["tarif"]
        data["subside_sans_verification"] = data["dec_fbp"] * data["tarif"]
        data["subside_avec_verification"] = data["val_fbp"] * data["tarif"]
        data["quarter"] = data["month"].map(dates.month_to_quarter)
        data = rbv.calcul_ecarts(data)
        data.to_csv(
            f"{workspace.files_path}/pipelines/initialize_vbr/data/quantity_data/quantity_data_{model_name}.csv",
            index=False,
        )
    else:
        data = pd.read_csv(
            f"{workspace.files_path}/pipelines/initialize_vbr/data/quantity_data/quantity_data_{model_name}.csv"
        )
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
def clean_quant_data(quant, clean_data, model_name):
    """
    Clean the quantity data. Only if the bool_clean_data is set to True.
    We remove the rows where the validated value is a lot bigger than the declared value.

    Parameters
    ----------
    quant: pd.DataFrame
        The quantity data.
    clean_data: bool
        If True, clean the data. If False, we don't do anything.
    model_name: str
        The name of the model. We will use it for the name of the csv file.

    Returns
    -------
    quant: pd.DataFrame
        The cleaned quantity data.
    """
    if clean_data:
        quant, outliers = remove_weirdly_high_validated_values(quant)
        outliers.to_csv(
            f"{workspace.files_path}/pipelines/initialize_vbr/data/quantity_data/outliers_quantity_data_{model_name}.csv",
            index=False,
        )

    return quant


def remove_weirdly_high_validated_values(quant):
    """
    Remove the rows where the validated value is a lot bigger than the declared value.

    Parameters
    ----------
    quant: pd.DataFrame
        The quantity data.

    Returns
    -------
    quant: pd.DataFrame
        The cleaned quantity data.
    """
    max_difference = -1500
    quant["ratio dec-val"] = 100 * (quant["dec"] - quant["val"]) / quant["ver"]
    ser_less_than_1500 = quant["ratio dec-val"] < max_difference
    outliers = quant[ser_less_than_1500].sort_values("ratio dec-val")
    quant = quant[~ser_less_than_1500]
    quant = quant.drop(columns=["ratio dec-val"])
    outliers = outliers.drop(columns=["ratio dec-val"])
    return quant, outliers


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
                toolbox.fetch_data_values(
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
