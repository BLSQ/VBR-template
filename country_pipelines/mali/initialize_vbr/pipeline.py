"""Template for newly generated pipelines."""

import json
import os
import pickle

import pandas as pd
import polars as pl
import requests
from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from pathlib import Path
from openhexa.toolbox.dhis2.dataframe import get_organisation_units
from openhexa.toolbox.dhis2.periods import Month


from RBV_package import data_extraction, dates
from RBV_package import rbv_environment as rbv

from toolbox import save_csv, save_json, load_json, get_date_series, extract_dataset, calcul_ecarts
import orgunit


@pipeline("initialize_vbr")
@parameter(
    "period",
    name="Last period",
    type=str,
    help="Last period to extract data from (either yyyymm eg 202406 or yyyyQt eg 2024Q2)",
    required=True,
    default="202512",
)
@parameter(
    "window",
    name="Number of months to consider",
    type=int,
    required=True,
    default=36,
)
@parameter("model_name", name="Name of the model", type=str, default="model_1710_2", required=True)
@parameter(
    "dhis_con",
    type=DHIS2Connection,
    help="Connection to DHIS2",
    default="pbf-mali-blsq",
    required=True,
)
@parameter("hesabu_con", type=str, help="Connection to hesabu", default="hesabu", required=True)
@parameter(
    "selection_provinces",
    name="Selection provinces",
    help="Save only the results coming from some provinces",
    type=bool,
    default=False,
)
@parameter(
    "extract",
    name="Extract data",
    help="Extract all of the data, no matter if it is already there",
    type=bool,
    default=False,
)
@parameter(
    "bool_hesabu_construct",
    name="Construct the Hesabu descriptor",
    help="Construct the Hesabu descriptor from the Hesabu instance",
    type=bool,
    default=False,
)
def initialize_vbr(
    period,
    window,
    model_name,
    dhis_con,
    hesabu_con,
    selection_provinces,
    extract,
    bool_hesabu_construct,
):
    """Pipeline to extract the necessary data.

    We use Hesabu and DHIS2 to obtain information about different health centers.
    We obtain both quantity and quality data, and we store them in the files quantity_data.csv and quality_data.csv.
    """
    dhis = get_dhis2(dhis_con)
    hesabu = get_hesabu(hesabu_con)
    setup = get_setup()
    hesabu_descriptor = fetch_hesabu_descriptor(
        hesabu, setup["hesabu_project_id"], bool_hesabu_construct
    )
    ous_ref = get_organisation_units(dhis)
    config_extraction = construct_config_extraction(
        dhis, bool_hesabu_construct, hesabu_descriptor, ous_ref
    )
    periods = get_periods_dict(period, window)
    data_elements_ids = construct_de_df(hesabu_descriptor, bool_hesabu_construct)
    values_to_use = extract_data_values(
        dhis, config_extraction, periods, data_elements_ids, extract, ous_ref
    )
    contracts = fetch_contracts(dhis, setup["dhis2_program_id"], model_name, extract)
    quant = prepare_quantity_data(values_to_use, contracts, setup, ous_ref, model_name)
    save_simulation_environment(quant, setup, model_name, selection_provinces)


def aggregate_monthly_indicator(data, quarter, indicator, payment_mode):
    """
    Concatenate monthly indicator files into a single DataFrame for a specific quarter.
    We sum the values for the indicator.

    Parameters
    ----------
    list_months: list
        The list of months we will group together
    base_path: str
        The base path where the CSV files are located.
    indicator: str
        The name of the indicator. (declare, valide, etc.)
    quarter: str
        The quarter we are working on (e.g. 2024Q2)
    ous_ref: pl.DataFrame
        A DataFrame containing the organisational unit pyramid.

    Returns
    -------
    pl.DataFrame:
        A DataFrame containing the concatenated and summed data for the specified indicator and quarter.
    """
    mod_df = data.select(
        pl.col("value").alias(indicator).cast(pl.Float64),
        pl.col("organisation_unit_id"),
        pl.col("service"),
        pl.lit(payment_mode).alias("payment_mode"),
    )

    monthly_df = (
        mod_df.group_by(["organisation_unit_id", "service", "payment_mode"])
        .agg(pl.sum(indicator).alias(indicator))
        .with_columns(pl.lit(str(quarter)).alias("quarter"))
    )

    return monthly_df


def format_quarterly_indicator(data, indicator, payment_mode):
    """
    Get the quarterly indicator file as a DataFrame.

    Parameters
    ----------
    base_path: str
        The base path where the CSV files are located.
    indicator: str
        The name of the indicator. (declare, valide, etc.)
    quarter: str
        The quarter we are working on (e.g. 2024Q2)
    ous_ref: pl.DataFrame
        A DataFrame containing the organisational unit pyramid.

    Returns
    -------
    pl.DataFrame:
        A DataFrame containing the data for the specified indicator and quarter.
    """
    mod_df = data.select(
        pl.col("value").alias(indicator),
        pl.col("organisation_unit_id"),
        pl.col("service"),
        pl.col("period").alias("quarter"),
        pl.lit(payment_mode).alias("payment_mode"),
    )

    return mod_df


def change_data_values(present_indicators, quarter, payment_mode, data_elements_codes):
    """
    Adapt the declared, validated and tarif_def data values.

    Parameters
    ----------
    config: dict
        The configuration dictionary.
    dict_periods: dict
        A dictionary mapping frequencies to periods.

    Returns
    -------
    list:
        A list of DataFrames containing the adapted data values.
    """
    data_to_concat = {}

    for indicator, indicator_dict in present_indicators.items():
        data = indicator_dict["data"]
        frequency = indicator_dict["frequency"]

        data = add_services(data, data_elements_codes, payment_mode, indicator)

        if frequency == "Monthly":
            data = aggregate_monthly_indicator(data, quarter, indicator, payment_mode)

        elif frequency == "Quarterly":
            data = format_quarterly_indicator(data, indicator, payment_mode)

        else:
            raise ValueError(f"Frequency {frequency} not recognized for indicator {indicator}.")

        data = data.with_columns(pl.col(indicator).cast(pl.Float64).alias(indicator))
        data_to_concat[indicator] = data

    return data_to_concat


def join_data_values(data_to_concat):
    """
    Put the declared, validated and tarif_def data values together.

    Parameters
    ----------
    data_to_concat: dict
        A dictionary containing the DataFrames to concatenate.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the joined data values.
    """
    declare = data_to_concat["declare"]
    valide = data_to_concat["valide"]
    tarif_def = data_to_concat["tarif_def"]

    merged = declare.join(
        valide,
        on=["organisation_unit_id", "service", "quarter"],
        how="full",
    )

    nulls_declare = merged["declare"].is_null()
    if nulls_declare.any():
        nulls_count = merged.filter(nulls_declare).height
        total_count = merged.height
        current_run.log_warning(
            f"There are {100 * nulls_count / total_count:.2f}% rows that had filled "
            "validated values but not declared values. This is not necessarily very bad,"
            ", I will fill not them with zeros, so you can see"
        )
        merged = merged.with_columns(
            pl.when(pl.col("declare").is_null())
            .then(pl.col("organisation_unit_id_right"))
            .otherwise(pl.col("organisation_unit_id"))
            .alias("organisation_unit_id"),
            pl.when(pl.col("declare").is_null())
            .then(pl.col("quarter_right"))
            .otherwise(pl.col("quarter"))
            .alias("quarter"),
            pl.when(pl.col("declare").is_null())
            .then(pl.col("service_right"))
            .otherwise(pl.col("service"))
            .alias("service"),
            pl.when(pl.col("declare").is_null())
            .then(pl.col("payment_mode_right"))
            .otherwise(pl.col("payment_mode"))
            .alias("payment_mode"),
        ).drop(
            ["organisation_unit_id_right", "service_right", "quarter_right", "payment_mode_right"]
        )
    else:
        merged = merged.drop(
            ["organisation_unit_id_right", "service_right", "quarter_right", "payment_mode_right"]
        )

    nulls_valide = merged["valide"].is_null()
    if nulls_valide.any():
        nulls_count = merged.filter(nulls_valide).height
        total_count = merged.height
        current_run.log_warning(
            f"There are {100 * nulls_count / total_count:.2f}% rows that had declared values"
            " but not validated values. This is not necessarily very bad,"
            ", I will fill not them with zeros, so you can see"
        )

    full_merged = merged.join(
        tarif_def,
        on=["service", "quarter"],
        how="left",
    ).drop(["organisation_unit_id_right", "payment_mode_right"])

    nulls_tarif = full_merged["tarif_def"].is_null()
    if nulls_tarif.any():
        nulls_count = full_merged.filter(nulls_tarif).height
        total_count = full_merged.height
        current_run.log_warning(
            f"There are {100 * nulls_count / total_count:.2f}% rows that do not have a tarif_def."
            " This is not good, as we will not be able to calculate the subsidies for these rows."
            " I will drop them."
        )
        full_merged = full_merged.filter(~nulls_tarif)

    full_merged = full_merged.select(
        [
            "organisation_unit_id",
            "service",
            "payment_mode",
            "declare",
            "quarter",
            "valide",
            "tarif_def",
        ]
    )

    return full_merged


def construct_config_extraction(dhis, bool_hesabu_construct, hesabu_descriptor, ous_ref):
    """
    Construct the configuration dictionary that will allow us to extract all of the relevant data from DHIS2.

    Parameters
    ----------
    dhis: DHIS2
        Connection to the DHIS2 instance.
    bool_hesabu_construct: bool
        If True, construct the Hesabu descriptor from the Hesabu instance.
        If False, read it from the file config_extraction.json.
    hesabu_descriptor: dict
        The Hesabu descriptor.
    ous_ref: pl.DataFrame
        The Organisational Unit pyramid

    Returns
    -------

    """
    path_output = f"{workspace.files_path}/pipelines/initialize_vbr/config/config_extraction.json"

    if os.path.exists(path_output) and not bool_hesabu_construct:
        current_run.log_info(f"Reading config_extraction from {path_output}.")
        config_extraction = load_json(Path(path_output))
        return config_extraction

    current_run.log_info("Constructing config_extraction from Hesabu descriptor.")
    datasets_dataelements = get_all_datasets_with_dataelements(dhis)
    config_extraction = init_config_extraction(hesabu_descriptor, datasets_dataelements)
    config_extraction = check_config(config_extraction)
    ous_accessible = accessible_orgunits(dhis, ous_ref)
    config_extraction = add_metadata_to_config(config_extraction, dhis, ous_accessible)

    save_json(config_extraction, Path(path_output))

    return config_extraction


def add_services(data_values, data_elements_codes, payment_mode, indicator):
    """
    Add the service names to the data values DataFrame.

    Parameters
    ----------
    data_values: pl.DataFrame
        A DataFrame containing the data values.
    data_elements_codes: pl.DataFrame
        A DataFrame containing the data elements codes, per service, indicator and payment type.
    payment_mode: str
        The payment mode we are working on (e.g. pma, etc)
    indicator: str
        The indicator we are working on (declare, valide, tarif_def)

    Returns
    -------
    pl.DataFrame:
        A DataFrame containing the data values with the service names added.

    """
    relevant_data_element_codes = data_elements_codes.filter(pl.col("payment_type") == payment_mode)
    data_values = data_values.join(
        relevant_data_element_codes,
        left_on=["data_element_id"],
        right_on=[indicator],
        how="left",
    )
    null_services = data_values["service"].is_null()
    if null_services.any():
        null_services_count = (
            data_values.filter(null_services).select("data_element_id").unique().height
        )
        total_services = data_values.select("data_element_id").unique().height
        current_run.log_warning(
            f"For the payment {payment_mode} and indicator {indicator},"
            f" {null_services_count} out of {total_services} data elements "
            "did not appear in the hesabu config."
            " I will not take them into account."
        )
        data_values = data_values.filter(~null_services)

    return data_values


def extract_data_values(dhis, config, dict_periods, data_elements_codes, extract, ous_ref):
    """
    Extract the data from DHIS2 and save it in CSV files.
    We save a file per payment mode, indicator and period.

    Parameters
    ----------
    dhis: DHIS2
        Connection to the DHIS2 instance.
    config: dict
        The configuration dictionary.
    dict_periods: dict
        A dictionary mapping frequencies to periods.
    data_elements_codes: pl.DataFrame
        A DataFrame containing the data elements codes.
    extract: bool
        If True, extract all the data from DHIS2. If False, try using the existing CSV files.
    ous_ref: pl.DataFrame
        A DataFrame containing the organisational unit pyramid.

    Returns
    -------
    dict:
        A dictionary mapping payment modes to lists of quarters for which data was successfully extracted.
    """
    quarters = dict_periods["Quarterly"]
    relevant_payment_quarters = {}
    for payment_mode, payment_dict in config.items():
        for quarter in quarters:
            output_file = f"{workspace.files_path}/pipelines/initialize_vbr/packages/{payment_mode}/{quarter}.csv"
            if os.path.exists(output_file) and not extract:
                current_run.log_info(
                    f"Data for {payment_mode} in {quarter} already extracted. Skipping."
                )
                if payment_mode not in relevant_payment_quarters:
                    relevant_payment_quarters[payment_mode] = []
                relevant_payment_quarters[payment_mode].append(quarter)
                continue
            else:
                current_run.log_info(f"Dealing with data for {payment_mode} in {quarter}.")
                present_indicators = extract_data_values_for_quarter_and_payment(
                    dict_periods["Linking"][str(quarter)],
                    str(quarter),
                    payment_mode,
                    payment_dict,
                    dhis,
                    extract,
                )
                if {"declare", "valide", "tarif_def"}.issubset(set(present_indicators.keys())):
                    data_to_concat = change_data_values(
                        present_indicators, quarter, payment_mode, data_elements_codes
                    )
                    merged = join_data_values(data_to_concat)
                    save_csv(merged, Path(output_file))
                    if payment_mode not in relevant_payment_quarters:
                        relevant_payment_quarters[payment_mode] = []
                    relevant_payment_quarters[payment_mode].append(quarter)
                else:
                    if len(present_indicators) == 0:
                        current_run.log_warning(
                            f"No indicators were present for {payment_mode} in {quarter}."
                        )
                    else:
                        current_run.log_warning(
                            f"Only {', '.join(present_indicators.keys())} were present. I will not work with this."
                        )

    return relevant_payment_quarters


def extract_data_value_for_indicator_quarter_and_payment(
    dhis, list_months, quarter, indicator, indicator_dict
):
    """
    Extract the values for a particular indicator, quarter and payment mode.

    Parameters
    ----------
    dhis: DHIS2
        Connection to the DHIS2 instance.
    list_months: list
        The list of months we need to extract data for
    quarter: str
        The quarter we need to extract data for
    indicator: str
        The indicator we need to extract data for (declare, valide, tarif_def)
    indicator_dict: dict
        The dictionary containing the information about the indicator (data set id, org units, frequency)

    Returns
    -------
    pl.DataFrame:
        A DataFrame containing the data for the specified indicator, quarter and payment mode.
    """
    dataset_id = indicator_dict["data_set_id"]
    ou_id_request = indicator_dict["ou_list"]
    frequency = indicator_dict["freq"]
    if frequency == "Monthly":
        periods_to_extract = list_months
    elif frequency == "Quarterly":
        periods_to_extract = [quarter]
    else:
        raise ValueError(f"Frequency {frequency} not recognized for indicator {indicator}.")

    data_values = extract_dataset(
        dhis2=dhis, dataset=dataset_id, periods=periods_to_extract, org_units=ou_id_request
    )
    return data_values


def extract_data_values_for_quarter_and_payment(
    list_months, quarter, payment_mode, payment_dict, dhis, extract
):
    """
    Extract the data for a particular quarter and payment mode.

    Parameters
    ----------
    list_months: list
        The list of months we need to extract data for
    quarter: str
        The quarter we need to extract data for
    payment_mode: str
        The payment mode we need to extract data for (e.g. pma, etc)
    payment_dict: dict
        The dictionary containing the information about the payment mode (indicators, data set ids, org units, frequency)
    dhis: DHIS2
        Connection to the DHIS2 instance.
    extract: bool
        If True, extract all the data from DHIS2. If False, try using the existing CSV files.

    Returns
    -------
    dict:
        A dictionary mapping with the prensent indicators for the specified quarter and payment mode.
    """
    present_indicators = {}
    for indicator, indicator_dict in payment_dict.items():
        folder_path = f"{workspace.files_path}/pipelines/initialize_vbr/packages/{payment_mode}"
        name = f"{indicator}_{quarter}.csv"
        output_file = f"{folder_path}/{name}"
        os.makedirs(folder_path, exist_ok=True)

        # if os.path.exists(output_file):
        if os.path.exists(output_file) and not extract:
            data_values = pl.read_csv(output_file)
        else:
            data_values = extract_data_value_for_indicator_quarter_and_payment(
                dhis, list_months, quarter, indicator, indicator_dict
            )
            # data_values = pl.DataFrame()
            # CHANGE THIS
            if len(data_values) > 0:
                data_values.write_csv(output_file)

        if len(data_values) > 0:
            new_dict = {"data": data_values, "frequency": indicator_dict["freq"]}
            present_indicators[indicator] = new_dict

    return present_indicators


def accessible_orgunits(dhis, ous_ref):
    """
    From a list of all of the organization units, find the ones that the user has access to.

    Parameters
    ----------
    dhis: DHIS2
        Connection to the DHIS2 instance.
    ous_ref: pl.DataFrame
        A DataFrame containing all of the organization units.

    Returns
    -------
    list:
        A list of organization units that the user has access to.
    """
    params = {"fields": "organisationUnits"}
    response = dhis.api.get("me", params=params)
    accessible_orgunit_ids = list(ou["id"] for ou in response.get("organisationUnits", []))
    ous = ous_ref.filter(
        pl.col("level_1_id").is_in(accessible_orgunit_ids)
        | pl.col("level_2_id").is_in(accessible_orgunit_ids)
    )
    result = ous.select("id").unique().to_series().to_list()
    return result


def add_metadata_to_config(config, dhis, ous_accessible):
    """
    Add to the config dictionary the list of organization units and the frequency of each of the indicators.

    Parameters
    ----------
    config: dict
        The configuration dictionary to update.
    dhis: DHIS2
        Connection to the DHIS2 instance.
    ous_accessible: list
        List of organization units that the user has access to.

    Returns
    -------
    dict:
        The updated configuration dictionary with the list of organization units and frequency for each indicator.
    """
    for payment_mode, payment_dict in config.items():
        for indicator, indicator_dict in payment_dict.items():
            dataset_id = indicator_dict["data_set_id"]
            params = {"fields": "periodType,name,organisationUnits"}
            response = dhis.api.get(f"dataSets/{dataset_id}.json", params=params)
            ou_id_dataset = [ou["id"] for ou in response["organisationUnits"]]
            indicator_dict["ou_list"] = [
                ou_id for ou_id in ou_id_dataset if ou_id in ous_accessible
            ]
            indicator_dict["freq"] = response["periodType"]
            payment_dict[indicator] = indicator_dict
        config[payment_mode] = payment_dict

    return config


def check_config(config):
    """
    Check that the config dictionary contains the necessary keys for each payment mode.

    Parameters
    ----------
    config: dict
        The configuration dictionary to check.

    Returns
    -------
    dict:
        The validated configuration dictionary with invalid payment modes removed.
    """
    required_keys = ["declare", "valide", "tarif_def"]
    invalid_modes = []

    for payment_mode, payment_dict in config.items():
        if not all(k in payment_dict for k in required_keys):
            current_run.log_warning(
                f"I will not take the payment mode {payment_mode} into account"
                " because it does not have all of the necessary keys."
            )
            invalid_modes.append(payment_mode)

    for mode in invalid_modes:
        del config[mode]

    return config


def find_dataset_for_dataelements(de_codes, dataset_map):
    """
    From a list of dataEelements ids, find the dataset id that contains all of them.

    Parameters
    ----------
    de_codes: list
        A list of dataElements ids.
    dataset_map: dict
        A dictionary that links each dataset id to the dataElements ids it contains.

    Returns
    -------
    str:
        The id of the dataset that contains all of the dataElements ids.

    Raises
    ------
    ValueError:
        If no dataset contains all of the dataElements ids or if multiple datasets do.
    """
    matching_datasets = []

    for ds_id, codes in dataset_map.items():
        if all(code in codes for code in de_codes):
            matching_datasets.append(ds_id)

    if len(matching_datasets) == 0:
        current_run.log_error(f"No dataset contains all of the dataElements ids: {de_codes}")
        raise ValueError(f"No dataset contains all of the dataElements ids: {de_codes}")
    elif len(matching_datasets) > 1:
        current_run.log_error(
            f"Multiple datasets contain all of the dataElements ids: {de_codes}. Datasets: {matching_datasets}"
        )
        raise ValueError(
            f"Multiple datasets contain all of the dataElements ids: {de_codes}. Datasets: {matching_datasets}"
        )

    return matching_datasets[0]


def get_all_datasets_with_dataelements(dhis2):
    """Construct a dictionary that links each dataset id to the dataElements ids it contains.

    Parameters
    ----------
    dhis2: DHIS2
        Connection to the DHIS2 instance.

    Returns
    -------
    dict:
        A dictionary that links each dataset id to the dataElements ids it contains.
    """
    endpoint = "dataSets.json"
    params = {"fields": "id,dataSetElements[dataElement[id]]", "paging": "false"}

    data = dhis2.api.get(endpoint, params=params)
    datasets = data.get("dataSets", [])

    dataset_map = {}

    for dataset in datasets:
        dataset_id = dataset["id"]
        codes = []
        for dataelement in dataset["dataSetElements"]:
            code = dataelement["dataElement"]["id"]
            if code:
                codes.append(code)
        dataset_map[dataset_id] = codes

    return dataset_map


def init_config_extraction(descriptor, datasets_dataelements):
    """
    From the hesabu descriptor, construct a dictionary that allows us to extract
    all of the relevant data from DHIS2.

    Parameters
    ----------
    descriptor: dict
        The Hesabu descriptor.
    datasets_dataelements: dict
        A dictionary that links each dataElement id to the dataset id it belongs to.

    Returns
    -------
    dict:
        A dictionary that allows us to extract all of the relevant data from DHIS2.
    """
    config = {}
    for payment_type, payment_type_dict in descriptor["payment_rules"].items():
        payment_dict = {}
        for package_name, package_dict in payment_type_dict["packages"].items():
            if "quantite" in package_name:
                for indicator in ["declare", "valide", "tarif_def"]:
                    indicator_dict = {}
                    list_de = []
                    list_activities = package_dict["activities"]
                    for activity in list_activities:
                        list_de.append(activity[indicator])
                    data_set_id = find_dataset_for_dataelements(list_de, datasets_dataelements)
                    indicator_dict["data_set_id"] = data_set_id
                    payment_dict[indicator] = indicator_dict
        config[payment_type] = payment_dict
    return config


def construct_de_df(descriptor, bool_hesabu_construct):
    """ "
    Construct a dictionary that links each service name and payment type
    to the dataElements ids for declare, tarif_def and valide in DHIS2.

    Parameters
    ----------
    descriptor: dict
        The Hesabu descriptor.
    bool_hesabu_construct: bool
        If True, construct the Hesabu descriptor from the Hesabu instance.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the mapping between service names, payment types and dataElements ids.
    """
    path_output = f"{workspace.files_path}/pipelines/initialize_vbr/config/data_elements_codes.csv"
    if os.path.exists(path_output) and not bool_hesabu_construct:
        current_run.log_info(f"Reading data_elements_codes from {path_output}.")
        return pl.read_csv(path_output)

    list_rows = []
    for payment_type, payment_type_dict in descriptor["payment_rules"].items():
        for package_name, package_dict in payment_type_dict["packages"].items():
            if "quantite" in package_name:
                for activity in package_dict["activities"]:
                    new_dict = {
                        "payment_type": payment_type,
                        "service": activity["name"],
                        "declare": activity["declare"],
                        "tarif_def": activity["tarif_def"],
                        "valide": activity["valide"],
                    }
                    list_rows.append(new_dict)

    save_csv(pl.DataFrame(list_rows), Path(path_output))

    return pl.DataFrame(list_rows)


def fetch_hesabu_descriptor(hesabu, project_id, bool_hesabu_construct):
    """
    Get the Hesabu descriptor associated to a particular project_id.

    Parameters
    ----------
    hesabu: HesabuConnection
        The connection to Hesabu.
    project_id: str
        The ID of the project.
    bool_hesabu_construct: bool
        If True, construct the Hesabu descriptor from the Hesabu instance.

    Returns
    -------
    dict
        The Hesabu descriptor.
    """
    path_output = f"{workspace.files_path}/pipelines/initialize_vbr/config/hesabu_descriptor.json"
    if os.path.exists(path_output) and not bool_hesabu_construct:
        current_run.log_info(f"Reading hesabu_descriptor from {path_output}.")
        hesabu_payload = load_json(Path(path_output))
        return hesabu_payload

    current_run.log_info("Fetching hesabu_descriptor from Hesabu instance.")
    headers = {
        "Accept": "application/vnd.api+json;version=2",
        "Accept-Language": "en-US",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "X-Token": hesabu.token,
        "X-Dhis2UserId": hesabu.dhis2_user_id,
    }
    url = f"{hesabu.url.rstrip('/sets')}/descriptors/{project_id}"
    response = requests.get(url, headers=headers)
    hesabu_payload = response.json()

    save_json(hesabu_payload, Path(path_output))

    return hesabu_payload


def prepare_quantity_data(values_to_use, contracts, setup, ous_ref, model_name):
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

    Returns
    -------
    data: pd.DataFrame
        A DataFrame containing the quantity data.
    """
    current_run.log_info(
        "You will be using the data for: "
        + ", ".join(f"{k}: {', '.join(map(str, v))}" for k, v in values_to_use.items())
    )
    dfs = []
    for payment, list_periods in values_to_use.items():
        for period in list_periods:
            file_path = (
                f"{workspace.files_path}/pipelines/initialize_vbr/packages/{payment}/{period}.csv"
            )
            if os.path.exists(file_path):
                df = pl.read_csv(file_path)
                for col in ["declare", "valide", "tarif_def"]:
                    df = df.with_columns(pl.col(col).cast(pl.Float64))
                dfs.append(df)
            else:
                raise FileNotFoundError(f"File {file_path} not found.")

    data = pl.concat(dfs)
    duplicated = data.select(["organisation_unit_id", "service", "quarter"]).is_duplicated()
    if duplicated.any():
        current_run.log_warning(f"Found duplicated rows in quantity data: {duplicated}")

    data = data.join(contracts, left_on="organisation_unit_id", right_on="org_unit_id", how="left")

    data = data.select(
        list(setup["quantite_attributes"].keys()) + list(setup["contracts_attributes"].keys())
    )
    data = data.rename(
        setup["contracts_attributes"],
    ).rename(
        setup["quantite_attributes"],
    )

    null_contracts = data["contract_start_date"].is_null()
    if null_contracts.any():
        data = deal_with_null_contracts(data, ous_ref)

    data = data.with_columns(
        pl.col("contract_end_date").cast(pl.Int64),
        (pl.col("dec") - pl.col("val")) * pl.col("tarif").alias("gain_verif"),
        (pl.col("dec") * pl.col("tarif")).alias("subside_sans_verification"),
        (pl.col("val") * pl.col("tarif")).alias("subside_avec_verification"),
        (pl.col("month").alias("quarter")),
    )
    data = calcul_ecarts(data)

    output_path = (
        f"{workspace.files_path}/pipelines/initialize_vbr/data/quantity_data/{model_name}.csv"
    )
    save_csv(data, Path(output_path))

    return data.to_pandas()


def deal_with_null_contracts(data, ous_ref):
    """
    In this case, there are some organizational units that do not have a contract.
    (I do not know why)
    We will fill the location information with the information from ous_ref.

    Returns
    -------
    """
    null_mask = data["contract_start_date"].is_null()
    null_count = null_mask.sum()
    total_count = data.height
    current_run.log_warning(
        f"There are {100 * null_count / total_count:.2f}% rows with null contract_start_date. "
        "I will fill the location information with the information from the pyramid."
    )

    pyramid = ous_ref.select(
        pl.col("id").alias("ou"),
        pl.col("level").alias("level_pyramid"),
        pl.col("level_1_id").alias("level_1_uid_pyramid"),
        pl.col("level_2_id").alias("level_2_uid_pyramid"),
        pl.col("level_3_id").alias("level_3_uid_pyramid"),
        pl.col("level_4_id").alias("level_4_uid_pyramid"),
        pl.col("level_5_id").alias("level_5_uid_pyramid"),
        pl.col("level_6_id").alias("level_6_uid_pyramid"),
        pl.col("level_7_id").alias("level_7_uid_pyramid"),
        pl.col("level_1_name").alias("level_1_name_pyramid"),
        pl.col("level_2_name").alias("level_2_name_pyramid"),
        pl.col("level_3_name").alias("level_3_name_pyramid"),
        pl.col("level_4_name").alias("level_4_name_pyramid"),
        pl.col("level_5_name").alias("level_5_name_pyramid"),
        pl.col("level_6_name").alias("level_6_name_pyramid"),
        pl.col("level_7_name").alias("level_7_name_pyramid"),
    )

    data = data.join(pyramid, on="ou", how="left")

    data = data.with_columns(
        pl.when(null_mask)
        .then(pl.col("level_1_uid_pyramid"))
        .otherwise(pl.col("level_1_uid"))
        .alias("level_1_uid"),
        pl.when(null_mask)
        .then(pl.col("level_2_uid_pyramid"))
        .otherwise(pl.col("level_2_uid"))
        .alias("level_2_uid"),
        pl.when(null_mask)
        .then(pl.col("level_3_uid_pyramid"))
        .otherwise(pl.col("level_3_uid"))
        .alias("level_3_uid"),
        pl.when(null_mask)
        .then(pl.col("level_4_uid_pyramid"))
        .otherwise(pl.col("level_4_uid"))
        .alias("level_4_uid"),
        pl.when(null_mask)
        .then(pl.col("level_5_uid_pyramid"))
        .otherwise(pl.col("level_5_uid"))
        .alias("level_5_uid"),
        pl.when(null_mask)
        .then(pl.col("level_6_uid_pyramid"))
        .otherwise(pl.col("level_6_uid"))
        .alias("level_6_uid"),
        pl.when(null_mask)
        .then(pl.col("level_7_uid_pyramid"))
        .otherwise(pl.col("level_7_uid"))
        .alias("level_7_uid"),
        pl.when(null_mask)
        .then(pl.col("level_1_name_pyramid"))
        .otherwise(pl.col("level_1_name"))
        .alias("level_1_name"),
        pl.when(null_mask)
        .then(pl.col("level_2_name_pyramid"))
        .otherwise(pl.col("level_2_name"))
        .alias("level_2_name"),
        pl.when(null_mask)
        .then(pl.col("level_3_name_pyramid"))
        .otherwise(pl.col("level_3_name"))
        .alias("level_3_name"),
        pl.when(null_mask)
        .then(pl.col("level_4_name_pyramid"))
        .otherwise(pl.col("level_4_name"))
        .alias("level_4_name"),
        pl.when(null_mask)
        .then(pl.col("level_5_name_pyramid"))
        .otherwise(pl.col("level_5_name"))
        .alias("level_5_name"),
        pl.when(null_mask)
        .then(pl.col("level_6_name_pyramid"))
        .otherwise(pl.col("level_6_name"))
        .alias("level_6_name"),
        pl.when(null_mask)
        .then(pl.col("level_7_name_pyramid"))
        .otherwise(pl.col("level_7_name"))
        .alias("level_7_name"),
        pl.when(null_mask).then(pl.col("level_pyramid")).otherwise(pl.col("level")).alias("level"),
    ).drop(
        [
            "level_1_uid_pyramid",
            "level_2_uid_pyramid",
            "level_3_uid_pyramid",
            "level_4_uid_pyramid",
            "level_5_uid_pyramid",
            "level_6_uid_pyramid",
            "level_7_uid_pyramid",
            "level_1_name_pyramid",
            "level_2_name_pyramid",
            "level_3_name_pyramid",
            "level_4_name_pyramid",
            "level_5_name_pyramid",
            "level_6_name_pyramid",
            "level_7_name_pyramid",
            "level_pyramid",
        ]
    )

    return data


def save_simulation_environment(quant, setup, model_name, selection_provinces):
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
    """
    regions = []
    orgunits = quant["ou"].unique()
    quality_indicators = []
    quality_df = pd.DataFrame(columns=["ou", "indicator", "quarter", "month"])
    if (
        selection_provinces
        and "selection_provinces" in setup
        and len(setup["selection_provinces"]) > 0
    ):
        for province in setup["selection_provinces"]:
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
                        orgunit.Orgunit(
                            ou,
                            temp,
                            quality_df,
                            quality_indicators,
                            False,
                        )
                    )
                    nb_tot += 1

                    if nb_tot % 1000 == 0:
                        current_run.log_info(
                            f"Saving data for province {province}... Current number of orgunits= {nb_tot}"
                        )

            current_run.log_info(
                f"Saved data for province= {province}. Total number of orgunits= {nb_tot}"
            )
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
                    orgunit.Orgunit(
                        ou,
                        temp,
                        quality_df,
                        quality_indicators,
                        False,
                    )
                )
                nb_tot += 1

                if nb_tot % 1000 == 0:
                    current_run.log_info(
                        f"Saving data for the national level... Current number of orgunits= {nb_tot}"
                    )

        current_run.log_info(f"Saved data at national level. Total number of orgunits= {nb_tot}")
        regions.append(group_of_ou)
    base_path = f"{workspace.files_path}/pipelines/initialize_vbr/initialization_simulation"
    os.makedirs(base_path, exist_ok=True)
    name = f"{model_name}.pickle"
    path = f"{base_path}/{name}"

    with open(path, "wb") as file:
        pickle.dump(regions, file)

    current_run.log_info(
        f"Saved the simulation environment in {path}. You can now run the second pipeline."
    )


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


def get_setup():
    """Open the JSON file with the setup.

    Returns
    -------
    Dict:
        Contains the setup.
    """
    current_run.log_info(f"Reading setup.json from {workspace.files_path}.")
    return load_json(Path(f"{workspace.files_path}/pipelines/initialize_vbr/config/setup.json"))


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


def fetch_contracts(dhis, contract_program_id, model_name, extract):
    """Using the DHIS2 connection and the ID of the contract, get the description of the data elements.

    Parameters
    ----------
    dhis:
        Connection to the DHIS2 instance.
    contract_program_id: str
        The ID of the program  in DHIS2 that we want to extract the data elements from.
        (The data in DHIS2 is organized by contracts.
        We specify the ID of the contract that relates to VBR in the pertinent country)

    Returns
    -------
    records_df: pd.DataFrame
        A DataFrame containing the information about the data elements we want to extract.
    """
    output_path = f"{workspace.files_path}/pipelines/initialize_vbr/data/contracts/{model_name}.csv"

    if os.path.exists(output_path) and not extract:
        current_run.log_info(f"Reading contracts from {output_path}.")
        return pl.read_csv(output_path)

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

    records_df = pl.DataFrame(records, infer_schema_length=None)
    save_csv(
        records_df,
        Path(output_path),
    )
    return records_df


def get_periods_dict(period, window):
    """
    Get a dictionary with the periods to extract data from. We will construct it both
    for quarters and months.

    Parameters
    ----------
    period: str
        The end of the period to be considered. Is inputed by the user.
    window: int
        The number of months to be considered. Is inputed by the user.

    Returns
    -------
    dict:
        A dictionary with the periods to extract data from, both for quarters and months.
    """
    frequency = get_period_type(period)
    start, end = get_start_end(period, window, frequency)

    if frequency == "quarter":
        quarters = get_date_series(start, end, type="quarter")
    else:
        start_q = dates.month_to_quarter(int(start))
        end_q = dates.month_to_quarter(int(end))
        quarters = get_date_series(start_q, end_q, type="quarter")

    month_list = []
    linking = {}
    for q in quarters:
        new_months = get_months_in_quarter(q)
        month_list.extend(new_months)
        linking[str(q)] = new_months

    current_run.log_info(f"Quarters considered: {[str(q) for q in quarters]}")
    return {"Quarterly": quarters, "Monthly": month_list, "Linking": linking}


def get_months_in_quarter(q):
    """
    Get the months in a quarter.

    Parameters
    ----------
    q: Quarter
        The quarter in the format yyyyQq (e.g., 2024Q2).

    Returns
    -------
    list:
        The months in the quarter.
    """
    year = int(q.year)
    q = int(q.quarter)
    first_month = (q - 1) * 3 + 1
    months = []
    for m in range(first_month, first_month + 3):
        months.append(Month.from_string(f"{year}{m:02d}"))
    return months


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
        start = dates.months_before(end, window - 1)
        start = dates.month_to_quarter(start)
        end = dates.month_to_quarter(end)
    else:
        end = int(period)
        start = dates.months_before(end, window - 1)
    return str(start), str(end)


if __name__ == "__main__":
    initialize_vbr()
