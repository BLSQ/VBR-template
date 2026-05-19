"""Template for newly generated pipelines."""

import json
import os
import pickle
from typing import Any
import numpy as np

import pandas as pd
import requests
from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2

from RBV_package import data_extraction, dates
from RBV_package import rbv_environment as rbv

import config
import toolbox


@pipeline("BUU_init_VBR")
@parameter(
    "dhis_con",
    type=DHIS2Connection,
    help="Connection to DHIS2",
    default="pbf-burundi",
    required=True,
)
@parameter("hesabu_con", type=str, help="Connection to Hesabu", default="hesabu", required=True)
@parameter(
    "program_id",
    type=str,
    help="DHIS2 program ID for the VBR contracts",
    default="okgPNNENgtD",
    required=True,
)
@parameter(
    "period",
    name="period",
    type=str,
    help="End month of the extraction (YYYYMM)",
    required=True,
    default="202603",
)
@parameter(
    "window",
    type=int,
    help="Number of months to extract",
    required=True,
    default=3,
)
@parameter(
    "model_name",
    name="Name of the output initialization file",
    type=str,
    default="model",
    required=True,
)
@parameter(
    "selection_provinces",
    name="Save only some provinces in the output simulation file",
    type=bool,
    default=False,
)
@parameter(
    "clean_data",
    help="Clean the data of the output simulation file",
    type=bool,
    default=True,
)
@parameter("extract", name="Extract all of the data", type=bool, default=True)
def buu_init_vbr(
    dhis_con: DHIS2Connection,
    hesabu_con: str,
    program_id: str,
    period: str,
    extract: bool,
    model_name: str,
    window: int,
    selection_provinces: bool,
    clean_data: bool,
) -> None:
    """Pipeline to extract the necessary data.

    We use Hesabu and DHIS2 to obtain information about different health centers.
    We obtain both quantity and quality data, and we store them in the files quantity_data.csv and quality_data.csv.
    """
    dhis = get_dhis2(dhis_con)
    hesabu = get_hesabu(hesabu_con)
    hesabu_token = get_hesabu_token(dhis)
    hesabu_params = get_hesabu_vbr_setup()
    packages_hesabu = get_hesabu_package_ids(hesabu_params)
    contract_group_id = get_contract_group_unit_id(hesabu_params)
    packages = fetch_hesabu_package(hesabu, packages_hesabu, hesabu_token)
    adaptaged_packages = adapt_hesabu_packages(packages)
    periods = get_periods(period, window)
    contracts = fetch_contracts(dhis, program_id, model_name)
    ou_list = get_package_values(dhis, periods, adaptaged_packages, contract_group_id, extract)
    verification = get_actual_verification(dhis, periods, ou_list)
    quant_full = prepare_quantity_data(
        ou_list, periods, packages, contracts, hesabu_params, extract, model_name, verification
    )
    quant = clean_quant_data(quant_full, clean_data, model_name)
    # qual = prepare_quality_data(ou_list, periods, packages, hesabu_params, extract, model_name)
    save_simulation_environment(quant, hesabu_params, model_name, selection_provinces)


def get_actual_verification(dhis: DHIS2, periods: list, ou_list: list) -> pd.DataFrame:
    """
    Get the verification information from DHIS2 for the relevant org units and the relevant periods

    Returns
    ------
    pd.DataFrame:
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
        url = f"dataValueSets.json?dataSet={config.verification_ds}{param_ou}{param_pe}"
        res = dhis.api.get(url)

        for dv in res.get("dataValues", []):
            if dv["dataElement"] == config.verification_de:
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

    verification = pd.DataFrame(verification_rows)
    if not verification.empty:
        verification["period"] = pd.to_numeric(verification["period"], errors="raise")
        verification["dhis2_is_not_verified"] = verification["dhis2_is_not_verified"].map(
            {"true": True, "false": False}
        )
    else:
        verification = pd.DataFrame(
            columns=["org_unit_id", "period", "dhis2_is_not_verified"]
        ).astype({"org_unit_id": str, "period": int, "dhis2_is_not_verified": bool})

    return verification


def get_hesabu_token(dhis: DHIS2) -> str:
    """
    Retrieve the hesabu token from the DHIS2 instance

    Returns
    -------
    str
        The hesabu token.
    """
    try:
        endpoint = "dataStore/hesabu/hesabu"
        response = dhis.api.get(endpoint=endpoint)
        return response["token"]
    except Exception as e:
        current_run.log_error(f"Error retrieving hesabu token from the DHIS2 instance: {e}")
        raise ValueError(f"Error retrieving hesabu token from the DHIS2 instance: {e}")


def adapt_hesabu_packages(packages: dict) -> dict:
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
            for coc_activity in [
                "declaree_indiv",
                "nb_cas_remboursement_mfp_80_indiv",
                "nb_cas_remboursement_complets_indiv",
            ]:
                if coc_activity in activity["inputMappings"].keys():
                    packages[package_id]["content"]["activities"][index]["inputMappings"][
                        coc_activity
                    ].update(
                        adapt_to_coc(
                            packages[package_id]["content"]["activities"][index]["inputMappings"][
                                coc_activity
                            ],
                        )
                    )

    return packages


def adapt_to_coc(info: dict) -> dict:
    external_reference = info["externalReference"].split(".")[0]
    coc = info["externalReference"].split(".")[1]
    name = info["name"]

    return {
        "externalReference": external_reference,
        "name": name,
        "categoryOptionCombo": coc,
    }


def prepare_quantity_data(
    done: list,
    periods: list,
    packages: dict,
    contracts: pd.DataFrame,
    hesabu_params: dict,
    extract: bool,
    model_name: str,
    verification: pd.DataFrame,
) -> pd.DataFrame:
    """Create a CSV file with the quantity data.
    (1) We combine all of the data from the packages cvs's that have quantity data.
    (2) We merge it with the data in the contracts.csv file.
    (3) We select the columns that we are interested in.
    (4) We perform some calculations on the data, to have all of the information we will need.
    (5) We save the data in a CSV file.

    Parameters
    ----------
    done: list
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
    path_data = f"{workspace.files_path}/pipelines/initialize_vbr/data/quantity_data/quantity_data_{model_name}.csv"
    if extract or not os.path.exists(path_data):
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
        # The data for Burundi is a bit strange:
        # Some services have the declaree = NaN or 0, and the declaree_indiv filled
        # Some rows have the declaree_indiv = NaN or 0, and the declaree filled.
        # We want to take both into account
        data = data.merge(contracts, on="org_unit_id")
        data = data.merge(verification, on=["org_unit_id", "period"], how="left")
        # data = deal_with_declared_partial_values(data)
        # data = deal_with_declared_indiv_values(data)
        # data = data[data["declaree"].notna() & (data["declaree"] != 0)]

        data = data[
            list(hesabu_params["quantite_attributes"].keys())
            + list(hesabu_params["contracts_attributes"].keys())
            + ["dhis2_is_not_verified"]
        ]
        data["dhis2_is_not_verified"] = data["dhis2_is_not_verified"].fillna(False)
        data.rename(
            columns=hesabu_params["contracts_attributes"],
            inplace=True,
        )
        data.rename(
            columns=hesabu_params["quantite_attributes"],
            inplace=True,
        )
        data["dec_fbp"] = data["dec"] - data["dec_cam"].fillna(0) - data["dec_mfp"].fillna(0)

        data["ver_fbp"] = data["ver"] - data["dec_cam"].fillna(0) - data["dec_mfp"].fillna(0)
        data["ver_cam"] = data["dec_cam"]
        data["ver_mfp"] = data["dec_mfp"]

        data["val_fbp"] = data["val"] - data["dec_cam"].fillna(0) - data["dec_mfp"].fillna(0)
        data["val_cam"] = data["dec_cam"]
        data["val_mfp"] = data["dec_mfp"]

        data["contract_end_date"] = data["contract_end_date"].astype(int)
        data = data[(data["contract_end_date"] >= data["month"]) & (~data["type_ou"].isna())]

        data["gain_verif"] = (data["dec_fbp"] - data["val_fbp"]) * data["tarif"]
        data["subside_sans_verification"] = data["dec_fbp"] * data["tarif"]
        data["subside_avec_verification"] = data["val_fbp"] * data["tarif"]
        data["quarter"] = data["month"].map(dates.month_to_quarter)
        data = rbv.calcul_ecarts(data)
        output_dir = f"{workspace.files_path}/pipelines/initialize_vbr/data/quantity_data"
        os.makedirs(output_dir, exist_ok=True)

        data.to_csv(
            f"{output_dir}/quantity_data_{model_name}.csv",
            index=False,
        )
    else:
        data = pd.read_csv(path_data)
    return data


def deal_with_declared_partial_values(data: pd.DataFrame) -> pd.DataFrame:
    """
    For the cases where declaree is empty, calculate it as declaree_fbp + declaree_cam + declaree_mfp.

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame containing the declared values.

    Returns
    -------
    data: pd.DataFrame
        The DataFrame with the declared values updated.
    """
    cols_to_sum = ["declaree_fbp", "declaree_cam", "declaree_mfp"]
    for col in cols_to_sum:
        if col not in data.columns:
            data[col] = np.NaN

    if "declaree" not in data.columns:
        data["declaree"] = sum(data[col].fillna(0) for col in cols_to_sum)
        return data

    ser_nan_0 = data["declaree"].isna() | (data["declaree"] == 0)
    data.loc[ser_nan_0, "declaree"] = data.loc[ser_nan_0, cols_to_sum].sum(axis=1, skipna=True)

    return data


def deal_with_declared_indiv_values(data: pd.DataFrame) -> pd.DataFrame:
    """
    When declaree is them still empty, replace it with the declaree_indiv.

    Parameters
    ----------
    data: pd.DataFrame
        The DataFrame containing the declared values.

    Returns
    -------
    data: pd.DataFrame
        The DataFrame with the declared values updated.
    """
    data["provenance_data"] = "declaree"
    if "declaree_indiv" in data.columns:
        ser_dec_nan_0 = data["declaree"].isna() | (data["declaree"] == 0)
        ser_dec_indiv_not_nan_0 = data["declaree_indiv"].notna() & (data["declaree_indiv"] != 0)
        # For these cases we do a replacement
        data.loc[ser_dec_nan_0 & ser_dec_indiv_not_nan_0, "declaree"] = data["declaree_indiv"]
        data.loc[ser_dec_nan_0 & ser_dec_indiv_not_nan_0, "validee"] = data["validee_indiv"]
        data.loc[ser_dec_nan_0 & ser_dec_indiv_not_nan_0, "verifiee"] = data["verifiee_indiv"]
        data.loc[ser_dec_nan_0 & ser_dec_indiv_not_nan_0, "provenance_data"] = "declaree_indiv"

    return data


def prepare_quality_data(
    done: list,
    periods: list,
    packages: dict,
    hesabu_params: dict,
    extract: bool,
    model_name: str,
) -> pd.DataFrame:
    """Create a CSV file with the quality data.
    (1) We combine all of the data from the packages cvs's that have quality data.
    (2) We select the columns that we are interested in.
    (3) We perform some calculations on the data, to have all of the information we will need.
    (4) We save the data in a CSV file.

    Parameters
    ----------
    done: list
        TIndicates that the process get_package_values has finished.
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
    path_data = f"{workspace.files_path}/pipelines/initialize_vbr/data/quality_data/quality_data_{model_name}.csv"
    if extract or not os.path.exists(path_data):
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
        output_dir = f"{workspace.files_path}/pipelines/initialize_vbr/data/quality_data"
        os.makedirs(output_dir, exist_ok=True)

        data.to_csv(
            f"{output_dir}/quality_data_{model_name}.csv",
            index=False,
        )
    else:
        data = pd.read_csv(path_data)
    return data


def save_simulation_environment(
    quant: pd.DataFrame, hesabu_params: dict, model_name: str, selection_provinces: bool
) -> list:
    """We save the simulation in a pickle file. We will then access this pickle file in the second pipeline.

    Parameters
    ----------
    quant: pd.DataFrame
        The quantity data.
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
    quality_indicators = []
    qual = pd.DataFrame(columns=["ou", "quarter", "indicator", "denom", "num", "month"])
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


def clean_quant_data(quant: pd.DataFrame, clean_data: bool, model_name: str) -> pd.DataFrame:
    """
    Clean the quantity data. Only if the bool_clean_data is set to True.
    We remove rows with no data, problematic services/OUs, inconsistent dec/ver/val combinations,
    null tarifs, ordering violations (ver > dec by more than 10%), and CAM/MFP contamination.

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
        outliers = pd.DataFrame()
        output_dir = f"{workspace.files_path}/pipelines/initialize_vbr/data/quantity_data"
        os.makedirs(output_dir, exist_ok=True)

        n_before = len(quant)
        quant, outliers = remove_no_data_rows(quant, outliers)
        dropped = n_before - len(quant)
        current_run.log_info(
            f"remove_no_data_rows: removed {dropped} rows ({100 * dropped / n_before:.1f}% of input)"
        )

        original_len = len(quant)
        for step in [
            remove_problematic_services,
            remove_problematic_ous,
            remove_ver_val_without_dec,
            remove_dec_without_ver_and_val,
            remove_null_tarifs,
            remove_ver_exceeds_dec,
            remove_cam_mfp_contamination,
        ]:
            n_before = len(quant)
            quant, outliers = step(quant, outliers)
            dropped = n_before - len(quant)
            current_run.log_info(
                f"{step.__name__}: removed {dropped} rows ({100 * dropped / original_len:.1f}% of original)"
            )

        total_dropped = original_len - len(quant)
        current_run.log_info(
            f"Total removed: {total_dropped} rows ({100 * total_dropped / original_len:.1f}%). "
            f"{len(quant)} rows remain."
        )

        outliers.to_csv(f"{output_dir}/outliers_quantity_data_{model_name}.csv", index=False)
        quant.to_csv(f"{output_dir}/cleaned_quantity_data_{model_name}.csv", index=False)

    return quant


def remove_null_tarifs(
    quant: pd.DataFrame,
    outliers: pd.DataFrame,
    months_okey: list = config.MONTHS_OKEY_DEC_WITHOUT_VER_VAL,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Remove the rows where the tarif is null.

    Parameters
    ----------
    quant: pd.DataFrame
        The quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.
    months_okey: list
        The list of months where the tarif can be null.

    Returns
    -------
    quant: pd.DataFrame
        The cleaned quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.
    """
    ser_null_tarif = quant["tarif"].isna() & (~quant["month"].isin(months_okey))
    new_outliers = quant[ser_null_tarif]
    new_outliers["cleaning_reason"] = "Null tarif"
    quant = quant[~ser_null_tarif]
    outliers = pd.concat([outliers, new_outliers], ignore_index=True)
    return quant, outliers


def remove_no_data_rows(
    quant: pd.DataFrame, outliers: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Remove the rows where dec, ver, and val are all null or zero.

    Parameters
    ----------
    quant: pd.DataFrame
        The quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.

    Returns
    -------
    quant: pd.DataFrame
        The cleaned quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.
    """
    dec_present = quant["dec"].notna() & (quant["dec"] != 0)
    ver_present = quant["ver"].notna() & (quant["ver"] != 0)
    val_present = quant["val"].notna() & (quant["val"] != 0)
    ser_no_data = ~(dec_present | ver_present | val_present)
    new_outliers = quant[ser_no_data]
    new_outliers["cleaning_reason"] = "No data: dec, ver and val are all null or zero"
    quant = quant[~ser_no_data]
    outliers = pd.concat([outliers, new_outliers], ignore_index=True)
    return quant, outliers


def remove_problematic_services(
    quant: pd.DataFrame, outliers: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Remove the rows for services that have no declared data across all periods.

    Parameters
    ----------
    quant: pd.DataFrame
        The quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.

    Returns
    -------
    quant: pd.DataFrame
        The cleaned quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.
    """
    ser_problematic_services = quant["service"].isin(config.PROBLEMATIC_SERVICES)
    new_outliers = quant[ser_problematic_services]
    new_outliers["cleaning_reason"] = "Problematic service: no declared data across all periods"
    quant = quant[~ser_problematic_services]
    outliers = pd.concat([outliers, new_outliers], ignore_index=True)
    return quant, outliers


def remove_problematic_ous(
    quant: pd.DataFrame, outliers: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Remove the rows for org units that have no declared data or no verified/validated data.

    Parameters
    ----------
    quant: pd.DataFrame
        The quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.

    Returns
    -------
    quant: pd.DataFrame
        The cleaned quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.
    """
    ser_problematic_ous = quant["ou"].isin(config.PROBLEMATIC_OUS)
    new_outliers = quant[ser_problematic_ous]
    new_outliers["cleaning_reason"] = (
        "Problematic OU: no declared data or no verified/validated data"
    )
    quant = quant[~ser_problematic_ous]
    outliers = pd.concat([outliers, new_outliers], ignore_index=True)
    return quant, outliers


def remove_ver_val_without_dec(
    quant: pd.DataFrame, outliers: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Remove the rows where verified or validated data is present but declared is absent (null or zero).

    Parameters
    ----------
    quant: pd.DataFrame
        The quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.

    Returns
    -------
    quant: pd.DataFrame
        The cleaned quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.
    """
    dec_absent = quant["dec"].isna() | (quant["dec"] == 0)
    ver_present = quant["ver"].notna() & (quant["ver"] != 0)
    val_present = quant["val"].notna() & (quant["val"] != 0)
    ser_ver_val_without_dec = (ver_present | val_present) & dec_absent
    new_outliers = quant[ser_ver_val_without_dec]
    new_outliers["cleaning_reason"] = (
        "Inconsistency: verified or validated present but declared absent"
    )
    quant = quant[~ser_ver_val_without_dec]
    outliers = pd.concat([outliers, new_outliers], ignore_index=True)
    return quant, outliers


def remove_dec_without_ver_and_val(
    quant: pd.DataFrame,
    outliers: pd.DataFrame,
    months_okey: list = config.MONTHS_OKEY_DEC_WITHOUT_VER_VAL,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Remove the rows where declared data is present but both verified and validated are absent (null or zero).

    Parameters
    ----------
    quant: pd.DataFrame
        The quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.

    Returns
    -------
    quant: pd.DataFrame
        The cleaned quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.
    """
    dec_present = quant["dec"].notna() & (quant["dec"] != 0)
    ver_absent = quant["ver"].isna() | (quant["ver"] == 0)
    val_absent = quant["val"].isna() | (quant["val"] == 0)
    ser_dec_without_ver_and_val = (
        dec_present & ver_absent & val_absent & (~quant["month"].isin(months_okey))
    )
    new_outliers = quant[ser_dec_without_ver_and_val]
    new_outliers["cleaning_reason"] = (
        "Inconsistency: declared present but both verified and validated absent"
    )
    quant = quant[~ser_dec_without_ver_and_val]
    outliers = pd.concat([outliers, new_outliers], ignore_index=True)
    return quant, outliers


def remove_ver_exceeds_dec(
    quant: pd.DataFrame,
    outliers: pd.DataFrame,
    max_pct: float = config.MAX_PCT_VER_EXCEEDS_DEC,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Remove the rows where verified exceeds declared by more than max_pct percent.

    Parameters
    ----------
    quant: pd.DataFrame
        The quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.
    max_pct: float
        The maximum allowed percentage by which verified can exceed declared.
        Default is config.MAX_PCT_VER_EXCEEDS_DEC.

    Returns
    -------
    quant: pd.DataFrame
        The cleaned quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.
    """
    ser_ver_exceeds_dec = (100 * (quant["ver"] - quant["dec"]) / quant["dec"]) > max_pct
    new_outliers = quant[ser_ver_exceeds_dec]
    new_outliers["cleaning_reason"] = (
        f"Ordering violation: verified exceeds declared by more than {max_pct}%"
    )
    quant = quant[~ser_ver_exceeds_dec]
    outliers = pd.concat([outliers, new_outliers], ignore_index=True)
    return quant, outliers


def remove_cam_mfp_contamination(
    quant: pd.DataFrame,
    outliers: pd.DataFrame,
    max_pct: float = config.MAX_PCT_CAM_MFP,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Remove the rows where CAM and MFP declared values together exceed max_pct percent of total declared.

    Parameters
    ----------
    quant: pd.DataFrame
        The quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.
    max_pct: float
        The maximum allowed percentage of CAM+MFP declared out of total declared.
        Default is config.MAX_PCT_CAM_MFP.

    Returns
    -------
    quant: pd.DataFrame
        The cleaned quantity data.
    outliers: pd.DataFrame
        The DataFrame where the outliers are stored.
    """
    cam_mfp_share = 100 * (quant["dec_cam"].fillna(0) + quant["dec_mfp"].fillna(0)) / quant["dec"]
    ser_cam_mfp_contamination = cam_mfp_share > max_pct
    new_outliers = quant[ser_cam_mfp_contamination]
    new_outliers["cleaning_reason"] = (
        f"CAM/MFP contamination: CAM+MFP share of declared exceeds {max_pct}%"
    )
    quant = quant[~ser_cam_mfp_contamination]
    outliers = pd.concat([outliers, new_outliers], ignore_index=True)
    return quant, outliers


def get_dhis2(con_oh: DHIS2Connection) -> DHIS2:
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


def get_hesabu_vbr_setup() -> dict:
    """Open the JSON file with the Hesabu setup.

    Returns
    -------
    Dict:
        Contains the information we want to extract from Hesabu.
    """
    return json.load(
        open(f"{workspace.files_path}/pipelines/initialize_vbr/hesabu/hesabu_vbr_setup.json")
    )


def get_hesabu_package_ids(hesabu_setup: dict) -> list:
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


def get_contract_group_unit_id(hesa_setup: dict) -> str:
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


def get_hesabu(con_hesabu: str) -> Any:
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


def fetch_hesabu_package(con_hesabu: Any, package_ids: list, hesabu_token: str) -> dict:
    """Using the Hesabu connection, get the codes for the information that we need from each of the packages.
    You have a list of package IDs. They correspond to the different informations that we are going to want to extract from DHIS2.
    These packages contain different informations. We go into the Hesabu page to get the codes/names/etc of those informations.

    Parameters
    ----------
    con_hesabu:
        Connection to the Hesabu instance.
    package_ids: list
        The IDs of the informations that we want to extract from DHIS2.
    hesabu_token: str
        The token to connect to Hesabu with

    Returns
    -------
    dict:
        A dictionary containing the codes/names/etc that we are going to want to extract from DHIS2.

    """
    headers = {
        "Accept": "application/vnd.api+json;version=2",
        "Accept-Language": "en-US",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "X-Token": hesabu_token,
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


def get_package_values(
    dhis: DHIS2, periods: list, hesabu_packages: dict, contract_group: str, extract: bool
) -> list:
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
                fetch_data_values(
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
    else:
        # We only need the organizational units
        for package_id in hesabu_packages:
            package = hesabu_packages[package_id]["content"]
            org_unit_ids = data_extraction.get_org_unit_ids_from_hesabu(
                contract_group, package, dhis
            )
            org_unit_ids = list(org_unit_ids)
            full_list_ous.extend(org_unit_ids)
    return full_list_ous


def fetch_data_values(
    dhis: DHIS2,
    deg_external_reference: str,
    org_unit_ids: list,
    periods: list,
    activities: list,
    package_id: int,
    path: str,
) -> None:
    """
    Get the datavalues from DHIS2.

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
        The activities we are interested in. It might have a CategoryOptionCombo.
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


def fetch_contracts(dhis: DHIS2, contract_program_id: str, model_name: str) -> pd.DataFrame:
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
    output_dir = f"{workspace.files_path}/pipelines/initialize_vbr/data/contracts"
    os.makedirs(output_dir, exist_ok=True)
    records_df.to_csv(
        f"{output_dir}/contracts_{model_name}.csv",
        index=False,
    )
    return records_df


def get_periods(period: str, window: int) -> list:
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


def get_period_type(period: str) -> str:
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


def get_start_end(period: str, window: int, frequency: str) -> tuple[str, str]:
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
