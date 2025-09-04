"""Template for newly generated pipelines."""

from openhexa.sdk import (
    current_run,
    pipeline,
    workspace,
    parameter,
)
import pandas as pd
from tqdm import tqdm
import warnings
import os
from os import listdir, environ
from copy import deepcopy
from os.path import isfile, join
from sqlalchemy import create_engine
import regex as re
import json
from pathlib import Path

import config


@pipeline("compile_results")
@parameter(
    "extraction_folder",
    name="Name of the folder containing the data to compile",
    default="Extraction",
    type=str,
    required=True,
)
@parameter(
    "save_db",
    name="Save results in the DB",
    type=bool,
    default=False,
    help="If True, we will save the results in the database",
)
@parameter(
    "suivi_risques_db",
    name="Name of the quantity risks file",
    default="Suivi des risques act",
    type=str,
    required=True,
)
@parameter(
    "verification_db",
    name="Name of the reduced verification list file",
    default="VBR_liste_verification",
    type=str,
    required=True,
)
@parameter(
    "detailed_information_db",
    name="Name of the detailed verification information file",
    default="VBR_verification_information",
    type=str,
    required=True,
)
@parameter(
    "simul_stats_db",
    name="Name of the simulation statistics file",
    default="VBR_simulation_statistics",
    type=str,
    required=True,
)
def compile_results(
    extraction_folder: str,
    save_db: bool,
    suivi_risques_db: str,
    verification_db: str,
    detailed_information_db: str,
    simul_stats_db: str,
):
    """
    Compile the results from all of the simulations in a single file and database.
    """
    data_path, output_path, quant_path, parents_path = define_paths(
        extraction_folder,
    )
    parents = json.load(open(parents_path))

    create_simulation_statistics_file(data_path, output_path, simul_stats_db, save_db)
    create_verification_file(
        data_path, output_path, verification_db, detailed_information_db, save_db
    )
    create_suivi_des_risques(quant_path, output_path, save_db, parents, suivi_risques_db)


def create_verification_file(
    data_path: str,
    output_path: str,
    verification_db: str,
    detailed_information_db: str,
    save_db: bool,
):
    """
    Creat the simulation statistics file and, if needed, the DB.
    """
    input_files = data_path + "verification_information/"
    output_path_list = output_path + f"{verification_db}.csv"
    output_path_detailed = output_path + f"{detailed_information_db}.csv"
    list_dfs = []
    list_detailed_dfs = []

    for f in listdir(input_files):
        if valid_simulation_name(f):
            new_detailed_df = get_parameters(f).merge(get_statistics(input_files, f), on="name")
            new_df = process_verification_info(new_detailed_df)
            list_dfs.append(new_df)
            list_detailed_dfs.append(new_detailed_df)
        else:
            current_run.log_warning(f"The file {f} does not correspond to a valid simulation.")

    df_detailed = pd.concat(list_detailed_dfs, ignore_index=True)
    df_list = pd.concat(list_dfs, ignore_index=True)

    df_list.rename(columns={"level_2_name": "province", "period": "periode"}, inplace=True)
    df_list = df_list.sort_values(["province", "periode"]).drop("name", axis=1)
    df_detailed.rename(columns={"level_2_name": "province", "period": "periode"}, inplace=True)
    df_detailed = df_detailed.sort_values(["province", "periode"]).drop("name", axis=1)

    df_detailed["date"] = df_detailed["periode"].map(str_to_date)
    df_detailed["bool_verified"] = df_detailed["bool_verified"].astype(int)

    df_list.to_csv(output_path_list, index=False)
    current_run.log_info(f"For the {verification_db}, the columns are: {df_list.columns}")

    df_detailed.to_csv(output_path_detailed, index=False)
    current_run.log_info(
        f"For the {detailed_information_db}, the columns are: {df_detailed.columns}"
    )

    if save_db:
        engine = create_engine(environ["WORKSPACE_DATABASE_URL"])
        df_list.to_sql(verification_db, con=engine, if_exists="replace")
        current_run.log_info(f"Saved the {verification_db} in the database.")
        df_detailed.to_sql(detailed_information_db, con=engine, if_exists="replace")
        current_run.log_info(f"Saved the {detailed_information_db} in the database.")


def create_simulation_statistics_file(
    data_path: str, output_path: str, simul_stats_db: str, save_db: bool
):
    """
    Creat the simulation statistics file and, if needed, the DB.
    """
    input_files = data_path + "simulation_statistics/"
    output_path = output_path + f"{simul_stats_db}.csv"
    list_dfs = []

    for f in listdir(input_files):
        if valid_simulation_name(f):
            new_df = get_parameters(f).merge(get_statistics(input_files, f), on="name")
            list_dfs.append(new_df)
        else:
            current_run.log_warning(f"The file {f} does not correspond to a valid simulation.")

    df_iteration = pd.concat(list_dfs, ignore_index=True)
    df_iteration.rename(columns={"level_2_name": "province", "period": "periode"}, inplace=True)
    df_iteration = df_iteration.sort_values(["province", "periode"]).drop("name", axis=1)
    df_iteration["gain_vbr"] = df_iteration["total cost (syst)"] - df_iteration["total cost (VBR)"]

    df_iteration.to_csv(output_path, index=False)
    current_run.log_info(f"For the {simul_stats_db}, the columns are: {df_iteration.columns}")

    if save_db:
        engine = create_engine(environ["WORKSPACE_DATABASE_URL"])
        df_iteration.to_sql(simul_stats_db, con=engine, if_exists="replace")
        current_run.log_info(f"Saved the {simul_stats_db} in the database.")


def add_parents(df, parents):
    filtered_parents = {key: parents[key] for key in df["ou"] if key in parents}
    # Transform the `parents` dictionary into a DataFrame
    parents_df = pd.DataFrame.from_dict(filtered_parents, orient="index").reset_index()

    # Rename the index column to match the "ou" column
    parents_df.rename(
        columns={
            "index": "ou",
            "level_2_id": "level_2_uid",
            "level_3_id": "level_3_uid",
            "level_4_id": "level_4_uid",
            "level_5_id": "level_5_uid",
        },
        inplace=True,
    )

    # Join the DataFrame with the parents DataFrame on the "ou" column
    result_df = df.merge(parents_df[["ou", "name"]], on="ou", how="left")
    return result_df


def create_suivi_des_risques(
    quant_path: str, output_path: str, save_db: bool, parents: dict, db_name: str
):
    """
    Create the suivi des risques files from the quantqual data paths.
    """
    regex_quant = "^quantity_data_(.+)\.csv$"
    files_quant = []

    for filename in os.listdir(quant_path):
        match = re.search(regex_quant, filename)
        if match:
            found_df = pd.read_csv(quant_path + filename)
            found_df["model"] = match.group(1)
            files_quant.append(found_df)

    quant = pd.concat(files_quant, ignore_index=True)

    quant["ratio dec-ver"] = 100 * (quant["dec"] - quant["ver"]) / quant["ver"]
    quant["ratio ver-val"] = 100 * (quant["ver"] - quant["val"]) / quant["ver"]
    quant["ratio dec-val"] = 100 * (quant["dec"] - quant["val"]) / quant["ver"]
    quant["service non nul"] = quant["dec"].map(lambda x: 1 if x > 0 else 0)

    results = add_parents(quant, parents)

    current_run.log_info(f"For the Suivi des risques, the columns are: {results.columns}")

    if save_db:
        engine = create_engine(environ["WORKSPACE_DATABASE_URL"])
        results.to_sql(db_name, con=engine, if_exists="replace")
        current_run.log_info(f"Saved the {db_name} in the database.")

    results.to_csv(f"{output_path}/VBR_suivi_des_risques.csv", index=False)


def define_paths(extraction_folder: str):
    """
    Define the paths where we will extract the data from / save it in.

    Returns
    -------
    dict:
        It maps the name of the input folder to the output file/db name.
    """
    data_path = f"{workspace.files_path}/pipelines/run_vbr/{extraction_folder}/data/"
    output_path = f"{workspace.files_path}/pipelines/run_vbr/{extraction_folder}/compiled_data/"
    Path(output_path).mkdir(parents=True, exist_ok=True)
    quant_path = f"{workspace.files_path}/pipelines/initialize_vbr/data/quantity_data/"
    parents_path = f"{workspace.files_path}/pipelines/run_vbr/config/orgunits.json"

    return data_path, output_path, quant_path, parents_path


def valid_simulation_name(filename):
    return all(substring in filename for substring, _ in config.dict_params.items())


def get_parameters(f):
    dict_params_full = deepcopy(config.dict_params)
    list_file_params = f.split("-")

    for p in dict_params_full:
        for p_match in list_file_params:
            if p in p_match:
                dict_params_full[p] = [p_match.split("___")[1].replace(".csv", "")]
                break
        else:
            dict_params_full[p] = [None]

    dict_params_full["name"] = [f]

    return pd.DataFrame.from_dict(dict_params_full)


def get_statistics(mypath, f):
    df = pd.read_csv(join(mypath, f))
    df["name"] = f
    return df


def process_verification_info(df):
    df["nb_centers_verified"] = df["bool_verified"].map(lambda x: 1 if x else 0)
    df["nb_centers_not_verified_dhis2"] = df["bool_not_verified_dhis2"].map(lambda x: 1 if x else 0)
    df["nb_centers"] = 1

    df["#_scores_risque_eleve"] = df["categorie_risque"].map(
        lambda x: 1 if x == "high" or x == "uneligible" else 0
    )
    df["#_scores_risque_mod1"] = df["categorie_risque"].map(lambda x: 1 if x == "moderate_1" else 0)
    df["#_scores_risque_mod2"] = df["categorie_risque"].map(lambda x: 1 if x == "moderate_2" else 0)
    df["#_scores_risque_mod3"] = df["categorie_risque"].map(lambda x: 1 if x == "moderate_3" else 0)
    df["#_scores_risque_faible"] = df["categorie_risque"].map(lambda x: 1 if x == "low" else 0)
    df = df.groupby(
        [
            "period",
            "model",
            "frq",
            "obswin",
            "minnb",
            "phigh",
            "plow",
            "cvrf",
            "seub",
            "seum",
            "pai",
            "mxs",
            "qtrisk",
            "vglow",
            "vgmod",
            "name",
            "level_2_name",
            "level_3_name",
        ],
        as_index=False,
    )[
        [
            "nb_centers",
            "nb_centers_verified",
            "nb_centers_not_verified_dhis2#_scores_risque_faible",
            "#_scores_risque_mod1",
            "#_scores_risque_mod2",
            "#_scores_risque_mod3",
            "#_scores_risque_eleve",
        ]
    ].sum()
    return df


def str_to_date(datestr):
    if isinstance(datestr, str) and "Q" in datestr:
        return datestr[:4] + "-" + str(int(datestr[5]) * 3) + "-1"
    elif isinstance(datestr, int) or datestr.isdigit():
        x = int(datestr)
        return f"{x // 100}-{x % 100}-1"


if __name__ == "__main__":
    compile_results()
