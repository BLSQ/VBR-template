"""Template for newly generated pipelines."""

from openhexa.sdk import (
    current_run,
    pipeline,
    workspace,
    parameter,
)
import pandas as pd
import os
from os import listdir, environ
from copy import deepcopy
from os.path import join
from sqlalchemy import create_engine
import regex as re
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
    name="Name for the compilation of the extracted quantity data.",
    default="Suivi des risques act",
    type=str,
    required=True,
)
@parameter(
    "verification_db",
    name="Name of the file with the detailed information for center and period.",
    default="VBR_liste_verification_act",
    type=str,
    required=True,
)
@parameter(
    "simul_stats_db",
    name="Name of the file with the compiled information per province and period.",
    default="VBR_simulation_statistics",
    type=str,
    required=True,
)
def compile_results(
    extraction_folder: str,
    save_db: bool,
    suivi_risques_db: str,
    verification_db: str,
    simul_stats_db: str,
):
    """
    Compile the results from all of the simulations in a single file and database.
    """
    data_path, output_path, quant_path = define_paths(
        extraction_folder,
    )

    create_simulation_statistics_file(data_path, output_path, simul_stats_db, save_db)
    create_verification_file(data_path, output_path, verification_db, save_db)
    create_suivi_des_risques(quant_path, output_path, save_db, suivi_risques_db)


def create_verification_file(
    data_path: str,
    output_path: str,
    verification_db: str,
    save_db: bool,
):
    """
    Creat the simulation statistics file and, if needed, the DB.
    """
    input_files = data_path + "verification_information/"
    output_path_list = output_path + f"{verification_db}.csv"
    list_dfs = []

    for f in listdir(input_files):
        if valid_simulation_name(f):
            new_detailed_df = get_parameters(f).merge(get_statistics(input_files, f), on="name")
            new_df = process_verification_info(new_detailed_df)
            list_dfs.append(new_df)
        else:
            current_run.log_warning(f"The file {f} does not correspond to a valid simulation.")

    df_list = pd.concat(list_dfs, ignore_index=True)

    df_list.rename(columns={"level_3_name": "district", "period": "periode"}, inplace=True)
    df_list = df_list.sort_values(["district", "periode"]).drop("name", axis=1)

    df_list.to_csv(output_path_list, index=False)
    current_run.log_info(f"For the {verification_db}, the columns are: {df_list.columns}")

    if save_db:
        engine = create_engine(environ["WORKSPACE_DATABASE_URL"])
        df_list.to_sql(verification_db, con=engine, if_exists="replace")
        current_run.log_info(f"Saved the {verification_db} in the database.")


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


def create_suivi_des_risques(quant_path: str, output_path: str, save_db: bool, db_name: str):
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

    current_run.log_info(f"For the {db_name}, the columns are: {quant.columns}")

    if save_db:
        engine = create_engine(environ["WORKSPACE_DATABASE_URL"])
        quant.to_sql(db_name, con=engine, if_exists="replace")
        current_run.log_info(f"Saved the {db_name} in the database.")

    quant.to_csv(f"{output_path}/{db_name}.csv", index=False)


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

    return data_path, output_path, quant_path


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
    df["nb_centers_verified"] = df["bool_verified_simulation"].map(lambda x: 1 if x else 0)
    df["nb_centers_not_verified_dhis2"] = df["bool_not_verified_dhis2"].map(lambda x: 1 if x else 0)
    df["nb_centers"] = 1

    df["#_scores_risque_eleve"] = df["categorie_risque"].map(
        lambda x: 1 if x == "high" or x == "uneligible" else 0
    )
    df["#_scores_risque_mod"] = df["categorie_risque"].map(lambda x: 1 if x == "moderate" else 0)
    df["#_scores_risque_faible"] = df["categorie_risque"].map(lambda x: 1 if x == "low" else 0)
    df = df.groupby(
        [
            "period",
            "mdl",
            "frq",
            "obswin",
            "minnb",
            "phigh",
            "plow",
            "cvrf",
            "seub",
            "seum",
            "pai",
            "qtrisk",
            "vglow",
            "vgmod",
            "name",
            "gvrf",
            "level_3_name",
        ],
        as_index=False,
    )[
        [
            "nb_centers",
            "nb_centers_verified",
            "nb_centers_not_verified_dhis2",
            "#_scores_risque_faible",
            "#_scores_risque_mod",
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
