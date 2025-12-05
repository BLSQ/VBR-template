"""Template for newly generated pipelines."""

from openhexa.sdk import (
    current_run,
    pipeline,
    workspace,
    parameter,
)
import os
from os import listdir, environ
from copy import deepcopy
from sqlalchemy import create_engine
import regex as re
from pathlib import Path
import polars as pl

import config


@pipeline("compile_results")
@parameter(
    "extract_folder",
    name="Name of the folder containing the data to compile",
    default="to_compile",
    type=str,
    required=True,
)
@parameter(
    "quantity_db",
    name="Name of the quantity risks file",
    help="If provided, the results will also be saved in a database",
    # default="quantity",
    type=str,
    required=False,
)
@parameter(
    "reduced_verification_db",
    name="Name of the reduced verification list file",
    help="If provided, the results will also be saved in a database",
    # default="reduced_verification",
    type=str,
    required=False,
)
@parameter(
    "detailed_verification_db",
    name="Name of the detailed verification information file",
    help="If provided, the results will also be saved in a database",
    # default="detailed_verification",
    type=str,
    required=False,
)
@parameter(
    "simulation_statistics_db",
    name="Name of the simulation statistics file",
    help="If provided, the results will also be saved in a database",
    # default="simulation_statistics",
    type=str,
    required=False,
)
def compile_results(
    extract_folder: str,
    quantity_db: str,
    reduced_verification_db: str,
    detailed_verification_db: str,
    simulation_statistics_db: str,
):
    """
    Compile the results from all of the simulations in a single file and database.
    """
    data_path, output_path, quant_path = paths(
        extract_folder,
    )

    simulation_stats(data_path, output_path, simulation_statistics_db)
    verification(data_path, output_path, reduced_verification_db, detailed_verification_db)
    quantity(quant_path, output_path, quantity_db)


def read_csv(path: str) -> pl.DataFrame:
    """
    Read a CSV file into a Polars DataFrame with error handling.

    Parameters
    ----------
    path : str
        Path to the CSV file containing the data.

    Returns
    -------
    pl.DataFrame
        Polars DataFrame containing the CSV data.

    Raises
    ------
    FileNotFoundError
        If the CSV file does not exist.
    pl.io.CsvReaderError
        If there is an error reading/parsing the CSV file.
    OSError
        For other I/O-related issues (e.g., permissions).
    """
    try:
        df = pl.read_csv(path)
        return df

    except FileNotFoundError as e:
        raise FileNotFoundError(f"CSV file not found: {path}") from e

    except pl.io.CsvReaderError as e:
        raise pl.io.CsvReaderError(f"Error reading CSV file '{path}': {e}") from e

    except OSError as e:
        raise OSError(f"Error accessing CSV file '{path}': {e}") from e


def verification(
    data_path: str,
    output_path: str,
    verification_db: str,
    detailed_information_db: str,
):
    """
    Creat the simulation statistics file and, if needed, the DB.

    Parameters
    ----------
    data_path : str
        Path where the data to compile is located.
    output_path : str
        Path where to save the compiled data.
    verification_db : str
        Name of the reduced verification database.
    detailed_information_db : str
        Name of the detailed verification information database.
    """
    input_files = data_path + "verification_information/"
    output_path_reduced = output_path + "verification_reduced.csv"
    output_path_detailed = output_path + "verification_detailed.csv"
    list_dfs_reduced = []
    list_dfs_detailed = []

    for f in listdir(input_files):
        if valid_simulation_name(f):
            parameters = get_parameters(f)
            file = read_csv(f"{input_files}{f}")
            file = file.with_columns(pl.lit(f).alias("name"))
            new_detailed_df = parameters.join(file, on="name")
            new_reduced_df = summarize_verification(new_detailed_df)

            list_dfs_reduced.append(new_reduced_df)
            list_dfs_detailed.append(new_detailed_df)
        else:
            current_run.log_warning(f"The file {f} does not correspond to a valid simulation.")

    df_detailed = pl.concat(list_dfs_detailed, how="vertical_relaxed")
    df_reduced = pl.concat(list_dfs_reduced, how="vertical_relaxed")

    df_reduced = df_reduced.sort(["Province", "Periode"]).drop("name").to_pandas()
    df_detailed = (
        df_detailed.with_columns(
            pl.col(config.col_verification).cast(pl.Int64).alias(config.col_verification),
        )
        .sort(["Province", "Periode"])
        .drop("name")
    ).to_pandas()

    df_reduced.to_csv(output_path_reduced, index=False)
    current_run.log_info(
        f"For the verification reduced file, the columns are: {df_reduced.columns}"
    )

    df_detailed.to_csv(output_path_detailed, index=False)
    current_run.log_info(
        f"For the verification detailed file, the columns are: {df_detailed.columns}"
    )

    if detailed_information_db:
        engine = create_engine(environ["WORKSPACE_DATABASE_URL"])
        df_detailed.to_sql(detailed_information_db, con=engine, if_exists="replace")
        current_run.log_info(f"Saved the {detailed_information_db} in the database.")

    if verification_db:
        engine = create_engine(environ["WORKSPACE_DATABASE_URL"])
        df_reduced.to_sql(verification_db, con=engine, if_exists="replace")
        current_run.log_info(f"Saved the {verification_db} in the database.")


def simulation_stats(data_path: str, output_path: str, simulation_statistics_db: str):
    """
    Creat the simulation statistics file and, if needed, the DB.

    Parameters
    ----------
    data_path : str
        Path where the data to compile is located.
    output_path : str
        Path where to save the compiled data.
    simulation_statistics_db : str
        Name of the simulation statistics database.
    """
    input_files = data_path + "simulation_statistics/"
    output_path = output_path + "simulation_statistics_db.csv"
    list_stats = []

    for f in listdir(input_files):
        if valid_simulation_name(f):
            parameters = get_parameters(f)
            results_simulation = read_csv(f"{input_files}{f}")
            results_simulation = results_simulation.with_columns(pl.lit(f).alias("name"))
            stats_df = parameters.join(results_simulation, on="name")
            list_stats.append(stats_df)
        else:
            current_run.log_warning(f"The file {f} does not correspond to a valid simulation.")

    full_stats = pl.concat(list_stats, how="vertical_relaxed")
    full_stats = full_stats.sort(["Province", "Periode"]).drop("name")
    full_stats = full_stats.to_pandas()

    full_stats.to_csv(output_path, index=False)
    current_run.log_info(
        f"For the simulation statistics file, the columns are: {full_stats.columns}"
    )

    if simulation_statistics_db:
        engine = create_engine(environ["WORKSPACE_DATABASE_URL"])
        full_stats.to_sql(simulation_statistics_db, con=engine, if_exists="replace")
        current_run.log_info(f"Saved the {simulation_statistics_db} in the database.")


def quantity(quant_path: str, output_path: str, db_name: str):
    """
    Compile the quantity file and, if needed, the DB.

    Parameters
    ----------
    quant_path : str
        Path where the quantity data is located.
    output_path : str
        Path where to save the compiled data.
    parents : pl.DataFrame
        Polars DataFrame containing parent information for orgunits.
    db_name : str
        Name of the quantity risks database.
    """
    output_path = output_path + "quantity_data_compiled.csv"
    regex_quant = r"^(.+)\.csv$"
    files_quant = []

    for filename in os.listdir(quant_path):
        match = re.search(regex_quant, filename)
        if match:
            df = read_csv(quant_path + filename)
            df = df.with_columns(pl.lit(match.group(1)).alias("model"))
            files_quant.append(df)
        else:
            current_run.log_warning(f"The file {filename} does not match the expected pattern.")

    quant = pl.concat(files_quant, how="vertical_relaxed").to_pandas()

    current_run.log_info(f"For the quantity file, the columns are: {quant.columns}")
    quant.to_csv(output_path, index=False)

    if db_name:
        engine = create_engine(environ["WORKSPACE_DATABASE_URL"])
        quant.to_sql(db_name, con=engine, if_exists="replace")
        current_run.log_info(f"Saved the {db_name} in the database.")


def paths(extraction_folder: str):
    """
    Define the paths where we will extract the data from / save it in.

    Returns
    -------
    data_path : str
        Path where the data to compile is located.
    output_path : str
        Path where to save the compiled data.
    quant_path : str
        Path where the quantity data is located.
    """
    data_path = f"{workspace.files_path}/pipelines/run_vbr/{extraction_folder}/data/"
    output_path = f"{workspace.files_path}/pipelines/run_vbr/{extraction_folder}/compiled_data/"
    Path(output_path).mkdir(parents=True, exist_ok=True)
    quant_path = f"{workspace.files_path}/pipelines/initialize_vbr/data/quantity_data/"

    return data_path, output_path, quant_path


def valid_simulation_name(filename):
    """
    Check if the filename matches the expected simulation name pattern.

    Parameters
    ----------
    filename : str
        The filename to check.

    Returns
    -------
    bool
        True if the filename matches the expected pattern, False otherwise.
    """
    return all(substring in filename for substring, _ in config.dict_params.items())


def get_parameters(f):
    """
    Get the parameters from the filename.

    Parameters
    ----------
    f : str
        The filename to extract parameters from.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the extracted parameters.
    """
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

    return pl.from_dict(dict_params_full)


def summarize_verification(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate the summary statistics for the verification information.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing the detailed verification information.

    Returns
    -------
    pl.DataFrame
        DataFrame containing the summarized verification statistics.
    """
    df = df.with_columns(
        pl.when(pl.col(config.col_verification)).then(1).otherwise(0).alias("nb_centers_verified"),
        pl.lit(1).alias("nb_centers"),
        pl.when(pl.col(config.col_risk) == "uneligible")
        .then(1)
        .otherwise(0)
        .alias("nb_risque_uneligible"),
        pl.when(pl.col(config.col_risk) == "high").then(1).otherwise(0).alias("nb_risque_high"),
        pl.when(pl.col(config.col_risk) == "moderate")
        .then(1)
        .otherwise(0)
        .alias("nb_risque_moderate"),
        pl.when(pl.col(config.col_risk) == "low").then(1).otherwise(0).alias("nb_risque_low"),
    )

    keys_group_by = list(config.dict_params.keys()) + ["Periode", "Province", "name"]

    df = df.group_by(keys_group_by).agg(
        [
            pl.col("nb_centers").sum(),
            pl.col("nb_centers_verified").sum(),
            pl.col("nb_risque_uneligible").sum(),
            pl.col("nb_risque_high").sum(),
            pl.col("nb_risque_moderate").sum(),
            pl.col("nb_risque_low").sum(),
        ]
    )
    return df


def str_to_date(datestr):
    if isinstance(datestr, str) and "Q" in datestr:
        return datestr[:4] + "-" + str(int(datestr[5]) * 3) + "-1"
    elif isinstance(datestr, int) or datestr.isdigit():
        x = int(datestr)
        return f"{x // 100}-{x % 100}-1"


if __name__ == "__main__":
    compile_results()
