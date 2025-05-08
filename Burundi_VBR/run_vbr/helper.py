from datetime import datetime
import tempfile
from typing import List
import polars as pl
import pandas as pd
import geopandas as gpd
from urllib.parse import urlparse
from openhexa.sdk import current_run, workspace, Dataset
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import period_from_string, Period
import os
import json


def is_iso_date(date_str: str) -> bool:
    """
    Check if a given string is a valid ISO 8601 date.

    Parameters:
    date_str (str): The string to be checked.

    Returns:
    bool: True if the string is a valid ISO 8601 date, False otherwise.
    """
    try:
        # Try to parse the date string in ISO 8601 format
        datetime.fromisoformat(date_str)
        return True
    except ValueError:
        # If parsing fails, it is not a valid ISO 8601 date
        return False


def get_week_as_DHIS2(date):
    """
    Converts a given date to the DHIS2 week format.

    Args:
        date (str): The date in the format 'YYYY-MM-DD'.

    Returns:
        str: The date in the DHIS2 week format, e.g., 'YYYYWww'.
    """
    date_y = datetime.strptime(date, "%Y-%m-%d").strftime("%Y")
    week_number = datetime.strptime(date, "%Y-%m-%d").isocalendar().week
    return f"{date_y}W{week_number}"


def isodate_to_period_type(date: str, periodType: str) -> Period:
    """
    Converts a given date to the specified period type.

    Args:
        date (str): The input date in ISO format (YYYY-MM-DD).
        periodType (str): The desired period type. Valid options are "Monthly", "Yearly", "Quarterly", "Weekly", and "Daily".

    Returns:
        str: The converted date in the specified period type format.

    Raises:
        ValueError: If an invalid period type is provided.

    """
    if periodType == "Monthly":
        date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m")
    elif periodType == "Yearly":
        date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y")
    elif periodType == "Quarterly":
        date = (
            datetime.strptime(date, "%Y-%m-%d").strftime("%Y")
            + "Q"
            + str((datetime.strptime(date, "%Y-%m-%d").month - 1) // 3 + 1)
        )
    elif periodType == "Weekly":
        date = get_week_as_DHIS2(date)
    elif periodType == "Daily":
        date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d")
    else:
        raise ValueError("Invalid period type provided.")

    return period_from_string(date)


def get_periods_with_no_data(
    retrieve_periods: List[str], start: str, end: str, dataset: dict
) -> List[str]:
    """
    Get the periods with no data associated.

    Args:
        retrieve_periods (List[str]): List of periods with data.
        start (str): Start date in ISO format.
        end (str): End date in ISO format.
        dataset (dict): The dataset metadata.

    Returns:
        List[str]: List of periods with no data associated.
    """
    periodType = dataset["periodType"]
    dataset_name = dataset["name"]
    start = isodate_to_period_type(start, periodType)
    end = isodate_to_period_type(end, periodType)
    print(start, end)
    if start != end:
        excepted_periods = start.get_range(end)
    else:
        excepted_periods = [start]
    retrieve_periods = [period_from_string(p) for p in retrieve_periods]
    missing_periods = [p for p in excepted_periods if p not in retrieve_periods]
    if len(missing_periods) > 0:
        current_run.log_warning(
            f"The following periods have no data associated: {missing_periods} for dataset {dataset_name}"
        )
    return missing_periods


def get_dataelements_with_no_data(retrieve_dataelements: List[str], dataset: dict):
    """
    Returns a list of data elements that are expected but not found in the retrieved data.

    Args:
        retrieve_dataelements (List[str]): A list of data elements retrieved from a source.
        dataset (dict): The dataset metadata.

    Returns:
        List[str]: A list of data elements that are expected but not found in the retrieved data.
    """
    expected_data_elements = dataset["data_elements"]
    dataset_name = dataset["name"]
    missing_dataelements = [
        dx for dx in expected_data_elements if dx not in retrieve_dataelements
    ]
    if len(missing_dataelements) > 0:
        current_run.log_warning(
            f"The following data elements have no data associated: {missing_dataelements} for dataset {dataset_name}"
        )
    return missing_dataelements


# add the data to the dataset
def add_to_dataset(table: pd.DataFrame, dhis2_connection: DHIS2, dataset: Dataset):
    """
    Adds the given table data to a dataset in DHIS2.

    Args:
        table (pd.DataFrame): The table data to be added to the dataset.
        dhis2_connection (DHIS2): The DHIS2 connection object.
        dataset (Dataset): The dataset object to which the data will be added.

    Returns:
        None
    """
    # we do not have access to the connection slug, so we use the url sub-domain instead.. for the moment
    subdomain = urlparse(dhis2_connection.url).netloc.split(".")[0]
    dataset_name = f"{subdomain.replace('-', '_')}_dataset_extraction"
    if dataset is None:
        dataset = search_dataset(dataset_name)
        if dataset is None:
            dataset = workspace.create_dataset(
                dataset_name, "dataset extraction"
            )  # Create dataset
    add_data_to_dataset(
        data=table, dataset=dataset, fname=dataset_name, extension="csv"
    )


def search_dataset(dataset_name: str):
    """
    Searches for a dataset with the given name in the workspace.

    Args:
        dataset_name (str): The name of the dataset to search for.

    Returns:
        dataset: The dataset object if found, None otherwise.
    """
    try:
        dataset = workspace.get_dataset(dataset_name)
    except Exception:
        current_run.log_error(f"Dataset {dataset_name} not found")
        return None
    return dataset


def add_data_to_dataset(data, dataset: Dataset, fname: str, extension: str = "csv"):
    """
    Add files to a dataset by creating a new version
    """

    try:
        if isinstance(data, pd.DataFrame):
            df = data
        elif isinstance(data, gpd.GeoDataFrame):
            df = data
        elif isinstance(data, pl.DataFrame):
            df = data.to_pandas()  # Convert polars dataFrame to pandas
        else:
            raise ValueError(
                "Input data must be a DataFrame, GeoDataFrame or Polars DataFrame."
            )

        # Add file to dataset version
        with tempfile.NamedTemporaryFile(suffix=f".{extension}") as tmp:
            if extension == "parquet":
                df.to_parquet(tmp.name)
            elif extension == "csv":
                df.to_csv(tmp.name, index=False)
            elif extension == "gpkg":
                if not isinstance(df, gpd.GeoDataFrame):
                    raise ValueError("GeoDataFrame required for .gpkg format.")
                df.to_file(tmp.name, driver="GPKG")
            else:
                raise ValueError(f"Unsupported file extension: {extension}")

            # create new version (let's just keep the date as version name?)
            dataset_version_name = datetime.now().strftime("%Y-%m-%d_%H:%M")
            new_version = dataset.create_version(dataset_version_name)
            new_version.add_file(tmp.name, filename=f"{fname}.{extension}")

        current_run.log_info(
            f"File {fname}.{extension} saved in new dataset version: {dataset_version_name}"
        )
    except Exception as e:
        raise Exception(f"Error while saving the dataset version: {e}")


def select_data_elements(data_element_ids, data_elements):
    """
    Selects the data elements from the given list of data_element_ids that are present in the data_elements list.

    Args:
        data_element_ids (list): A list of data element IDs.
        data_elements (list): Another list of data elements IDs.

    Returns:
        list or None: A list of selected data elements IDs if data_element_ids is not empty and contains valid data element IDs,
                     None otherwise.
    """
    if data_element_ids:
        return [dx for dx in data_element_ids if dx in data_elements]
    else:
        return None


def get_levels(ous, orgunit_ids):
    """
    Returns a dictionary mapping the levels of the given orgunits to their IDs.

    Parameters:
    - ous (list): A list of orgunits.
    - orgunit_ids (list): A list of orgunit IDs.

    Returns:
    - dict: A dictionary mapping the levels of the orgunits to their IDs.
    """
    return {ou["level"]: ou["id"] for ou in ous if ou["id"] in orgunit_ids}


def datasets_temp(dhis) -> List[dict]:
    """Get datasets metadata.

    Return
    ------
    list of dict
        Id, name, data elements, indicators and org units of all datasets.
    """
    datasets = {}
    for page in dhis.api.get_paged(
        "dataSets",
        params={
            "fields": "id,name,dataSetElements,indicators,organisationUnits,periodType",
            "pageSize": 10,
        },
    ):
        for ds in page["dataSets"]:
            id = ds.get("id")
            row = datasets.setdefault(id, {})
            row["name"] = ds.get("name")
            row["data_elements"] = [
                dx["dataElement"]["id"] for dx in ds["dataSetElements"]
            ]
            row["indicators"] = [indicator["id"] for indicator in ds["indicators"]]
            row["organisation_units"] = [ou["id"] for ou in ds["organisationUnits"]]
            row["periodType"] = ds["periodType"]
    return datasets


def add_parents(df, parents):
    filtered_parents = {key: parents[key] for key in df["ou"] if key in parents}
    # Transform the `parents` dictionary into a DataFrame
    parents_df = pd.DataFrame.from_dict(filtered_parents, orient="index").reset_index()

    # Rename the index column to match the "ou" column
    parents_df.rename(columns={"index": "ou"}, inplace=True)

    # Join the DataFrame with the parents DataFrame on the "ou" column
    result_df = df.merge(parents_df, on="ou", how="left")
    return result_df


def path_to_parents_ids(ou):
    level = ou.get("level")
    ou[f"level_{level}_id"] = ou.get("id")
    ou[f"level_{level}_name"] = ou.get("name")
    if level > 1:
        for parent_level in range(1, int(level)):
            ou[f"level_{parent_level}_id"] = ou.get("path").split("/")[parent_level]
    return ou


def ous_to_dict(ous: List) -> dict:
    """Convert a list of orgunits to a dictionary."""
    return {ou["id"]: ou for ou in ous}


def find_name(ous: dict, id: str) -> str:
    """Find the name of an orgunit by its ID."""
    try:
        return ous.get(id)["name"]
    except KeyError:
        current_run.log_warning(f"Not Found: {id}")
        return "unknown"


def save_metadata(filename: str, data: dict):
    """Save metadata to a file."""
    if not os.path.exists(f"{workspace.files_path}/metadata"):
        os.makedirs(f"{workspace.files_path}/metadata")
    with open(f"{workspace.files_path}/metadata/{filename}.json", "w") as f:
        f.write(json.dumps(data))


def get_orgunits_with_parents(ous: List) -> dict:
    """
    Processes a list of organizational units (OUs) and adds parent information to each OU.
    Args:
        ous (list or dict): A list or dictionary of organizational units. Each OU should contain at least an "id" and "level".
    Returns:
        dict: A dictionary of organizational units with added parent information. Each OU will have additional keys for parent names at each level.
    The function performs the following steps:
    1. Converts the input list of OUs to a dictionary if necessary.
    2. Iterates through each OU and adds parent information based on the OU's level.
    3. For each level above 1, it adds the parent name corresponding to that level.
    4. Saves the updated metadata.
    5. Returns the updated dictionary of OUs.
    """

    ous_dict = ous_to_dict(ous)
    for ou in ous_dict.values():
        ou = path_to_parents_ids(ou)
        lvl = ou.get("level")
        if lvl > 1:
            for upper_lvl in range(1, lvl):
                parent_id = ou.get(f"level_{upper_lvl}_id")
                ou[f"level_{upper_lvl}_name"] = find_name(ous_dict, parent_id)
    save_metadata("orgunits", ous_dict)
    return ous_dict
