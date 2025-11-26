import polars as pl
from pathlib import Path
import json
from openhexa.sdk import current_run
from openhexa.toolbox.dhis2.periods import Month, Quarter


import logging
from datetime import datetime

from openhexa.toolbox.dhis2 import DHIS2

logger = logging.getLogger(__name__)


def save_csv(df: pl.DataFrame, file_path: Path):
    """Saves a Polars DataFrame to a CSV file, creating parent directories if needed.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to save.
    file_path : Path
        The path where the CSV file will be saved.
    """
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_csv(file_path)
        current_run.log_info(f"DataFrame successfully saved to CSV at {file_path}.")

    except PermissionError as e:
        raise PermissionError(
            f"Permission denied when trying to save CSV to '{file_path}': {e}"
        ) from e
    except OSError as e:
        raise OSError(f"OS error occurred while saving CSV to '{file_path}': {e}") from e
    except Exception as e:
        raise RuntimeError(f"An unexpected error occurred while saving CSV: {e}") from e


def save_parquet(df: pl.DataFrame, file_path: Path):
    """Saves a Polars DataFrame to a Parquet file, creating parent directories if needed.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to save.
    file_path : Path
        The path where the parquet file will be saved.
    """
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(file_path)
        current_run.log_info(f"DataFrame successfully saved to Parquet at {file_path}.")

    except PermissionError as e:
        raise PermissionError(
            f"Permission denied when trying to save Parquet to '{file_path}': {e}"
        ) from e
    except OSError as e:
        raise OSError(f"OS error occurred while saving Parquet to '{file_path}': {e}") from e
    except Exception as e:
        raise RuntimeError(f"An unexpected error occurred while saving Parquet: {e}") from e


def save_json(data: dict, file_path: Path):
    """Saves a dictionary to a JSON file, creating parent directories if needed.

    Parameters
    ----------
    df : dict
        The dict to save.
    file_path : Path
        The path where the JSON file will be saved.
    """
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        current_run.log_info(f"DataFrame successfully saved to JSON at {file_path}.")

    except PermissionError as e:
        raise PermissionError(
            f"Permission denied when trying to save JSON to '{file_path}': {e}"
        ) from e
    except OSError as e:
        raise OSError(f"OS error occurred while saving JSON to '{file_path}': {e}") from e
    except Exception as e:
        raise RuntimeError(f"An unexpected error occurred while saving JSON: {e}") from e


def load_json(file_path: Path) -> dict:
    """Loads a JSON file into a Python dictionary.

    Parameters
    ----------
    file_path : Path
        The path to the JSON file.

    Returns
    -------
    dict
        The contents of the JSON file as a dictionary.
    """
    try:
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data

    except FileNotFoundError as e:
        raise FileNotFoundError(f"The JSON file was not found: {file_path}") from e
    except PermissionError as e:
        raise PermissionError(
            f"Permission denied when trying to read JSON from '{file_path}': {e}"
        ) from e
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format in file '{file_path}': {e}") from e
    except Exception as e:
        raise RuntimeError(f"An unexpected error occurred while loading JSON: {e}") from e


def get_date_series(start, end, type) -> list:
    """
    Get a list of consecutive months or quarters between two dates.

    Parameters:
    --------------
    start: int
        The starting date (e.g. 201811)
    end: int
        The ending date (e.g. 201811)
    type: str
        The type of period to generate ("month" or "quarter").

    Returns
    ------
    range: list
        A list of consecutive months or quarters between the start and end dates.
    """
    if type == "quarter":
        q1 = Quarter.from_string(start)
        q2 = Quarter.from_string(end)
        range = q1.get_range(q2)
    else:
        m1 = Month.from_string(start)
        m2 = Month.from_string(end)
        range = m1.get_range(m2)
    return range


def month_to_quarter(num) -> str:
    """
    Given a month, return the quarter corresponding to the given month.

    Parameters
    ----------
    num: int
        A given month (e.g. 201808)

    Returns
    -------
    str
        The quarter corresponding to the given month (e.g. 2018Q3)
    """
    num = int(num)
    y = num // 100
    m = num % 100
    return str(y) + "Q" + str((m - 1) // 3 + 1)


class MissingParameterError(Exception):
    """Exception raised when a required parameter is missing."""


class MissingColumnError(Exception):
    """Exception raised when a required column is missing."""


class InvalidDataTypeError(Exception):
    """Exception raised when a column has an invalid data type."""


class InvalidParameterError(Exception):
    """Exception raised when a parameter is invalid."""


DHIS2_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%3f%z"


def extract_dataset(
    dhis2: DHIS2,
    dataset: str,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    periods: list[str] | None = None,
    org_units: list[str] = None,
    org_unit_groups: list[str] = None,
    include_children: bool = False,
    last_updated: datetime | None = None,
) -> pl.DataFrame:
    """Extract dataset data values.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    dataset : str
        Dataset ID.
    start_date : str, optional
        Start date in the format "YYYY-MM-DD".
        Use either start_date and end_date or periods.
    end_date : str, optional
        End date in the format "YYYY-MM-DD".
        Use either start_date and end_date or periods.
    periods : list[str], optional
        Periods to extract data values for (ex: ["202101", "202102", "202103"]). Periods must be
        provided in DHIS2 period format. Use either start_date and end_date or periods.
    org_units : list[str], optional
        Organisation units IDs.
    org_unit_groups : list[str], optional
        Organisation unit groups IDs.
    include_children : bool, optional
        Include children organisation units.
    last_updated : datetime, optional
        Extract only data values that have been updated since this date.

    Returns
    -------
    pl.DataFrame
        Dataframe containing data values with the following columns: data_element_id, period,
        organisation_unit, category_option_combo, attribute_option_combo, value, created,
        last_updated.

    Raises
    ------
    MissingParameter
        If org_units or org_unit_groups is not provided.
    InvalidParameter
        If org_units and org_unit_groups are provided at the same time.
    """
    if org_units is None and org_unit_groups is None:
        msg = "org_units or org_unit_groups must be provided"
        logger.error(msg)
        raise MissingParameterError(msg)

    if org_units is not None and org_unit_groups is not None:
        msg = "org_units and org_unit_groups cannot be provided at the same time"
        logger.error(msg)
        raise InvalidParameterError(msg)

    if not (start_date and end_date) and not periods:
        msg = "Either start_date and end_date or periods must be provided"
        logger.error(msg)
        raise MissingParameterError(msg)

    if (start_date or end_date) and periods:
        msg = "Either start_date and end_date or periods must be provided, not both"
        logger.error(msg)
        raise InvalidParameterError(msg)

    if start_date:
        start_date = start_date.strftime("%Y-%m-%d")
    if end_date:
        end_date = end_date.strftime("%Y-%m-%d")

    values = dhis2.data_value_sets.get(
        datasets=[dataset],
        periods=periods if periods else None,
        start_date=start_date if start_date else None,
        end_date=end_date if end_date else None,
        org_units=org_units if org_units else None,
        org_unit_groups=org_unit_groups if org_unit_groups else None,
        children=include_children,
        last_updated=last_updated.isoformat() if last_updated else None,
    )

    df = _data_values_to_dataframe(values)

    return df


def _data_values_to_dataframe(values: list[dict]) -> pl.DataFrame:
    """Convert a list of raw DHIS2 data values to a Polars DataFrame.

    Parameters
    ----------
    values : list[dict]
        List of data values.

    Returns
    -------
    pl.DataFrame
        Dataframe containing data values with the following columns: data_element_id, period,
        organisation_unit, category_option_combo, attribute_option_combo, value, created,
        last_updated.
    """
    schema = {
        "dataElement": str,
        "period": str,
        "orgUnit": str,
        "categoryOptionCombo": str,
        "attributeOptionCombo": str,
        "value": str,
        "created": str,
        "lastUpdated": str,
    }

    if not values:
        return pl.DataFrame(schema=schema)

    df = pl.DataFrame(data=values, schema=schema)
    df = df.select(
        pl.col("dataElement").alias("data_element_id"),
        pl.col("period"),
        pl.col("orgUnit").alias("organisation_unit_id"),
        pl.col("categoryOptionCombo").alias("category_option_combo_id"),
        pl.col("attributeOptionCombo").alias("attribute_option_combo_id"),
        pl.col("value"),
        pl.col("created").str.to_datetime(DHIS2_DATE_FORMAT).alias("created"),
        pl.col("lastUpdated").str.to_datetime(DHIS2_DATE_FORMAT).alias("last_updated"),
    )

    return df


def calcul_ecarts(q) -> pl.DataFrame:
    """
    Calculate the relations between the declared, verified and validated values.

    Parameters
    ----------
    q : pl.DataFrame
        DataFrame containing the quantitative information for the particular Organizational Unit

    Returns
    -------
    q: pl.DataFrame
        The same DataFrame with the new columns added.
    """
    q = q.with_columns(
        pl.when(pl.col("dec") != 0)
        .then(1 - (pl.col("dec") - pl.coalesce([pl.col("val"), pl.lit(0)])) / pl.col("dec"))
        .when(
            ((pl.col("dec") == 0) | pl.col("dec").is_null())
            & ((pl.col("val").is_not_null()) & (pl.col("val") != 0))
        )
        .then(1)
        .otherwise(None)
        .alias("taux_validation"),
        pl.when(pl.col("dec") != 0)
        .then((pl.col("dec") - pl.coalesce([pl.col("val"), pl.lit(0)])).abs() / pl.col("dec"))
        .when(
            ((pl.col("dec") == 0) | pl.col("dec").is_null())
            & ((pl.col("val").is_not_null()) & (pl.col("val") != 0))
        )
        .then(0)
        .otherwise(None)
        .alias("ecart_dec_val"),
    )
    return q
