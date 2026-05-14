import json
from polars.exceptions import ComputeError
from pathlib import Path
from typing import Any

import polars as pl
from openhexa.sdk import current_run


def read_csv(file_path: Path) -> pl.DataFrame:
    """Reads input data from a csv file and returns it as a Polars DataFrame.
    Tries first with UTF-8 encoding, and if it fails due to invalid UTF-8 sequences,
    it retries with 'iso-8859-1' encoding.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the input data read from the CSV file.
    """
    try:
        df = pl.read_csv(file_path)
        current_run.log_info(f"Input data read successfully with UTF-8 from {file_path}.")
        return df
    except FileNotFoundError as e:
        raise FileNotFoundError(f"The file '{file_path}' was not found: {e}") from e
    except ComputeError:
        current_run.log_warning(
            f"UTF-8 decoding failed for {file_path}. Attempting with 'iso-8859-1'..."
        )
        try:
            df = pl.read_csv(file_path, encoding="iso-8859-1")
            current_run.log_info(
                f"Input data read successfully with 'iso-8859-1' from {file_path}."
            )
            return df
        except Exception as fallback_e:
            raise Exception(
                f"An error occurred while reading input data with fallback encoding: {fallback_e}"
            ) from fallback_e
    except Exception as e:
        raise Exception(f"An error occurred while reading input data: {e}") from e


def save_outputs(
    output_dir: Path,
    filtered_data: pl.DataFrame,
    transformed_data: pl.DataFrame,
    payload: list[dict],
    post_results: dict[str, Any],
    summary: dict[str, Any],
) -> None:
    """Save all pipeline outputs to the run output directory."""
    if len(filtered_data) > 0:
        filtered_data.write_parquet(output_dir / "filtered_data.parquet")
        current_run.add_file_output((output_dir / "filtered_data.parquet").as_posix())

    if len(transformed_data) > 0:
        transformed_data.write_parquet(output_dir / "transformed_data.parquet")
        current_run.add_file_output((output_dir / "transformed_data.parquet").as_posix())

    if payload:
        pl.DataFrame(payload).write_parquet(output_dir / "payload.parquet")
        current_run.add_file_output((output_dir / "payload.parquet").as_posix())

    failed_chunks = post_results.get("failed_chunks", [])
    if failed_chunks:
        failed_chunks_file = output_dir / "failed_chunks.json"
        with failed_chunks_file.open("w", encoding="utf-8") as f:
            json.dump(failed_chunks, f, indent=2)
        current_run.add_file_output(failed_chunks_file.as_posix())

    summary_file = output_dir / "pipeline_summary.json"
    with summary_file.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    current_run.add_file_output(summary_file.as_posix())
    current_run.log_info(f"Outputs saved to {output_dir}")


def coerce_value(value, value_type):
    """Coerce a value to the correct type for DHIS2. Returns None if not possible.

    Args:
        value: The value to be coerced.
        value_type: The DHIS2 value type to coerce to.

    Returns:
        The coerced value, or None if coercion is not possible.
    """
    try:
        if value_type == "INTEGER":
            return int(value)
        elif value_type == "NUMBER":
            return float(value)
        elif value_type == "UNIT_INTERVAL":
            v = float(value)
            return v if 0 <= v <= 1 else None
        elif value_type == "PERCENTAGE":
            v = float(value)
            return v if 0 <= v <= 100 else None
        elif value_type == "INTEGER_POSITIVE":
            v = int(value)
            return v if v > 0 else None
        elif value_type == "INTEGER_NEGATIVE":
            v = int(value)
            return v if v < 0 else None
        elif value_type == "INTEGER_ZERO_OR_POSITIVE":
            v = int(value)
            return v if v >= 0 else None
        elif value_type in ("TEXT", "LONG_TEXT"):
            s = str(value)
            if value_type == "TEXT" and len(s) > 50000:
                return None
            return s
        elif value_type == "LETTER":
            s = str(value)
            return s if len(s) == 1 else None
        elif value_type == "BOOLEAN":
            if isinstance(value, bool):
                return value
            if str(value).strip().lower() in ["true", "1", "yes", "y"]:
                return True
            if str(value).strip().lower() in ["false", "0", "no", "n"]:
                return False
            return None
        else:
            current_run.log_warning(f"Unknown DHIS2 value type '{value_type}'; passing value as string")
            return str(value)
    except (ValueError, TypeError):
        return None
