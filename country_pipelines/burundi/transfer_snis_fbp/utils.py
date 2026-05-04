import json
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import Any

import polars as pl
from openhexa.sdk import current_run, workspace
from openhexa.toolbox.dhis2 import DHIS2


def read_json_file(path: str | Path) -> dict:
    """Read and parse a JSON file from the given path.

    Returns:
        dict: Parsed JSON content.
    """
    path = Path(path)
    try:
        with path.open(encoding="utf-8") as f:
            mapping_data = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Mapping file not found: {path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in file {path}: {e}") from e
    except OSError as e:
        raise OSError(f"Error reading file {path}: {e}") from e

    current_run.log_info(f"Successfully loaded mapping file: {path}")
    return mapping_data


def _chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk


def coc_is_valid_for_de(item: dict, de_to_cc: dict, cc_to_coc: dict) -> bool:
    """Return True if the item's categoryOptionCombo belongs to the DE's categoryCombo.

    Args:
        item (dict): A dictionary containing at least "dataElement" and "categoryOptionCombo" keys.
        de_to_cc (dict): Mapping of data element IDs to category combo IDs.
        cc_to_coc (dict): Mapping of category combo IDs to lists of valid category option combo IDs.
    Returns:
        bool: True if the categoryOptionCombo is valid for the dataElement, False otherwise.
    """
    de = item.get("dataElement")
    coc = item.get("categoryOptionCombo")
    if not de or not coc:
        return False
    cc = de_to_cc.get(de)
    if not cc:
        return False
    valid_cocs = cc_to_coc.get(cc)
    if not valid_cocs:
        return False
    return coc in valid_cocs


def validate_and_transform(dv: dict, client: DHIS2, value_types: dict):
    """Validate and coerce a single data value against its DHIS2 value type.

    Args:
        dv (dict): A dictionary representing a data value, containing at least "dataElement" and "value" keys.
        client (DHIS2): An instance of the DHIS2 client to fetch metadata if needed.
        value_types (dict): A cache mapping data element IDs to their DHIS2 value types.

    Returns:
        The coerced value, or None if coercion is not possible.
    """
    de_uid = dv.get("dataElement")
    if not de_uid:
        return None

    if de_uid not in value_types:
        value_types[de_uid] = client.meta.identifiable_objects(de_uid).get("valueType")

    value_type = value_types[de_uid]
    return coerce_value(dv.get("value"), value_type)


def save_outputs(
    dataset_id: str,
    des_source: pl.DataFrame,
    des_target: pl.DataFrame,
    cocs_source: pl.DataFrame,
    cocs_target: pl.DataFrame,
    dataset_org_units: list[str],
    datasets_source: dict,
    source_data: pl.DataFrame,
    payload: list[dict],
    post_results: dict[str, Any],
    summary: dict[str, Any],
) -> None:
    """Create output directories and save all pipeline outputs."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = (
        Path(workspace.files_path)
        / "pipelines"
        / "dhis2_to_dhis2_data_elements"
        / dataset_id
        / timestamp
    )
    metadata_dir = output_dir / "metadata"
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    des_source.write_parquet(metadata_dir / "data_elements_source.parquet")
    des_target.write_parquet(metadata_dir / "data_elements_target.parquet")
    cocs_source.write_parquet(metadata_dir / "category_option_combos_source.parquet")
    cocs_target.write_parquet(metadata_dir / "category_option_combos_target.parquet")
    pl.DataFrame({"org_unit_id": dataset_org_units}).write_parquet(
        metadata_dir / "dataset_org_units.parquet"
    )
    with (metadata_dir / "datasets_source.json").open("w", encoding="utf-8") as f:
        json.dump(datasets_source, f, indent=2)

    source_data.write_parquet(output_dir / "data_values.parquet")
    current_run.add_file_output((output_dir / "data_values.parquet").as_posix())

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
    current_run.log_info(f"✓ Outputs saved to {output_dir}")


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
            return None
    except (ValueError, TypeError):
        return None
