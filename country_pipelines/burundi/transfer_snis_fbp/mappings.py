from typing import Any

import polars as pl
from openhexa.sdk import current_run
from openhexa.toolbox.dhis2 import DHIS2

import config
from utils import coerce_value


def validate_data_values(
    data_values: pl.DataFrame,
    client: DHIS2,
    des_target: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Validate and coerce values against their target DE value types.

    Drops rows with None fields or values that cannot be coerced to the expected
    DHIS2 value type. Logs a breakdown of what was dropped.

    Returns:
        pl.DataFrame: Cleaned data with coerced string values, invalid rows removed.
    """
    value_types = {}
    if des_target is not None and "valueType" in des_target.columns:
        value_types = dict(
            data_values.join(
                des_target.select(pl.col("id").alias("data_element_id"), "valueType"),
                on="data_element_id",
                how="left",
            )
            .select(["data_element_id", "valueType"])
            .unique()
            .iter_rows()
        )

    skipped_none = 0
    skipped_invalid = 0
    valid_rows = []

    for row in data_values.iter_rows(named=True):
        if any(v is None for v in row.values()):
            skipped_none += 1
            current_run.log_info(f"Skipping row with None values: {row}")
            continue

        de_uid = row["data_element_id"]
        if de_uid not in value_types:
            value_types[de_uid] = client.meta.identifiable_objects(de_uid).get("valueType")

        coerced = coerce_value(row["value"], value_types.get(de_uid))
        if coerced is None:
            skipped_invalid += 1
            continue

        coerced_str = (
            ("true" if coerced else "false") if isinstance(coerced, bool) else str(coerced)
        )
        valid_rows.append({**row, "value": coerced_str})

    total_dropped = skipped_none + skipped_invalid
    if skipped_none > 0:
        current_run.log_warning(f"Dropped {skipped_none} rows with None field values")
    if skipped_invalid > 0:
        current_run.log_warning(
            f"Dropped {skipped_invalid} rows with values incompatible with their DE value type"
        )
    current_run.log_info(
        f"Value validation: {len(data_values)} → {len(data_values) - total_dropped} records kept"
        f" ({total_dropped} dropped)"
    )

    if not valid_rows:
        return data_values.clear()
    return pl.DataFrame(valid_rows, schema=data_values.schema)


def prepare_data_value_payload(data_values: pl.DataFrame) -> list[dict[str, Any]]:
    """Rename columns and convert to DHIS2 API payload format.

    Returns:
        list[dict[str, Any]]: List of data value dictionaries ready for the API.
    """
    missing_columns = [col for col in config.column_mapping if col not in data_values.columns]
    if missing_columns:
        current_run.log_error(f"Missing required columns: {missing_columns}")
        raise ValueError(f"Missing required columns: {missing_columns}")

    return (
        data_values.rename(config.column_mapping)
        .select(list(config.column_mapping.values()))
        .to_dicts()
    )
