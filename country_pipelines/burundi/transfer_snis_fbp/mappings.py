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


def validate_ou_mapping(ous_source: pl.DataFrame, ous_target: pl.DataFrame) -> None:
    """Check that every configured OU mapping UID exists in its instance.

    Raises ValueError if any source or target UID is missing, so a typo fails the run
    before any data is posted rather than misfiling values.
    """
    if not config.ou_mapping:
        current_run.log_info("No organisation unit mapping configured; OU UIDs pass through.")
        return

    current_run.log_info(f"Validating {len(config.ou_mapping)} organisation unit mapping(s)...")
    source_names = dict(ous_source.select(["id", "name"]).iter_rows())
    target_names = dict(ous_target.select(["id", "name"]).iter_rows())

    for label, uids, names in (
        ("source", config.ou_mapping.keys(), source_names),
        ("target", config.ou_mapping.values(), target_names),
    ):
        missing = set(uids) - set(names)
        if missing:
            msg = f"{label} organisation unit IDs in mapping not found in DHIS2: {missing}"
            current_run.log_error(msg)
            raise ValueError(msg)

    for source_uid, target_uid in config.ou_mapping.items():
        current_run.log_info(
            f"  {source_uid} ({source_names[source_uid]}) → "
            f"{target_uid} ({target_names[target_uid]})"
        )


def remap_organisation_units(data_values: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """Translate source OU UIDs to their target-instance equivalents.

    UIDs absent from config.ou_mapping are left untouched, since most OUs share the same
    UID in both instances.

    Returns
    -------
    tuple[pl.DataFrame, int]
        Data with organisation_unit_id remapped, and the number of rows remapped.
    """
    if not config.ou_mapping:
        return data_values, 0

    matched = (
        data_values.filter(pl.col("organisation_unit_id").is_in(list(config.ou_mapping)))
        .group_by("organisation_unit_id")
        .len()
        .sort("organisation_unit_id")
    )
    remapped_count = int(matched["len"].sum()) if len(matched) else 0

    if remapped_count == 0:
        current_run.log_info(
            f"OU remapping: no rows matched the {len(config.ou_mapping)} configured mapping(s)"
        )
        return data_values, 0

    current_run.log_info(f"OU remapping: {remapped_count} rows remapped to target OU UIDs")
    for source_uid, count in matched.iter_rows():
        current_run.log_info(f"  {source_uid} → {config.ou_mapping[source_uid]}: {count} rows")

    return (
        data_values.with_columns(pl.col("organisation_unit_id").replace(config.ou_mapping)),
        remapped_count,
    )


def log_datapoints_per_period(data_values: pl.DataFrame) -> None:
    """Log the number of data points that will be pushed for each period."""
    if len(data_values) == 0:
        current_run.log_info("No data points to push.")
        return

    counts = data_values.group_by("period").len().sort("period")
    current_run.log_info(f"Data points to push per period ({counts.height} periods):")
    for period, count in counts.iter_rows():
        current_run.log_info(f"  {period}: {count} data points")


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
