"""DHIS2 to DHIS2 Data Elements Pipeline.

This pipeline extracts data values from a source DHIS2 instance for a given dataset
and writes the values to a target DHIS2 instance, using mappings for dataElement,
categoryOptionCombos, and attributeOptionCombos IDs.
"""

from pathlib import Path
from typing import Any, Literal

import polars as pl
from openhexa.sdk import (
    DHIS2Connection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.sdk.pipelines.parameter import DHIS2Widget
from openhexa.toolbox.dhis2.dataframe import get_data_elements, get_category_option_combos

import config
from api import (
    get_dataset_org_units,
    get_datasets_as_dict,
    extract_source_data,
    get_dhis2_client,
    post_to_target,
    validate_connection,
    check_datasets_associated,
)
from utils import read_json_file, save_outputs
from dates import get_start_end_date, get_periods_from_start_end_dates
from mappings import prepare_data_value_payload


@pipeline("dhis2_to_dhis2_data_elements")
@parameter(
    "source_connection",
    type=DHIS2Connection,
    name="Source DHIS2 Connection",
    required=True,
    default="snis",
)
@parameter(
    "target_connection",
    type=DHIS2Connection,
    name="Target DHIS2 Connection",
    required=True,
    default="pbf-burundi",
)
@parameter(
    "dataset_id",
    type=str,
    widget=DHIS2Widget.DATASETS,
    connection="source_connection",
    help="In the source connection",
    name="Dataset ID",
    required=True,
    default="KxHklDfg96M",
)
@parameter(
    "mapping_file",
    type=str,
    help="Example: mapping_bdi_snis_pbf_HC.json",
    name="Mapping JSON filename",
    required=True,
    default="mapping_bdi_snis_pbf_HC.json",
)
@parameter(
    "start_date",
    type=str,
    name="Start Date (YYYY-MM-DD)",
    required=False,
    default="2025-01-01",
)
@parameter(
    "end_date",
    type=str,
    name="End Date (YYYY-MM-DD)",
    required=False,
    default="2025-12-31",
)
@parameter(
    "use_relative_dates",
    type=bool,
    name="Use Relative Dates",
    help="Calculate date range relative to today instead of using fixed start and end dates",
    default=False,
    required=False,
)
@parameter(
    "days_back",
    type=int,
    name="Days Back (if relative dates)",
    help="Number of days to go back from today for start date (when using relative dates)",
    default=365,
    required=False,
)
@parameter(
    "dry_run",
    type=bool,
    name="Dry Run Mode",
    default=True,
    required=False,
)
def dhis2_to_dhis2_data_elements(
    source_connection: DHIS2Connection,
    target_connection: DHIS2Connection,
    dataset_id: str,
    mapping_file: str,
    start_date: str,
    end_date: str,
    dry_run: bool,
    use_relative_dates: bool,
    days_back: int = 365,
):
    """Extract data values from source DHIS2 and write to target DHIS2 with mappings."""
    current_run.log_info("Validating DHIS2 connections...")
    source_dhis2 = get_dhis2_client(source_connection, config.use_cache)
    target_dhis2 = get_dhis2_client(target_connection, config.use_cache)
    validate_connection(source_dhis2, "Source DHIS2")
    validate_connection(target_dhis2, "Target DHIS2")

    current_run.log_info("Extracting the necessary data from DHIS2...")
    des_source = get_data_elements(source_dhis2)
    des_target = get_data_elements(target_dhis2)
    cocs_source = get_category_option_combos(source_dhis2)
    cocs_target = get_category_option_combos(target_dhis2)
    dataset_org_units_source = get_dataset_org_units(source_dhis2, dataset_id)
    datasets_source = get_datasets_as_dict(source_dhis2)

    current_run.log_info("Validating and loading mapping file...")
    mapping_data = read_json_file(Path(workspace.files_path) / config.pipeline_input / mapping_file)
    validate_mapping_structure(mapping_data)
    for df, key, side in [
        (des_source, "dataElements", "keys"),
        (des_target, "dataElements", "values"),
        (cocs_source, "categoryOptionCombos", "keys"),
        (cocs_target, "categoryOptionCombos", "values"),
        (cocs_source, "attributeOptionCombos", "keys"),
        (cocs_target, "attributeOptionCombos", "values"),
    ]:
        validate_ids(df, mapping_data, key, side)  # type: ignore[arg-type]

    current_run.log_info("Dealing with the extraction periods...")
    start_date, end_date = get_start_end_date(
        use_relative_dates=use_relative_dates,
        days_back=days_back,
        start_date=start_date,
        end_date=end_date,
    )
    period_type = datasets_source[dataset_id]["periodType"]
    current_run.log_info(f"Dataset period type: {period_type}")
    periods_extraction = get_periods_from_start_end_dates(start_date, end_date, period_type)

    source_data = extract_source_data(
        source_dhis2,
        dataset_id,
        periods_extraction,
        dataset_org_units_source,
    )

    transformed_data, transform_stats = transform_data_values(source_data, mapping_data)

    if len(transformed_data) == 0:
        current_run.log_warning("No data to post after transformation")
        post_results = {"status": "no_data", "imported": 0, "updated": 0, "ignored": 0}
        payload = []
    else:
        current_run.log_info(f"Posting data to target DHIS2 (dry_run={dry_run})...")
        payload = prepare_data_value_payload(transformed_data, target_dhis2)
        current_run.log_info(f"Prepared {len(payload)} data values for posting")
        check_datasets_associated(target_dhis2, payload)
        post_results = post_to_target(target_dhis2, payload, dry_run)

    summary = generate_summary(transform_stats, post_results, dry_run)
    save_outputs(
        dataset_id,
        des_source,
        des_target,
        cocs_source,
        cocs_target,
        dataset_org_units_source,
        datasets_source,
        source_data,
        payload,
        post_results,
        summary,
    )


def validate_mapping_structure(mapping_data: dict[str, Any]) -> None:
    """Validate mapping file structure. Raises ValueError if invalid."""
    required_keys = {"dataElements", "categoryOptionCombos", "attributeOptionCombos"}
    present_keys = set(mapping_data.keys())
    missing_keys = required_keys - present_keys
    extra_keys = present_keys - required_keys

    if missing_keys:
        raise ValueError(f"Missing required keys in mapping file: {missing_keys}")
    if extra_keys:
        current_run.log_warning(f"Extra keys in mapping file that will be ignored: {extra_keys}")

    for key in required_keys:
        if not isinstance(mapping_data[key], dict):
            raise ValueError(f"Key '{key}' must be a dictionary")


def apply_data_mappings(
    data_values: pl.DataFrame, mapping: dict[str, dict[str, str]]
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Transform data values using mappings.

    Returns:
        tuple[pl.DataFrame, dict[str, Any]]: Transformed data and statistics.
    """
    original_count = len(data_values)
    stats = {
        "original_count": original_count,
        "mapped_data_elements": 0,
        "mapped_category_option_combos": 0,
        "unmapped_data_elements": 0,
        "unmapped_category_option_combos": 0,
        "mapped_attribute_option_combos": 0,
        "unmapped_attribute_option_combos": 0,
        "final_count": 0,
    }

    de_mapping = mapping.get("dataElements", {})
    if de_mapping:
        count_before = len(data_values)
        data_values = data_values.filter(pl.col("data_element_id").is_in(list(de_mapping.keys())))
        stats["mapped_data_elements"] = len(data_values)
        stats["unmapped_data_elements"] = count_before - len(data_values)
        data_values = data_values.with_columns(
            pl.col("data_element_id").map_elements(
                lambda x: de_mapping.get(x, x), return_dtype=pl.Utf8
            )
        )

    coc_mapping = mapping.get("categoryOptionCombos", {})
    if coc_mapping and "category_option_combo_id" in data_values.columns:
        count_before = len(data_values)
        data_values = data_values.filter(
            pl.col("category_option_combo_id").is_in(list(coc_mapping.keys()))
        )
        stats["mapped_category_option_combos"] = len(data_values)
        stats["unmapped_category_option_combos"] = count_before - len(data_values)
        data_values = data_values.with_columns(
            pl.col("category_option_combo_id").map_elements(
                lambda x: coc_mapping.get(x, x), return_dtype=pl.Utf8
            )
        )

    aoc_mapping = mapping.get("attributeOptionCombos", {})
    if aoc_mapping and "attribute_option_combo_id" in data_values.columns:
        count_before = len(data_values)
        data_values = data_values.filter(
            pl.col("attribute_option_combo_id").is_in(list(aoc_mapping.keys()))
        )
        stats["mapped_attribute_option_combos"] = len(data_values)
        stats["unmapped_attribute_option_combos"] = count_before - len(data_values)
        data_values = data_values.with_columns(
            pl.col("attribute_option_combo_id").map_elements(
                lambda x: aoc_mapping.get(x, x), return_dtype=pl.Utf8
            )
        )

    stats["final_count"] = len(data_values)
    return data_values, stats


def validate_ids(
    source_data: pl.DataFrame,
    mapping_data_all: dict[str, dict[str, str]],
    key: Literal["dataElements", "categoryOptionCombos", "attributeOptionCombos"],
    keys_or_values: Literal["keys", "values"],
):
    """Validate that IDs in mapping file exist in source DHIS2 data. Raises ValueError if any IDs are missing."""
    if keys_or_values == "keys":
        mapping_ids = set(mapping_data_all.get(key, {}).keys())
        dhis2_type = "source"
    else:
        mapping_ids = set(mapping_data_all.get(key, {}).values())
        dhis2_type = "target"

    source_ids = set(source_data.select("id").to_series().to_list())
    missing_source = mapping_ids - source_ids
    if missing_source:
        msg = f"{key} IDs in mapping file not found in DHIS2 {dhis2_type}: {missing_source}"
        current_run.log_error(msg)
        raise ValueError(msg)


def transform_data_values(
    data_values: pl.DataFrame,
    mapping_data: dict[str, dict[str, str]],
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Transform data values using mappings.

    Returns:
        tuple[pl.DataFrame, dict[str, Any]]: Transformed data and statistics.
    """
    current_run.log_info("Transforming data values...")

    transformed_data, stats = apply_data_mappings(data_values, mapping_data)
    current_run.log_info("✓ Transformation completed")
    current_run.log_info(f"  - Original records: {stats['original_count']}")
    current_run.log_info(
        f"  - Final records (after unmapped elements filtered): {stats['final_count']}"
    )
    current_run.log_info(f"  - Mapped data elements: {stats['mapped_data_elements']}")
    current_run.log_info(f"  - Unmapped data elements: {stats['unmapped_data_elements']}")
    current_run.log_info(
        f"  - Mapped category option combos: {stats['mapped_category_option_combos']}"
    )
    current_run.log_info(
        f"  - Unmapped category option combos: {stats['unmapped_category_option_combos']}"
    )
    current_run.log_info(
        f"  - Mapped attribute option combos: {stats['mapped_attribute_option_combos']}"
    )
    current_run.log_info(
        f"  - Unmapped attribute option combos: {stats['unmapped_attribute_option_combos']}"
    )
    return transformed_data, stats


def generate_summary(
    transform_stats: dict[str, Any],
    post_results: dict[str, Any],
    dry_run: bool,
) -> dict[str, Any]:
    """Build, log, and return the pipeline execution summary."""
    summary = {
        "pipeline": "dhis2_to_dhis2_data_elements",
        "dry_run": dry_run,
        "extraction": {
            "original_records": transform_stats.get("original_count", 0),
            "final_records": transform_stats.get("final_count", 0),
        },
        "transformation": {
            "mapped_data_elements": transform_stats.get("mapped_data_elements", 0),
            "unmapped_data_elements": transform_stats.get("unmapped_data_elements", 0),
            "mapped_category_option_combos": transform_stats.get(
                "mapped_category_option_combos", 0
            ),
            "unmapped_category_option_combos": transform_stats.get(
                "unmapped_category_option_combos", 0
            ),
            "mapped_attribute_option_combos": transform_stats.get(
                "mapped_attribute_option_combos", 0
            ),
            "unmapped_attribute_option_combos": transform_stats.get(
                "unmapped_attribute_option_combos", 0
            ),
        },
        "import": {
            "imported": post_results.get("imported", 0),
            "updated": post_results.get("updated", 0),
            "ignored": post_results.get("ignored", 0),
            "conflicts": post_results.get("conflicts", []),
        },
    }

    current_run.log_info("=== PIPELINE SUMMARY ===")
    current_run.log_info(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    current_run.log_info(f"Extracted: {summary['extraction']['original_records']} records")
    current_run.log_info(f"Transformed: {summary['extraction']['final_records']} records")
    current_run.log_info(f"Imported: {summary['import']['imported']} records")
    current_run.log_info(f"Updated: {summary['import']['updated']} records")
    current_run.log_info(f"Ignored: {summary['import']['ignored']} records")
    current_run.log_info("========================")
    return summary


if __name__ == "__main__":
    dhis2_to_dhis2_data_elements()
