"""DHIS2 to DHIS2 Data Elements Pipeline.

This pipeline extracts data values from a source DHIS2 instance for a given dataset
and writes the values to a target DHIS2 instance, using mappings for dataElement and
categoryOptionCombos IDs.
"""

from datetime import datetime
from pathlib import Path
from typing import Any

import json

import polars as pl
from openhexa.sdk import (
    DHIS2Connection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.toolbox.dhis2.dataframe import get_data_elements, get_category_option_combos

import config
from api import (
    get_datasets_as_dict,
    extract_source_data,
    get_dhis2_client,
    post_to_target,
    validate_connection,
    check_datasets_associated,
    get_coc_per_des,
)
from utils import read_csv, save_outputs
from dates import get_start_end_date, get_periods_from_start_end_dates
from mappings import prepare_data_value_payload, validate_data_values


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
    "mapping_file",
    type=str,
    help="Example: Mapping SNIS-FBP - Mapping_new.csv",
    name="Mapping CSV filename",
    required=True,
    default="Mapping SNIS-FBP - Mapping_new_CDS.csv",
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
    default="2026-05-31",
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
    default=False,
    required=False,
)
def dhis2_to_dhis2_data_elements(
    source_connection: DHIS2Connection,
    target_connection: DHIS2Connection,
    mapping_file: str,
    start_date: str,
    end_date: str,
    dry_run: bool,
    use_relative_dates: bool,
    days_back: int = 365,
):
    """Extract data values from source DHIS2 and write to target DHIS2 with mappings."""
    output_dir = initialize_output_dir()
    current_run.log_info("Checking connections to source and target DHIS2...")
    source_dhis2 = get_dhis2_client(source_connection, config.use_cache)
    target_dhis2 = get_dhis2_client(target_connection, config.use_cache)
    validate_connection(source_dhis2, "Source DHIS2")
    validate_connection(target_dhis2, "Target DHIS2")

    current_run.log_info("Loading metadata from source and target DHIS2...")
    des_source = get_data_elements(source_dhis2)
    des_target = get_data_elements(target_dhis2)
    cocs_source = get_category_option_combos(source_dhis2)
    cocs_target = get_category_option_combos(target_dhis2)
    datasets_source = get_datasets_as_dict(source_dhis2)
    des_coc_target, des_coc_target_list = get_coc_per_des(target_dhis2)
    des_source.write_parquet(output_dir / "des_snis.parquet")
    des_target.write_parquet(output_dir / "des_fbp.parquet")
    cocs_source.write_parquet(output_dir / "cocs_snis.parquet")
    cocs_target.write_parquet(output_dir / "cocs_fbp.parquet")
    json.dump(
        datasets_source, (output_dir / "datasets_snis.json").open("w", encoding="utf-8"), indent=2
    )
    json.dump(
        des_coc_target_list,
        (output_dir / "des_cocs_fbp_list.json").open("w", encoding="utf-8"),
        indent=2,
    )

    current_run.log_info("Validating and loading mapping file...")
    mapping_data = read_csv(Path(workspace.files_path) / config.pipeline_input / mapping_file)
    validate_mapping_structure(mapping_data)
    validate_mapping_ids(
        mapping_data, des_source, des_target, cocs_source, cocs_target, datasets_source
    )

    start_date, end_date = get_start_end_date(
        use_relative_dates=use_relative_dates,
        days_back=days_back,
        start_date=start_date,
        end_date=end_date,
    )
    current_run.log_info(f"Extracting data for period: {start_date} to {end_date}")

    dataset_ids = mapping_data["ds_id_snis"].drop_nulls().unique().to_list()
    source_data = extract_all_dataset_data(
        source_dhis2, dataset_ids, datasets_source, start_date, end_date, output_dir
    )
    source_data.write_parquet(output_dir / "all_extracted_data.parquet")

    if len(source_data) == 0:
        current_run.log_warning("No data extracted from source DHIS2; stopping pipeline.")
        summary = generate_summary(
            source_count=0,
            filtered_count=0,
            transformed_count=0,
            coc_filtered_count=0,
            validation_dropped=0,
            dedup_dropped=0,
            post_results={"status": "no_data", "imported": 0, "updated": 0, "ignored": 0},
            dry_run=dry_run,
        )
        save_outputs(output_dir, [], {"status": "no_data"}, summary)
        return

    filtered_data = select_relevant_values(source_data, mapping_data)
    transformed_data = transform_data_values(filtered_data, mapping_data)
    transformed_count = len(transformed_data)

    validation_dropped = 0
    coc_filtered_count = 0
    dedup_dropped = 0
    if transformed_count == 0:
        current_run.log_warning("No data to post after transformation")
        post_results = {"status": "no_data", "imported": 0, "updated": 0, "ignored": 0}
        payload = []
    else:
        mode_label = (
            "DRY RUN — data will NOT be saved" if dry_run else "LIVE — data WILL be saved to DHIS2"
        )
        current_run.log_info(f"Preparing import to target DHIS2 ({mode_label})...")
        transformed_data = add_attribute_option_combo_id(transformed_data)
        transformed_data = filter_valid_cocs(transformed_data, des_coc_target)
        coc_filtered_count = len(transformed_data)
        transformed_data = validate_data_values(transformed_data, target_dhis2, des_target)
        validation_dropped = coc_filtered_count - len(transformed_data)
        data_to_post = deduplicate_data(transformed_data)
        dedup_dropped = len(transformed_data) - len(data_to_post)
        check_datasets_associated(target_dhis2, data_to_post)
        sel_data = select_some_data(data_to_post)
        sel_data.write_parquet(output_dir / "imported_data.parquet")
        payload = prepare_data_value_payload(sel_data)
        current_run.log_info(f"Sending {len(payload)} records to target DHIS2...")
        post_results = post_to_target(target_dhis2, payload, dry_run)

    summary = generate_summary(
        source_count=len(source_data),
        filtered_count=len(filtered_data),
        transformed_count=transformed_count,
        coc_filtered_count=coc_filtered_count,
        validation_dropped=validation_dropped,
        dedup_dropped=dedup_dropped,
        post_results=post_results,
        dry_run=dry_run,
    )
    save_outputs(output_dir, payload, post_results, summary)


def select_some_data(data: pl.DataFrame) -> pl.DataFrame:
    """We want to start with a test."""
    return data.filter(
        (
            pl.col("period").is_in(
                [
                    "202501",
                    "202502",
                    "202503",
                    "202504",
                    "202505",
                    "202506",
                    "202507",
                    "202508",
                    "202509",
                    "202510",
                    "202511",
                    "202512",
                ]
            )
        )
    )


def add_attribute_option_combo_id(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl.lit(config.att_default).alias("attribute_option_combo_id"))


def deduplicate_data(df: pl.DataFrame) -> pl.DataFrame:
    """Remove duplicate rows with the same DE/COC/OU/period, keeping the last value."""
    keys = ["data_element_id", "category_option_combo_id", "organisation_unit_id", "period"]
    before = len(df)
    deduped = df.unique(subset=keys, keep="last")
    removed = before - len(deduped)
    if removed > 0:
        current_run.log_warning(
            f"Removed {removed} duplicate records (same DE/COC/OU/period); keeping last value"
        )
    return deduped


def filter_valid_cocs(data: pl.DataFrame, des_coc_target: dict[str, set]) -> pl.DataFrame:
    """Filter out rows whose category option combo is not valid for the data element.

    Uses the pre-fetched des_coc_target mapping (DE ID → set of valid COC IDs) so no
    additional API calls are needed.

    Parameters
    ----------
    data : pl.DataFrame
        Transformed data values with columns data_element_id and category_option_combo_id.
    des_coc_target : dict[str, set]
        Mapping of target data element ID to its set of valid category option combo IDs.

    Returns
    -------
    pl.DataFrame
        Filtered data containing only rows with valid DE/COC combinations.
    """
    valid_pairs = pl.DataFrame(
        [
            {"data_element_id": de, "category_option_combo_id": coc}
            for de, cocs in des_coc_target.items()
            for coc in cocs
        ]
    ).unique()
    initial_count = len(data)
    filtered = data.join(
        valid_pairs, on=["data_element_id", "category_option_combo_id"], how="inner"
    )
    current_run.log_info(
        f"COC validation: {initial_count} → {len(filtered)} records kept "
        f"({initial_count - len(filtered)} removed: CoC not valid for its DE)"
    )
    return filtered


def validate_mapping_structure(mapping_data: pl.DataFrame) -> None:
    """Validate mapping CSV columns are present and fully populated. Raises ValueError if invalid."""
    missing_cols = [col for col in config.REQUIRED_ID_COLUMNS if col not in mapping_data.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in mapping file: {missing_cols}")

    for col in config.REQUIRED_ID_COLUMNS:
        empty_count = mapping_data.select(
            (
                pl.col(col).is_null()
                | (pl.col(col).cast(pl.String).str.strip_chars().str.len_chars() == 0)
            ).sum()
        ).item()
        if empty_count > 0:
            raise ValueError(f"Column '{col}' has {empty_count} empty or null values")


def _check_ids(
    mapping_data: pl.DataFrame, id_cols: list[str], dhis2_df: pl.DataFrame, label: str
) -> None:
    """Collect unique IDs from id_cols and raise if any are missing from dhis2_df."""
    ids: set[str] = set()
    for col in id_cols:
        ids.update(mapping_data[col].unique().drop_nulls().to_list())
    dhis2_ids = set(dhis2_df["id"].to_list())
    missing = ids - dhis2_ids
    if missing:
        msg = f"{label} IDs in mapping not found in DHIS2: {missing}"
        current_run.log_error(msg)
        raise ValueError(msg)


def validate_mapping_ids(
    mapping_data: pl.DataFrame,
    des_source: pl.DataFrame,
    des_target: pl.DataFrame,
    cocs_source: pl.DataFrame,
    cocs_target: pl.DataFrame,
    datasets_source: dict,
) -> None:
    """Validate all ID columns in the mapping against source and target DHIS2 metadata."""
    dataset_ids = set(mapping_data["ds_id_snis"].drop_nulls().unique().to_list())
    missing_datasets = dataset_ids - set(datasets_source.keys())
    if missing_datasets:
        msg = f"Dataset IDs in mapping not found in source DHIS2: {missing_datasets}"
        current_run.log_error(msg)
        raise ValueError(msg)

    _check_ids(mapping_data, ["de_id_snis"], des_source, "source dataElements")
    _check_ids(
        mapping_data,
        ["coc_id_snis_mfp", "coc_id_snis_cam", "coc_id_snis_fbp"],
        cocs_source,
        "source categoryOptionCombos",
    )
    _check_ids(
        mapping_data,
        ["de_id_fbp_dec"],
        des_target,
        "target dataElements",
    )
    _check_ids(
        mapping_data,
        [
            "coc_id_fbp_mfp",
            "coc_id_fbp_cam",
            "coc_id_fbp_total",
        ],
        cocs_target,
        "target categoryOptionCombos",
    )


def initialize_output_dir() -> Path:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = Path(workspace.files_path) / "pipelines" / "transfer_snis_fbp" / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def extract_dataset_data(
    source_dhis2: Any,
    dataset_id: str,
    datasets_source: dict,
    start_date: str,
    end_date: str,
    output_dir: Path,
) -> pl.DataFrame:
    period_type = datasets_source[dataset_id]["periodType"]
    current_run.log_info(f"Dataset {dataset_id}: period type {period_type}")
    periods = get_periods_from_start_end_dates(start_date, end_date, period_type)
    org_units = datasets_source[dataset_id]["organisation_units"]

    data = extract_source_data(source_dhis2, dataset_id, periods, org_units)

    if len(data) == 0:
        current_run.log_warning(f"No data extracted for dataset {dataset_id}")
    else:
        out_path = output_dir / f"{dataset_id}.parquet"
        data.write_parquet(out_path)
        current_run.add_file_output(out_path.as_posix())

    return data


def extract_all_dataset_data(
    source_dhis2: Any,
    dataset_ids: list[str],
    datasets_source: dict,
    start_date: str,
    end_date: str,
    output_dir: Path,
) -> pl.DataFrame:
    current_run.log_info(f"Extracting data for {len(dataset_ids)} datasets...")
    extracted = [
        extract_dataset_data(source_dhis2, ds_id, datasets_source, start_date, end_date, output_dir)
        for ds_id in dataset_ids
    ]
    non_empty = [df for df in extracted if len(df) > 0]
    if not non_empty:
        current_run.log_warning("No data extracted for any dataset")
    return pl.concat(non_empty) if non_empty else pl.DataFrame()


def select_relevant_values(source_data: pl.DataFrame, mapping_data: pl.DataFrame) -> pl.DataFrame:
    """Filter source data to only DE+COC pairs present in the mapping.

    Returns
    --------
    pl.DataFrame
        Filtered data containing only rows with dataElement and categoryOptionCombo combinations
        that are defined in the mapping file.
    """
    valid_pairs = pl.concat(
        [
            mapping_data.select(
                pl.col("de_id_snis").alias("data_element_id"),
                pl.col("coc_id_snis_mfp").alias("category_option_combo_id"),
            ),
            mapping_data.select(
                pl.col("de_id_snis").alias("data_element_id"),
                pl.col("coc_id_snis_cam").alias("category_option_combo_id"),
            ),
            mapping_data.select(
                pl.col("de_id_snis").alias("data_element_id"),
                pl.col("coc_id_snis_fbp").alias("category_option_combo_id"),
            ),
        ]
    ).unique()

    initial_count = len(source_data)
    filtered = source_data.join(
        valid_pairs, on=["data_element_id", "category_option_combo_id"], how="inner"
    )
    current_run.log_info(f"Relevant values: {initial_count} → {len(filtered)} records")
    return filtered


def aggregate_cocs(data: pl.DataFrame) -> pl.DataFrame:
    return (
        data.drop_nulls("value")
        .group_by(["organisation_unit_id", "period", "data_element_id", "dataset_id_snis"])
        .agg(pl.col("value").sum().alias("de_total_snis"))
    )


def transform_data_values(filtered_data: pl.DataFrame, mapping_data: pl.DataFrame) -> pl.DataFrame:
    current_run.log_info("Transforming data values...")
    totals = aggregate_cocs(filtered_data)

    dec_total_mapping = mapping_data.select(
        ["ds_id_snis", "de_id_snis", "de_id_fbp_dec", "coc_id_fbp_total"]
    ).unique()
    dec_mfp_mapping = mapping_data.select(
        ["ds_id_snis", "de_id_snis", "coc_id_snis_mfp", "de_id_fbp_dec", "coc_id_fbp_mfp"]
    ).unique()
    dec_cam_mapping = mapping_data.select(
        ["ds_id_snis", "de_id_snis", "coc_id_snis_cam", "de_id_fbp_dec", "coc_id_fbp_cam"]
    ).unique()

    dec_rows = totals.join(
        dec_total_mapping,
        left_on=["dataset_id_snis", "data_element_id"],
        right_on=["ds_id_snis", "de_id_snis"],
        how="inner",
    ).select(
        [
            pl.col("de_id_fbp_dec").alias("data_element_id"),
            pl.col("coc_id_fbp_total").alias("category_option_combo_id"),
            "organisation_unit_id",
            "period",
            pl.col("de_total_snis").alias("value"),
        ]
    )

    mfp_rows = filtered_data.join(
        dec_mfp_mapping,
        left_on=["dataset_id_snis", "data_element_id", "category_option_combo_id"],
        right_on=["ds_id_snis", "de_id_snis", "coc_id_snis_mfp"],
        how="inner",
    ).select(
        [
            pl.col("de_id_fbp_dec").alias("data_element_id"),
            pl.col("coc_id_fbp_mfp").alias("category_option_combo_id"),
            "organisation_unit_id",
            "period",
            "value",
        ]
    )

    cam_rows = filtered_data.join(
        dec_cam_mapping,
        left_on=["dataset_id_snis", "data_element_id", "category_option_combo_id"],
        right_on=["ds_id_snis", "de_id_snis", "coc_id_snis_cam"],
        how="inner",
    ).select(
        [
            pl.col("de_id_fbp_dec").alias("data_element_id"),
            pl.col("coc_id_fbp_cam").alias("category_option_combo_id"),
            "organisation_unit_id",
            "period",
            "value",
        ]
    )

    result = pl.concat([dec_rows, mfp_rows, cam_rows])
    current_run.log_info(f"Transformation complete: {len(result)} target records")
    current_run.log_info(f"- {len(dec_rows)} from DEC (total) mapping")
    current_run.log_info(f"- {len(mfp_rows)} from MFP mapping")
    current_run.log_info(f"- {len(cam_rows)} from CAM mapping")
    return result


def generate_summary(
    source_count: int,
    filtered_count: int,
    transformed_count: int,
    coc_filtered_count: int,
    validation_dropped: int,
    dedup_dropped: int,
    post_results: dict[str, Any],
    dry_run: bool,
) -> dict[str, Any]:
    """Build, log, and return the pipeline execution summary."""
    status = post_results.get("status", "UNKNOWN")
    conflicts = post_results.get("conflicts", [])
    failed_http_chunks = post_results.get("failed_http_chunks", [])
    conflict_chunks = post_results.get("conflict_chunks", [])

    summary = {
        "pipeline": "dhis2_to_dhis2_data_elements",
        "dry_run": dry_run,
        "extraction": {
            "source_records": source_count,
            "relevant_records": filtered_count,
            "transformed_records": transformed_count,
            "coc_filtered_records": coc_filtered_count,
            "validation_dropped": validation_dropped,
            "dedup_dropped": dedup_dropped,
        },
        "import": {
            "status": status,
            "imported": post_results.get("imported", 0),
            "updated": post_results.get("updated", 0),
            "ignored": post_results.get("ignored", 0),
            "deleted": post_results.get("deleted", 0),
            "conflicts_count": len(conflicts),
            "conflicts": conflicts,
            "failed_http_chunks": failed_http_chunks,
            "conflict_chunks": conflict_chunks,
        },
    }

    records_sent = coc_filtered_count - validation_dropped - dedup_dropped
    imported = summary["import"]["imported"]
    updated = summary["import"]["updated"]
    ignored = summary["import"]["ignored"]

    current_run.log_info("=== PIPELINE SUMMARY ===")
    current_run.log_info(
        f"Mode: {'DRY RUN — data was NOT saved to DHIS2' if dry_run else 'LIVE — data WAS saved to DHIS2'}"
    )
    current_run.log_info(f"Records extracted from source DHIS2: {source_count}")
    current_run.log_info(f"Records matched to mapping file: {filtered_count}")

    not_in_mapping = source_count - filtered_count
    if not_in_mapping > 0:
        current_run.log_info(
            f"  → {not_in_mapping} records not in mapping file (expected, skipped)"
        )

    current_run.log_info(
        f"Records after transformation (target DE/COC mapping): {transformed_count}"
    )

    coc_removed = transformed_count - coc_filtered_count
    if coc_removed > 0:
        current_run.log_warning(
            f"  → {coc_removed} records dropped: invalid indicator/category combination for target"
        )

    if validation_dropped > 0:
        current_run.log_warning(
            f"  → {validation_dropped} records dropped: failed value validation"
        )

    if dedup_dropped > 0:
        current_run.log_warning(
            f"  → {dedup_dropped} duplicate records removed (same DE/COC/OU/period)"
        )

    current_run.log_info(f"Records sent to target DHIS2: {records_sent}")
    current_run.log_info("--- Import results ---")
    current_run.log_info(f"  New records created: {imported}")
    current_run.log_info(f"  Existing records updated: {updated}")

    if ignored > 0:
        current_run.log_warning(
            f"  Records ignored by DHIS2: {ignored} ({len(conflicts)} conflict(s) — see details above)"
        )

    if failed_http_chunks:
        current_run.log_error(
            f"  Batches that failed to reach DHIS2: {len(failed_http_chunks)} (data may be incomplete)"
        )

    if status in ("FAILURE", "PARTIAL_FAILURE"):
        current_run.log_error(f"Import result: {status}")
    elif status == "SUCCESS_WITH_CONFLICTS":
        current_run.log_warning(f"Import result: {status}")
    else:
        current_run.log_info(f"Import result: {status}")

    current_run.log_info("========================")
    return summary


if __name__ == "__main__":
    dhis2_to_dhis2_data_elements()
