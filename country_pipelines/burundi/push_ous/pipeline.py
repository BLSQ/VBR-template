"""Pipeline to push new organisation units (Gasc) from a CSV file into a DHIS2 instance."""

import json
import logging
import os
from datetime import datetime

import polars as pl
import pandas as pd
from openhexa.sdk import DHIS2Connection, current_run, parameter, pipeline, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_organisation_units

from pyramid_matcher.matchers import FuzzyMatcher
from pyramid_matcher.pyramid_matcher import PyramidMatcher

import config
from org_unit_aligner import DHIS2PyramidAligner


@pipeline("push_ous")
@parameter(
    "file_path",
    type=str,
    help="Path to the CSV file (relative to the workspace files path)",
    required=True,
    default="pipelines/push_ous/gasc2.csv",
)
@parameter(
    "dhis_con",
    type=DHIS2Connection,
    help="Connection to the DHIS2 instance",
    default="pbf-burundi",
    required=True,
)
@parameter(
    "dry_run",
    type=bool,
    help="If True, no changes will be applied to DHIS2",
    default=False,
    required=True,
)
@parameter(
    "delete_or_import",
    type=str,
    help="If 'delete', delete existing org units before importing new ones. If 'import', import new org units without deleting.",
    default="delete",
    required=True,
    choices=["delete", "import"],
)
@parameter(
    "async_delete",
    type=bool,
    help="If True, deletes org units via a single async metadata request (faster, avoids 504). If False, deletes one by one.",
    default=True,
    required=False,
)
def push_ous(file_path, dhis_con, dry_run, delete_or_import, async_delete):
    """Push new Gasc organisation units from a CSV file into a DHIS2 instance.

    For each row in the CSV, the org unit in column D (Gasc) is created under
    the parent in column C (fosa). Parents are matched by name against the
    existing DHIS2 pyramid.
    """
    dhis = get_dhis2(dhis_con)
    target_pyramid = fetch_dhis2_pyramid(dhis)
    df = read_source_csv(file_path)
    if delete_or_import == "import":
        df_mod = change_names_before_match(df)
        df_matched, df_unmatched = match_parent_ids(df_mod, target_pyramid)
        uids = generate_uids(dhis, len(df_matched))
        df_matched = create_short_names(df_matched)
        source_pyramid = build_source_pyramid(df_matched, uids)
        aligner = create_aligner()
        aligner.align_to(target_dhis2=dhis, source_pyramid=source_pyramid, dry_run=dry_run)
        log_summary(aligner)
        save_outputs(source_pyramid, df_unmatched, aligner)
    elif delete_or_import == "delete":
        ous_to_delete = select_ous_to_delete(target_pyramid)
        ous_to_delete_filtered = filter_ous_to_delete(ous_to_delete, df)
        delete_ous(dhis, ous_to_delete_filtered, dry_run, async_delete)
    else:
        current_run.log_error(f"Invalid value for delete_or_import: {delete_or_import}")


def filter_ous_to_delete(ous_to_delete: pl.DataFrame, df: pl.DataFrame) -> pl.DataFrame:
    """Filter the org units to delete to only those that are present in the source CSV.

    This is a safety measure to prevent deleting org units that are not intended to be replaced.

    Parameters
    ----------
    ous_to_delete : pl.DataFrame
        DataFrame with columns id, name of org units to delete.
    df : pl.DataFrame
        Source CSV data with columns Province, fosa, Gasc.

    Returns
    -------
    pl.DataFrame
        Filtered DataFrame with columns id, name of org units to delete.
    """
    gasc_names_in_csv = df.select(pl.col("Gasc")).unique().to_series().to_list()
    ous_to_delete_filtered = ous_to_delete.filter(pl.col("name").is_in(gasc_names_in_csv))
    current_run.log_info(
        f"Filtered org units to delete from {len(ous_to_delete)} to {len(ous_to_delete_filtered)} based on presence in the CSV."
    )
    missing_in_dhis2 = set(gasc_names_in_csv) - set(ous_to_delete_filtered["name"].to_list())
    if len(missing_in_dhis2) > 0:
        current_run.log_warning(
            f"{len(missing_in_dhis2)} org unit(s) present in the CSV were not found in DHIS2 and will not be deleted: "
            + ", ".join(missing_in_dhis2)
        )
    return ous_to_delete_filtered


def select_ous_to_delete(target_pyramid: pl.DataFrame) -> pl.DataFrame:
    """Select the ous to delete (Level 6, start with 'GASC')

    Parameters
    ----------
    target_pyramid : pl.DataFrame
        DHIS2 pyramid with columns: id, name, level, level_{n}_id, level_{n}_name, geometry.

    Returns
    -------
    pl.DataFrame
        DataFrame with columns id, name of org units to delete.
    """
    ous_to_delete = target_pyramid.filter(
        (pl.col("level") == 6) & pl.col("name").str.starts_with(config.GASC_NAME_PREFIX)
    ).select(["id", "name"])
    current_run.log_info(f"Selected {len(ous_to_delete)} org units to delete.")
    return ous_to_delete


def delete_ous(
    dhis: DHIS2, ous_to_delete: pl.DataFrame, dry_run: bool, async_delete: bool = False
) -> None:
    """Delete org units from DHIS2.

    Parameters
    ----------
    dhis : DHIS2
        Connected DHIS2 client.
    ous_to_delete : pl.DataFrame
        DataFrame with columns id, name of org units to delete.
    dry_run : bool
        If True, do not actually delete, just log what would be deleted.
    async_delete : bool
        If True, uses async bulk metadata delete instead of one-by-one.
    """
    aligner = create_aligner()
    ou_ids = ous_to_delete["id"].to_list()
    aligner.delete_org_units(
        target_dhis2=dhis, ou_ids=ou_ids, dry_run=dry_run, async_delete=async_delete
    )
    log_summary(aligner)


def change_names_before_match(df: pl.DataFrame) -> pl.DataFrame:
    """Apply any necessary transformations to the source DataFrame before matching.

    This can include trimming whitespace, standardizing case, or applying known
    corrections to names that are expected to differ between the source and
    target.

    Parameters
    ----------
    df : pl.DataFrame
        Original DataFrame read from the CSV file.

    Returns
    -------
    pl.DataFrame
        Modified DataFrame with cleaned/standardized names for better matching.
    """
    for level_2, changes in config.level_5_changes.items():
        for [old_name, new_name] in changes:
            df = df.with_columns(
                pl.when((pl.col("Province") == level_2) & (pl.col("fosa") == old_name))
                .then(pl.lit(new_name))
                .otherwise(pl.col("fosa"))
                .alias("fosa")
            )

    return df


def get_dhis2(con_oh: DHIS2Connection) -> DHIS2:
    """Initialise and return a DHIS2 client.

    Parameters
    ----------
    con_oh : DHIS2Connection
        OpenHexa connection object for the DHIS2 instance.

    Returns
    -------
    DHIS2
        Connected DHIS2 client.
    """
    return DHIS2(con_oh)


def read_source_csv(file_path: str) -> pl.DataFrame:
    """Read the source CSV file containing the Gasc org units to create.

    Parameters
    ----------
    file_path : str
        Path to the CSV file, relative to the workspace files path.

    Returns
    -------
    pl.DataFrame
        DataFrame with columns: Nouvelle_province, Province, fosa, Gasc.
    """
    full_path = f"{workspace.files_path}/{file_path}"
    current_run.log_info(f"Reading source CSV from {full_path}")
    df = pl.read_csv(full_path)
    current_run.log_info(f"Loaded {len(df)} rows from the CSV file.")
    return df


def fetch_dhis2_pyramid(dhis: DHIS2) -> pl.DataFrame:
    """Fetch organisation unit metadata from DHIS2.

    Parameters
    ----------
    dhis : DHIS2
        Connected DHIS2 client.

    Returns
    -------
    pl.DataFrame
        DataFrame with columns: id, name, level, level_{n}_id, level_{n}_name, geometry.
    """
    current_run.log_info("Fetching the DHIS2 organisation unit pyramid...")
    df = get_organisation_units(dhis)
    current_run.log_info(f"Fetched {len(df)} organisation units from DHIS2.")
    return df


def match_parent_ids(
    df: pl.DataFrame, target_pyramid: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Match fosa names (column C) to their DHIS2 IDs using the target pyramid.

    Rows whose parent name cannot be matched are split out and logged as a warning.

    Parameters
    ----------
    df : pl.DataFrame
        Source CSV data with columns Province, fosa, Gasc.
    target_pyramid : pl.DataFrame
        DHIS2 pyramid with at least level_2_name, level_5_name, level_5_id columns.

    Returns
    -------
    tuple[pl.DataFrame, pl.DataFrame]
        (df_matched, df_unmatched): matched rows contain level_5_id as parent ID;
        unmatched rows are candidate entries that could not be paired.
    """
    reference_pyramid = target_pyramid.select(
        ["level_2_name", "level_5_name", "level_5_id"]
    ).unique()
    candidate_pyramid = df.select(["Province", "fosa", "Gasc"]).rename(
        {"Province": "level_2_name", "fosa": "level_5_name", "Gasc": "level_5_gasc"}
    )

    matcher = FuzzyMatcher(threshold=94, scorer_name="wratio")
    pyramid_matcher = PyramidMatcher(matcher)
    matched_data, matched_data_simplified, reference_not_matched, candidate_not_matched = (
        pyramid_matcher.run_matching(
            reference_pyramid=reference_pyramid,
            candidate_pyramid=candidate_pyramid,
        )
    )

    if len(candidate_not_matched) > 0:
        unmatched = candidate_not_matched["level_5_name"].to_list()
        current_run.log_warning(
            f"{len(unmatched)} parent name(s) could not be matched in DHIS2 and will be skipped: "
            + ", ".join(unmatched)
        )

    current_run.log_info(
        f"{len(matched_data_simplified)} org units have a matched parent and will be processed."
    )
    return matched_data_simplified, candidate_not_matched


def generate_uids(dhis: DHIS2, n: int) -> list[str]:
    """Generate n new UIDs from the DHIS2 instance.

    Parameters
    ----------
    dhis : DHIS2
        Connected DHIS2 client.
    n : int
        Number of UIDs to generate.

    Returns
    -------
    list[str]
        List of n unique DHIS2 UIDs.
    """
    current_run.log_info(f"Generating {n} new UIDs from DHIS2...")
    response = dhis.api.get("system/id", params={"limit": n})
    return response["codes"]


def create_short_names(df: pl.DataFrame) -> pl.DataFrame:
    """Create shortName values by stripping the prefix and truncating.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame with a column candidate_level_5_gasc containing the full names.

    Returns
    -------
    pl.DataFrame
        DataFrame with an additional column shortName derived from candidate_level_5_gasc.
    """
    return df.with_columns(
        pl.col("candidate_level_5_gasc")
        .str.strip_prefix(config.GASC_NAME_PREFIX)
        .str.slice(0, config.SHORT_NAME_MAX_LENGTH)
        .alias("shortName")
    )


def build_source_pyramid(df_matched: pl.DataFrame, uids: list[str]):
    """Build the source pyramid DataFrame required by DHIS2PyramidAligner.

    Each Gasc org unit is assigned a new UID. The name comes from candidate_level_5_gasc,
    the shortName is the name minus the "GASC - " prefix, and the parent ID
    comes from level_5_id.

    Parameters
    ----------
    df_matched : pl.DataFrame
        Matched rows with columns: candidate_level_5_gasc, level_5_id (and others).
    uids : list[str]
        Pre-generated DHIS2 UIDs, one per row.

    Returns
    -------
    pandas DataFrame
        DataFrame with columns: id, name, shortName, openingDate, closedDate,
        parent, level, path, geometry — ready for DHIS2PyramidAligner.align_to().
    """
    source = df_matched.with_columns(pl.Series("id", uids)).select(
        [
            pl.col("id"),
            pl.col("candidate_level_5_gasc").alias("name"),
            pl.col("shortName"),
            pl.lit(config.OU_OPENING_DATE).alias("openingDate"),
            pl.lit(None).cast(pl.Utf8).alias("closedDate"),
            pl.struct(pl.col("reference_level_5_id").alias("id")).alias("parent"),
            pl.lit(6).cast(pl.Int32).alias("level"),
            pl.lit(None).cast(pl.Utf8).alias("path"),
            pl.lit(None).cast(pl.Utf8).alias("geometry"),
        ]
    )

    current_run.log_info(f"Built source pyramid with {len(source)} org units to create.")
    return source.to_pandas()


def create_aligner() -> DHIS2PyramidAligner:
    """Instantiate a DHIS2PyramidAligner with a standard logger.

    Returns
    -------
    DHIS2PyramidAligner
        Aligner instance ready for use.
    """
    logger = logging.getLogger(__name__)
    return DHIS2PyramidAligner(logger=logger)


def log_summary(aligner: DHIS2PyramidAligner) -> None:
    """Log the create and delete summary from the aligner.

    Parameters
    ----------
    aligner : DHIS2PyramidAligner
        Aligner instance after align_to() or delete_org_units() has been called.
    """
    summary = aligner.summary

    if summary["CREATE"]["CREATE_COUNT"] > 0 or summary["CREATE"]["ERROR_COUNT"] > 0:
        current_run.log_info(
            f"CREATE: {summary['CREATE']['CREATE_COUNT']} succeeded, "
            f"{summary['CREATE']['ERROR_COUNT']} failed."
        )
        if summary["CREATE"]["ERROR_COUNT"] > 0:
            current_run.log_error(f"Errors during creation: {summary['CREATE']['ERROR_DETAILS']}")

    if summary["INVALID"]["INVALID_COUNT"] > 0:
        current_run.log_warning(
            f"INVALID: {summary['INVALID']['INVALID_COUNT']} org units were invalid and skipped."
        )

    if summary["DELETE"]["DELETE_COUNT"] > 0 or summary["DELETE"]["ERROR_COUNT"] > 0:
        current_run.log_info(
            f"DELETE: {summary['DELETE']['DELETE_COUNT']} succeeded, "
            f"{summary['DELETE']['ERROR_COUNT']} failed."
        )
        if summary["DELETE"]["ERROR_COUNT"] > 0:
            current_run.log_error(f"Errors during deletion: {summary['DELETE']['ERROR_DETAILS']}")


def save_outputs(source_pyramid, df_unmatched: pl.DataFrame, aligner: DHIS2PyramidAligner) -> None:
    """Save three output files for the run into a timestamped folder.

    Files written:
      - imported_orgunits.csv     : org units successfully created in DHIS2
      - non_imported_orgunits.csv : org units skipped (unmatched parent, invalid, or error)
      - import_results.json       : full aligner summary with counts and API responses

    Parameters
    ----------
    source_pyramid : pandas DataFrame
        All org units that were attempted, with columns id, name, shortName, parent.
    df_unmatched : pl.DataFrame
        Candidate rows whose fosa parent was not matched in DHIS2.
    aligner : DHIS2PyramidAligner
        Aligner instance after align_to() has been called.
    """
    run_dir = os.path.join(
        workspace.files_path,
        "pipelines",
        "push_ous",
        "output",
        datetime.now().strftime("%Y%m%d_%H%M%S"),
    )
    os.makedirs(run_dir, exist_ok=True)

    summary = aligner.summary
    successful_ids = {d["RESPONSE"]["ou_id"] for d in summary["CREATE"]["CREATE_DETAILS"]}
    error_ids = {d["ou_id"] for d in summary["CREATE"]["ERROR_DETAILS"]}
    invalid_ids = set(source_pyramid["id"]) - successful_ids - error_ids

    source_pyramid = source_pyramid.copy()
    source_pyramid["parent_id"] = source_pyramid["parent"].apply(
        lambda p: p.get("id") if isinstance(p, dict) else None
    )
    output_cols = ["id", "name", "shortName", "parent_id"]

    # (a) successfully imported org units
    imported = source_pyramid[source_pyramid["id"].isin(successful_ids)][output_cols]
    imported.to_csv(os.path.join(run_dir, "imported_orgunits.csv"), index=False)

    # (b) non-imported: errors + invalids from attempted OUs, plus unmatched from CSV
    not_pushed = source_pyramid[source_pyramid["id"].isin(error_ids | invalid_ids)][
        output_cols
    ].copy()
    not_pushed["reason"] = not_pushed["id"].apply(
        lambda i: "create_error" if i in error_ids else "invalid"
    )

    if df_unmatched is not None and len(df_unmatched) > 0:
        unmatched_rows = (
            df_unmatched.select(["level_5_gasc", "level_5_name"])
            .rename({"level_5_gasc": "name", "level_5_name": "parent_id"})
            .with_columns(
                [
                    pl.lit(None).cast(pl.Utf8).alias("id"),
                    pl.lit(None).cast(pl.Utf8).alias("shortName"),
                    pl.lit("parent_not_found").alias("reason"),
                ]
            )
            .select(output_cols + ["reason"])
            .to_pandas()
        )
        not_pushed = pd.concat([not_pushed, unmatched_rows], ignore_index=True)

    not_pushed.to_csv(os.path.join(run_dir, "non_imported_orgunits.csv"), index=False)

    # (c) full import results
    with open(os.path.join(run_dir, "import_results.json"), "w") as f:
        json.dump(summary, f, indent=2)

    current_run.log_info(f"Outputs saved to {run_dir}")


if __name__ == "__main__":
    push_ous()
