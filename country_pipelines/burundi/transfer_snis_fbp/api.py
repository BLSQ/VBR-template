import re
from pathlib import Path
from typing import Any

import polars as pl
import requests
from openhexa.sdk import DHIS2Connection, current_run
from openhexa.toolbox.dhis2 import DHIS2

import config


def get_dhis2_client(connection: DHIS2Connection, use_cache: bool) -> DHIS2:
    """Initialize DHIS2 client with caching and patch API to handle empty responses.

    Returns:
        DHIS2: Configured DHIS2 client instance.
    """
    cache_dir = Path(".cache") if use_cache else None
    dhis2_client = DHIS2(connection=connection, cache_dir=cache_dir)
    return dhis2_client


def validate_connection(dhis2: DHIS2, name: str = "DHIS2") -> None:
    """Validate a single DHIS2 connection by calling system_info."""
    try:
        dhis2.meta.system_info()
        current_run.log_info(f"✓ {name} connection successful: {dhis2.api.url}")
    except Exception as e:
        current_run.log_error(f"✗ {name} connection failed: {e!s}")
        raise


# --- Metadata queries ---


def get_datasets_as_dict(dhis: DHIS2) -> dict:
    """Get datasets metadata.

    Returns:
        dict: Dictionary of dataset metadata by ID.
    """
    datasets = {}
    for page in dhis.api.get_paged(
        "dataSets",
        params={
            "fields": "id,name,dataSetElements,indicators,organisationUnits,periodType",
            "pageSize": 50,
        },
    ):
        for ds in page["dataSets"]:
            ds_id = ds.get("id")
            datasets[ds_id] = {
                "name": ds.get("name"),
                "data_elements": [dx["dataElement"]["id"] for dx in ds["dataSetElements"]],
                "indicators": [indicator["id"] for indicator in ds["indicators"]],
                "organisation_units": [ou["id"] for ou in ds["organisationUnits"]],
                "periodType": ds["periodType"],
            }
    return datasets


def check_datasets_associated(target_dhis2: DHIS2, data: pl.DataFrame) -> None:
    """Log whether each data element in the data is assigned to a dataset."""
    unique_des = data["data_element_id"].unique().to_list()
    current_run.log_info(f"Checking {len(unique_des)} unique data elements...")
    for de_id in unique_des:
        fields = "id,name,dataSetElements[dataSet[id,name]]"
        de_info = target_dhis2.api.get(f"dataElements/{de_id}", params={"fields": fields})
        datasets = de_info.get("dataSetElements", []) if isinstance(de_info, dict) else []
        if not datasets:
            current_run.log_warning(
                f"⚠ Data element {de_id} ({de_info.get('name', 'Unknown')}) "
                f"is NOT assigned to any dataset!"
            )
        else:
            dataset_names = [ds["dataSet"]["name"] for ds in datasets]
            current_run.log_info(f"✓ Data element {de_id} in datasets: {', '.join(dataset_names)}")


# --- Data extraction ---


def extract_source_data(
    source_dhis2: DHIS2,
    dataset_id: str,
    periods_extraction: list[str],
    dataset_org_units: list[str],
) -> pl.DataFrame:
    """Extract data values from source DHIS2 instance.

    Uses period-based queries for better performance by generating a list of periods
    and querying each individually rather than using date ranges.

    Returns:
        pl.DataFrame: Extracted data values.
    """
    current_run.log_info(f"Extracting data from dataset {dataset_id}...")
    source_dhis2.data_value_sets.MAX_ORG_UNITS = config.max_org_unit_per_request
    source_dhis2.data_value_sets.MAX_PERIODS = config.max_periods_per_request
    try:
        result = source_dhis2.data_value_sets.get(
            datasets=[dataset_id],
            periods=periods_extraction,
            org_units=dataset_org_units,
        )
        data_values_list = result if isinstance(result, list) else []

        if not data_values_list:
            current_run.log_warning("No data values returned from API")
            return pl.DataFrame()

        data_values = (
            pl.DataFrame(data_values_list, infer_schema_length=None)
            .with_columns(pl.lit(dataset_id).alias("dataset_id_snis"))
            .select(
                [
                    "dataset_id_snis",
                    "dataElement",
                    "orgUnit",
                    "categoryOptionCombo",
                    "attributeOptionCombo",
                    "period",
                    "value",
                ]
            )
            .rename(
                {
                    "dataElement": "data_element_id",
                    "orgUnit": "organisation_unit_id",
                    "categoryOptionCombo": "category_option_combo_id",
                    "attributeOptionCombo": "attribute_option_combo_id",
                }
            )
        )

        data_values_numeric = data_values.with_columns(
            pl.col("value").cast(pl.Float64, strict=False)
        )
        non_numeric = data_values_numeric.filter(pl.col("value").is_null()).height
        if non_numeric > 0:
            current_run.log_warning(
                f"Found {non_numeric} rows with non-numeric values; "
                f"these will be dropped during value validation."
            )

        current_run.log_info(f"✓ Extracted {len(data_values_numeric)} data values in bulk")
        current_run.log_info(
            f"  - Unique org units: {data_values_numeric['organisation_unit_id'].n_unique()}"
        )
        current_run.log_info(
            f"  - Unique data elements: {data_values_numeric['data_element_id'].n_unique()}"
        )
        return data_values_numeric

    except Exception as e:
        current_run.log_error(f"Error extracting data: {e!s}")
        raise


# --- Payload preparation ---


def fetch_de_category_combos(
    dhis2: DHIS2,
    fields: str = "id,categoryCombo[categoryOptionCombos[id]]",
    filters: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Fetch data elements with their category option combos from DHIS2.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    fields : str, optional
        Comma-separated DHIS2 fields to include in the response.
    filters : list of str, optional
        DHIS2 query filters.

    Returns
    -------
    List[Dict[str, Any]]
        Raw list of data element dicts with nested categoryCombo/categoryOptionCombos.
    """

    def format_element(element: dict[str, Any], fields: str) -> dict[str, Any]:
        splitted_fields = [f.split("[")[0] for f in re.split(r",(?![^\[]*\])", fields)]
        return {key: element.get(key) for key in splitted_fields}

    params = {"fields": fields}
    if filters:
        params["filter"] = filters

    return [
        format_element(element, fields)
        for page in dhis2.api.get_paged("dataElements", params=params)
        for element in page.get("dataElements", [])
    ]


def get_coc_per_des(
    dhis2: DHIS2,
    filters: list[str] | None = None,
) -> tuple[dict[str, set], dict[str, list]]:
    """Get valid category option combo IDs per data element.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    filters : list of str, optional
        DHIS2 query filter expressions.

    Returns
    -------
    dict[str, set]
        Mapping of data element ID to set of valid category option combo IDs.
    dict[str, list]
        Mapping of data element ID to list of valid category option combo IDs (for JSON serialization).
    """
    meta = fetch_de_category_combos(
        dhis2, fields="id,categoryCombo[categoryOptionCombos[id]]", filters=filters
    )
    df = pl.DataFrame(meta, infer_schema_length=None)
    df = df.with_columns(
        pl.col("categoryCombo")
        .struct.field("categoryOptionCombos")
        .list.eval(pl.element().struct.field("id"))
        .alias("coc_ids")
    ).select(["id", "coc_ids"])
    return {row["id"]: set(row["coc_ids"] or []) for row in df.iter_rows(named=True)}, {
        row["id"]: list(row["coc_ids"] or []) for row in df.iter_rows(named=True)
    }


# --- Data posting ---


def _get_response_value_errors(response: dict | None, chunk: list | None) -> dict | None:
    """Collect relevant data for error logs.

    Returns:
        dict | None: A dictionary containing relevant error data, or None if no errors are found.
    """
    if response is None or not chunk:
        return None

    try:
        out = {}
        for k in ["responseType", "status", "description", "importCount", "dataSetComplete"]:
            out[k] = response.get(k)
        if response.get("conflicts"):
            out["rejected_datapoints"] = [chunk[i] for i in response.get("rejectedIndexes", [])]
            out["conflicts"] = [
                {
                    "object": conflict.get("object"),
                    "objects": conflict.get("objects"),
                    "value": conflict.get("value"),
                    "errorCode": conflict.get("errorCode"),
                }
                for conflict in response["conflicts"]
            ]
        return out
    except AttributeError:
        return None


def post_to_target(
    target_dhis2: DHIS2,
    payload: list[dict[str, Any]],
    dry_run: bool,
) -> dict[str, Any]:
    """Post transformed data to target DHIS2 instance.

    Returns:
        dict[str, Any]: Import results from DHIS2 API.
    """
    total_records = len(payload)
    num_chunks = (total_records + config.chunk_size_post - 1) // config.chunk_size_post
    aggregated_results = {
        "imported": 0,
        "updated": 0,
        "ignored": 0,
        "deleted": 0,
        "conflicts": [],
    }
    failed_http_chunks = []
    conflict_chunks = []
    params = {
        "importStrategy": "CREATE_AND_UPDATE",
        "dryRun": "true" if dry_run else "false",
        "skipValidation": "false",
    }

    for i in range(0, total_records, config.chunk_size_post):
        chunk = payload[i : i + config.chunk_size_post]
        chunk_num = (i // config.chunk_size_post) + 1
        current_run.log_info(f"Posting chunk {chunk_num}/{num_chunks} ({len(chunk)} records)...")

        response = None
        try:
            response = target_dhis2.api.session.post(
                f"{target_dhis2.api.url}/dataValueSets",
                json={"dataValues": chunk},
                params=params,
            )
            response.raise_for_status()
            response_dict = response.json()

            import_summary = response_dict.get("response") or response_dict
            import_count = import_summary.get("importCount", {})

            if not import_count and "response" not in response_dict:
                current_run.log_warning(
                    f"  ⚠ Chunk {chunk_num} - Unexpected response format "
                    f"(keys: {list(response_dict.keys())}); import counts may be missing"
                )

            aggregated_results["imported"] += import_count.get("imported", 0)
            aggregated_results["updated"] += import_count.get("updated", 0)
            aggregated_results["ignored"] += import_count.get("ignored", 0)
            aggregated_results["deleted"] += import_count.get("deleted", 0)

            chunk_conflicts = import_summary.get("conflicts", [])
            if chunk_conflicts:
                aggregated_results["conflicts"].extend(chunk_conflicts)
                error_response = _get_response_value_errors(import_summary, chunk=chunk)
                conflict_chunks.append(
                    {"chunk_number": chunk_num, "conflicts": error_response}
                )
                current_run.log_warning(
                    f"  ⚠ Chunk {chunk_num} - Imported: {import_count.get('imported', 0)}, "
                    f"Updated: {import_count.get('updated', 0)}, "
                    f"Ignored: {import_count.get('ignored', 0)} "
                    f"({len(chunk_conflicts)} conflict(s))"
                )
            else:
                current_run.log_info(
                    f"  ✓ Chunk {chunk_num} - Imported: {import_count.get('imported', 0)}, "
                    f"Updated: {import_count.get('updated', 0)}, "
                    f"Ignored: {import_count.get('ignored', 0)}"
                )

        except requests.exceptions.RequestException as e:
            try:
                response_dict = response.json() if response else None
            except Exception:
                response_dict = None
            error_response = _get_response_value_errors(response_dict, chunk=chunk)
            current_run.log_error(f"  ✗ Chunk {chunk_num} failed (HTTP error): {e!s}")
            failed_http_chunks.append(
                {"chunk_number": chunk_num, "error": str(e), "response": error_response}
            )

    if failed_http_chunks:
        current_run.log_error(f"✗ {len(failed_http_chunks)} out of {num_chunks} batch(es) failed to reach DHIS2")
    else:
        current_run.log_info("✓ All data batches reached DHIS2 successfully")

    conflicts = aggregated_results["conflicts"]
    if conflicts:
        current_run.log_warning(
            f"Found {len(conflicts)} conflict(s) causing {aggregated_results['ignored']} ignored record(s):"
        )
        for idx, conflict in enumerate(conflicts[:10], 1):
            if isinstance(conflict, dict):
                obj = conflict.get("object", "Unknown")
                reason = conflict.get("value", "No reason provided")
                error_code = conflict.get("errorCode", "")
                current_run.log_warning(
                    f"  {idx}. [{error_code}] {obj}: {reason}"
                )
        if len(conflicts) > 10:
            current_run.log_warning(f"  ... and {len(conflicts) - 10} more conflicts")
    elif aggregated_results["ignored"] > 0:
        current_run.log_warning(
            f"Found {aggregated_results['ignored']} ignored records but no conflict details returned"
        )

    if not failed_http_chunks:
        status = "SUCCESS" if not conflict_chunks else "SUCCESS_WITH_CONFLICTS"
    elif len(failed_http_chunks) == num_chunks:
        status = "FAILURE"
    else:
        status = "PARTIAL_FAILURE"

    return {
        "status": status,
        "imported": aggregated_results["imported"],
        "updated": aggregated_results["updated"],
        "ignored": aggregated_results["ignored"],
        "deleted": aggregated_results["deleted"],
        "conflicts": aggregated_results["conflicts"],
        "failed_http_chunks": failed_http_chunks,
        "conflict_chunks": conflict_chunks,
    }
