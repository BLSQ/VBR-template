from pathlib import Path
from typing import Any

import polars as pl
import requests
from openhexa.sdk import DHIS2Connection, current_run
from openhexa.toolbox.dhis2 import DHIS2

import config
from utils import _chunked


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


def get_dataset_org_units(dhis: DHIS2, dataset_id: str) -> list[str]:
    """Retrieve the list of organization unit IDs associated with a given dataset.

    Returns:
        list[str]: A list of organization unit IDs linked to the specified dataset.
    """
    response = dhis.api.get(
        f"dataSets/{dataset_id}.json", params={"fields": "organisationUnits[id]"}
    )
    return [ou["id"] for ou in response.get("organisationUnits", [])]


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


def check_datasets_associated(target_dhis2: DHIS2, payload: list[dict]) -> None:
    """Log whether each data element in the payload is assigned to a dataset."""
    unique_des = {dv["dataElement"] for dv in payload}
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

        data_values = pl.DataFrame(data_values_list, infer_schema_length=None).rename(
            {
                "dataElement": "data_element_id",
                "period": "period",
                "orgUnit": "organisation_unit_id",
                "categoryOptionCombo": "category_option_combo_id",
                "attributeOptionCombo": "attribute_option_combo_id",
                "value": "value",
            }
        )

        current_run.log_info(f"✓ Extracted {len(data_values)} data values in bulk")
        current_run.log_info(
            f"  - Unique org units: {data_values['organisation_unit_id'].n_unique()}"
        )
        current_run.log_info(
            f"  - Unique data elements: {data_values['data_element_id'].n_unique()}"
        )
        return data_values

    except Exception as e:
        current_run.log_error(f"Error extracting data: {e!s}")
        raise


# --- Payload preparation ---


def get_ccs_for_de(dhis2: DHIS2, des_id: list[str]) -> dict[str, str]:
    """Get category combo IDs for a list of data element IDs.

    Returns:
        dict[str, str]: Mapping of data element ID to category combo ID.
    """
    de_to_cc = {}
    for chunk in _chunked(des_id, config.de_chunk):
        ids = ",".join(chunk)
        r = dhis2.api.get(
            "dataElements",
            params={
                "filter": f"id:in:[{ids}]",
                "fields": "id,categoryCombo[id]",
                "paging": "false",
            },
        )
        for de in r.get("dataElements", []):
            if "categoryCombo" in de and "id" in de["categoryCombo"]:
                de_to_cc[de["id"]] = de["categoryCombo"]["id"]

    return de_to_cc


def get_cocs_for_cc(dhis2: DHIS2, cc_ids: list[str]) -> dict[str, set]:
    """Get category option combo IDs for a list of category combo IDs.

    Returns:
        dict[str, set]: Mapping of category combo ID to set of category option combo IDs.
    """
    cc_to_coc = {}
    for chunk in _chunked(cc_ids, config.cc_chunk):
        ids = ",".join(chunk)
        r = dhis2.api.get(
            "categoryCombos",
            params={
                "filter": f"id:in:[{ids}]",
                "fields": "id,categoryOptionCombos[id]",
                "paging": "false",
            },
        )
        for cc in r.get("categoryCombos", []):
            cc_to_coc[cc["id"]] = {coc["id"] for coc in cc.get("categoryOptionCombos", [])}
    return cc_to_coc


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
    current_run.log_info(
        f"Splitting {total_records} records into {num_chunks} chunks of {config.chunk_size_post} records each"
    )
    aggregated_results = {
        "imported": 0,
        "updated": 0,
        "ignored": 0,
        "deleted": 0,
        "conflicts": [],
    }
    failed_chunks = []
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
            response_dict = response.json()

            if "response" in response_dict:
                import_summary = response_dict["response"]
                import_count = import_summary.get("importCount", {})

                aggregated_results["imported"] += import_count.get("imported", 0)
                aggregated_results["updated"] += import_count.get("updated", 0)
                aggregated_results["ignored"] += import_count.get("ignored", 0)
                aggregated_results["deleted"] += import_count.get("deleted", 0)

                chunk_conflicts = import_summary.get("conflicts", [])
                if chunk_conflicts:
                    aggregated_results["conflicts"].extend(chunk_conflicts)
                    error_response = _get_response_value_errors(response_dict, chunk=chunk)
                    failed_chunks.append(
                        {"chunk_number": chunk_num, "errors_import": error_response}
                    )

                current_run.log_info(
                    f"  ✓ Chunk {chunk_num} - Imported: {import_count.get('imported', 0)}, "
                    f"Updated: {import_count.get('updated', 0)}, "
                    f"Ignored: {import_count.get('ignored', 0)}"
                )

        except requests.exceptions.RequestException as e:
            response_dict = response.json() if response else None
            error_response = _get_response_value_errors(response_dict, chunk=chunk)
            current_run.log_error(f"  ✗ Chunk {chunk_num} failed: {e!s}")
            failed_chunks.append(
                {"chunk_number": chunk_num, "error": str(e), "response": error_response}
            )

    if failed_chunks:
        current_run.log_warning(f"⚠ {len(failed_chunks)} chunk(s) failed to post")
    else:
        current_run.log_info("✓ All chunks posted successfully")

    current_run.log_info(
        f"Total Results - Imported: {aggregated_results['imported']}, "
        f"Updated: {aggregated_results['updated']}, "
        f"Ignored: {aggregated_results['ignored']}, "
        f"Failed chunks: {len(failed_chunks)}"
    )

    if aggregated_results["ignored"] > 0:
        conflicts = aggregated_results["conflicts"]
        if conflicts:
            current_run.log_warning(
                f"Found {aggregated_results['ignored']} ignored records with conflicts:"
            )
            for i, conflict in enumerate(conflicts[:10], 1):
                if isinstance(conflict, dict):
                    obj = conflict.get("object", "Unknown")
                    reason = conflict.get("value", "No reason provided")
                    current_run.log_warning(f"  {i}. {obj}: {reason}")
            if len(conflicts) > 10:
                current_run.log_warning(f"  ... and {len(conflicts) - 10} more conflicts")
        else:
            current_run.log_warning(
                f"Found {aggregated_results['ignored']} ignored records but no conflict details"
            )

    return {
        "status": "SUCCESS",
        "imported": aggregated_results["imported"],
        "updated": aggregated_results["updated"],
        "ignored": aggregated_results["ignored"],
        "deleted": aggregated_results["deleted"],
        "conflicts": aggregated_results["conflicts"],
        "failed_chunks": failed_chunks,
    }
