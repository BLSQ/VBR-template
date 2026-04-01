"""DHIS2 to DHIS2 Data Elements Pipeline.

This pipeline extracts data values from a source DHIS2 instance for a given dataset
and writes the values to a target DHIS2 instance, using mappings for dataElement
and categoryOptionCombos IDs.
"""

import json
import logging
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import requests
from humanize import naturalsize
from openhexa.sdk import (
    DHIS2Connection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.sdk.pipelines.parameter import DHIS2Widget
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import Period, period_from_string, get_range


def get_dhis2_client(connection: DHIS2Connection, use_cache: bool) -> DHIS2:
    """Initialize DHIS2 client with caching and patch API to handle empty responses.

    Returns:
        DHIS2: Configured DHIS2 client instance.
    """
    if use_cache:
        cache_dir = Path(".cache")
    else:
        cache_dir = None

    dhis2_client = DHIS2(connection=connection, cache_dir=cache_dir)

    # Monkey-patch the api.get() method to handle empty responses
    def patched_get(endpoint: str, params: dict | None = None, use_cache: bool = True) -> dict:
        """Patched GET request that handles empty responses from DHIS2."""
        logger = logging.getLogger(__name__)

        r = requests.Request(method="GET", url=f"{dhis2_client.api.url}/{endpoint}", params=params)
        url = r.prepare().url
        # current_run.log_info(f"GET {url}")

        use_cache_flag = dhis2_client.api.cache and use_cache

        if use_cache_flag:
            cached = dhis2_client.api.cache.get(endpoint=endpoint, params=params)
            if cached:
                logger.debug("Cache hit, returning cached response")
                return cached

        r = dhis2_client.api.session.get(f"{dhis2_client.api.url}/{endpoint}", params=params)
        dhis2_client.api.raise_if_error(r)

        # Handle empty response (DHIS2 returns 200 with 0 bytes when no data exists)
        if len(r.content) == 0:
            logger.warning(f"Empty response from DHIS2 API for {endpoint}")
            # Return empty dict or appropriate structure based on endpoint
            if endpoint == "dataValueSets" or endpoint.startswith("dataValueSets"):
                return {"dataValues": []}
            return {}

        # Try to parse JSON, catch JSONDecodeError
        try:
            json_response = r.json()
        except requests.exceptions.JSONDecodeError:
            logger.error(
                f"Failed to parse JSON response from {endpoint}. "
                f"Status: {r.status_code}, Content-Length: {len(r.content)}"
            )
            # For dataValueSets, return empty structure instead of raising
            if endpoint == "dataValueSets" or endpoint.startswith("dataValueSets"):
                logger.warning("Returning empty dataValues structure")
                return {"dataValues": []}
            raise

        if use_cache_flag:
            logger.debug("Cache miss, caching response")
            dhis2_client.api.cache.set(endpoint=endpoint, params=params, response=json_response)

        logger.debug(f"Successful request of size {naturalsize(len(r.content))}")

        return json_response

    # Apply the patch
    dhis2_client.api.get = patched_get

    return dhis2_client


def validate_mapping_structure(
    mapping_data: dict[str, Any], different_org_units: bool = False
) -> bool:
    """Validate mapping file structure.

    Returns:
        bool: True if structure is valid, False otherwise.
    """
    required_keys = ["dataElements", "categoryOptionCombos", "attributeOptionCombos"]

    if different_org_units:
        required_keys.append("orgUnits")

    for key in required_keys:
        if key not in mapping_data:
            current_run.log_error(f"Missing required key '{key}' in mapping file")
            return False

        if not isinstance(mapping_data[key], dict):
            current_run.log_error(f"Key '{key}' must be a dictionary")
            return False

    return True


def check_objects_exist(dhis2: DHIS2, object_type: str, object_ids: list[str]) -> dict[str, bool]:
    """Check if objects exist in DHIS2 instance.

    Returns:
        dict[str, bool]: Mapping of object IDs to existence status.
    """
    endpoint = f"{object_type}s"
    existing_objects = {}

    try:
        response = dhis2.api.get(endpoint, params={"fields": "id", "paging": "false"})
        existing_ids = {obj["id"] for obj in response.get(endpoint, [])}

        for obj_id in object_ids:
            existing_objects[obj_id] = obj_id in existing_ids

    except Exception as e:
        current_run.log_error(f"Error checking {object_type} existence: {e!s}")
        for obj_id in object_ids:
            existing_objects[obj_id] = False

    return existing_objects


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
        "mapped_org_units": 0,
        "unmapped_org_units": 0,
        "final_count": 0,
    }

    # Apply data element mappings
    de_mapping = mapping.get("dataElements", {})
    if de_mapping:
        # Filter for mapped data elements
        mapped_des = list(de_mapping.keys())
        data_values = data_values.filter(pl.col("data_element_id").is_in(mapped_des))
        stats["mapped_data_elements"] = len(data_values)
        stats["unmapped_data_elements"] = original_count - len(data_values)

        # Apply mapping
        mapping_expr = pl.col("data_element_id").map_elements(
            lambda x: de_mapping.get(x, x), return_dtype=pl.Utf8
        )
        data_values = data_values.with_columns(mapping_expr)

    # Apply category option combo mappings
    coc_mapping = mapping.get("categoryOptionCombos", {})
    if coc_mapping and "category_option_combo_id" in data_values.columns:
        # Filter for mapped COCs
        mapped_cocs = list(coc_mapping.keys())
        data_values = data_values.filter(pl.col("category_option_combo_id").is_in(mapped_cocs))
        stats["mapped_category_option_combos"] = len(data_values)

        # Apply mapping
        mapping_expr = pl.col("category_option_combo_id").map_elements(
            lambda x: coc_mapping.get(x, x), return_dtype=pl.Utf8
        )
        data_values = data_values.with_columns(mapping_expr)
    aoc_mapping = mapping.get("attributeOptionCombos", {})
    if aoc_mapping and "attribute_option_combo_id" in data_values.columns:
        # Filter for mapped AOCs
        mapped_aocs = list(aoc_mapping.keys())
        data_values = data_values.filter(pl.col("attribute_option_combo_id").is_in(mapped_aocs))
        stats["mapped_attribute_option_combos"] = len(data_values)

        # Apply mapping
        mapping_expr = pl.col("attribute_option_combo_id").map_elements(
            lambda x: aoc_mapping.get(x, x), return_dtype=pl.Utf8
        )
        data_values = data_values.with_columns(mapping_expr)

    # Apply organization unit mappings
    ou_mapping = mapping.get("orgUnits", {})
    if ou_mapping and "organisation_unit_id" in data_values.columns:
        # Filter for mapped org units
        mapped_ous = list(ou_mapping.keys())
        data_values = data_values.filter(pl.col("organisation_unit_id").is_in(mapped_ous))
        stats["mapped_org_units"] = len(data_values)
        stats["unmapped_org_units"] = original_count - len(data_values)

        # Apply mapping
        mapping_expr = pl.col("organisation_unit_id").map_elements(
            lambda x: ou_mapping.get(x, x), return_dtype=pl.Utf8
        )
        data_values = data_values.with_columns(mapping_expr)

    stats["final_count"] = len(data_values)
    return data_values, stats


from itertools import islice


def _chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk


def build_cc_and_coc_cache(dhis2_client, data_values, de_chunk=200, cc_chunk=100):
    """
    dhis2_client: e.g. target_dhis2.client.api
    data_values: iterable of dicts containing at least 'dataElement'
    Returns:
      de_to_cc:  {dataElementId -> categoryComboId}
      cc_to_coc: {categoryComboId -> set(categoryOptionComboId)}
    """

    # 1) Distinct DE ids from the payload
    de_ids = sorted({item["dataElement"] for item in data_values if "dataElement" in item})

    # 2) Batch get DE -> CC
    de_to_cc = {}
    for chunk in _chunked(de_ids, de_chunk):
        # /api/dataElements?filter=id:in:[id1,id2]&fields=id,categoryCombo[id]&paging=false
        ids = ",".join(chunk)  # DHIS2 accepts comma-separated, or [id1,id2] depending on version
        r = dhis2_client.get(
            "dataElements",
            params={
                "filter": f"id:in:[{ids}]",
                "fields": "id,categoryCombo[id]",
                "paging": "false",
            },
        )
        print(r)
        for de in r.get("dataElements", []):
            if "categoryCombo" in de and de["categoryCombo"]:
                de_to_cc[de["id"]] = de["categoryCombo"]["id"]

    # 3) Distinct CC ids
    cc_ids = sorted(set(de_to_cc.values()))

    # 4) Batch get CC -> COCs
    cc_to_coc = {}
    for chunk in _chunked(cc_ids, cc_chunk):
        ids = ",".join(chunk)
        r = dhis2_client.get(
            "categoryCombos",
            params={
                "filter": f"id:in:[{ids}]",
                "fields": "id,categoryOptionCombos[id]",
                "paging": "false",
            },
        )

        for cc in r.get("categoryCombos", []):
            cc_to_coc[cc["id"]] = {coc["id"] for coc in cc.get("categoryOptionCombos", [])}

    return de_to_cc, cc_to_coc


def coc_is_valid_for_de(item, de_to_cc, cc_to_coc):
    """
    Returns True iff the item's categoryOptionCombo belongs to the DE's categoryCombo.
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


def coerce_value(value, value_type):
    """Try to coerce value into the correct type for DHIS2.
    Return None if not possible/consistent."""
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
            # Not supported types
            return None
    except Exception:
        return None


def validate_and_transform(dv, client, value_types):
    """Validate & coerce values for a payload list of data_values."""
    de_uid = dv.get("dataElement")
    if not de_uid:
        return None  # skip if missing

    # cache valueType per dataElement
    if de_uid not in value_types:
        value_types[de_uid] = client.meta.identifiable_objects(de_uid).get("valueType")

    value_type = value_types[de_uid]
    coerced = coerce_value(dv.get("value"), value_type)
    if coerced is None:
        # skip inconsistent
        return None

    return coerced


def prepare_data_value_payload(data_values: pl.DataFrame, client) -> list[dict[str, Any]]:
    """Prepare data values for DHIS2 API payload.

    Returns:
        list[dict[str, Any]]: List of data value dictionaries for API.
    """
    mapping_toolbox_dhis2_name = {
        "data_element_id": "dataElement",
        "organisation_unit_id": "orgUnit",
        "period": "period",
        "category_option_combo_id": "categoryOptionCombo",
        "value": "value",
        "attribute_option_combo_id": "attributeOptionCombo",
    }

    # Check for required columns
    missing_columns = [col for col in mapping_toolbox_dhis2_name if col not in data_values.columns]
    if missing_columns:
        current_run.log_error(f"Missing required columns: {missing_columns}")
        raise ValueError(f"Missing required columns: {missing_columns}")
    data_values = data_values.rename(mapping_toolbox_dhis2_name)
    # Convert to dictionaries
    payload = data_values.select(list(mapping_toolbox_dhis2_name.values())).to_dicts()
    # Convert values to strings as required by DHIS2
    valid_payload = []
    value_types = {}
    de_to_cc, cc_to_coc = build_cc_and_coc_cache(client.api, payload)
    for item in payload:
        if len(set(mapping_toolbox_dhis2_name.values()) - set(item.keys())) > 0:
            current_run.log_info(f"skip {item} for posting values")
            continue
        if any([value is None for value in item.values()]):
            print(f"Skipping item with None values: {item}")
            continue  # Skip items with None values
        if "value" in item and item["value"] is not None:
            if coc_is_valid_for_de(item, de_to_cc, cc_to_coc):
                value = validate_and_transform(item, client, value_types)
                if value is not None:
                    item["value"] = value
                    valid_payload.append(item)

    return valid_payload


def calculate_relative_dates(days_back: int) -> tuple[str, str]:
    """Calculate relative date range based on today's date.

    Args:
        days_back (int): Number of days to go back from today for start date.

    Returns:
        tuple[str, str]: (start_date, end_date) in YYYY-MM-DD format.
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


@pipeline("dhis2_to_dhis2_data_elements")
@parameter(
    "source_connection",
    type=DHIS2Connection,
    name="Source DHIS2 Connection",
    required=True,
    # default="dhis2-demo-2-41",
)
@parameter(
    "target_connection",
    type=DHIS2Connection,
    name="Target DHIS2 Connection",
    required=True,
    # default="dhis2-demo-2-39",
)
@parameter(
    "dataset_id",
    type=str,
    widget=DHIS2Widget.DATASETS,
    connection="source_connection",
    name="Dataset ID",
    required=True,
    # default="TuL8IOPzpHh",
)
@parameter(
    "mapping_file",
    type=str,
    name="Mapping JSON filename",
    required=True,
)
@parameter(
    "different_org_units",
    type=bool,
    name="Organization Unit IDs differ",
    help="Enable if source and target DHIS2 instances have different org unit IDs (must be mapped)",
    default=False,
    required=False,
)
@parameter(
    "start_date",
    type=str,
    name="Start Date (YYYY-MM-DD)",
    required=False,
)
@parameter(
    "end_date",
    type=str,
    name="End Date (YYYY-MM-DD)",
    required=False,
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
@parameter(
    "use_cache",
    type=bool,
    name="Use Cache",
    default=False,
    required=False,
)
@parameter("max_org_unit_per_request", type=int, default=20, required=False)
@parameter("max_periods_per_request", type=int, default=5, required=False)
def dhis2_to_dhis2_data_elements(
    source_connection: DHIS2Connection,
    target_connection: DHIS2Connection,
    dataset_id: str,
    mapping_file: str,
    start_date: str,
    end_date: str,
    use_cache: bool,
    max_periods_per_request: int,
    max_org_unit_per_request: int,
    dry_run: bool = False,
    different_org_units: bool = False,
    use_relative_dates: bool = False,
    days_back: int = 365,
):
    """Extract data values from source DHIS2 and write to target DHIS2 with mappings."""
    # Handle relative dates if enabled
    if use_relative_dates:
        calculated_start_date, calculated_end_date = calculate_relative_dates(days_back)
        current_run.log_info(
            f"Using relative dates: {calculated_start_date} to {calculated_end_date}"
        )
        current_run.log_info(f"  - Days back: {days_back}")
        start_date = calculated_start_date
        end_date = calculated_end_date
    else:
        current_run.log_info(f"Using provided dates: {start_date} to {end_date}")

    # Initialize clients
    source_dhis2, target_dhis2 = validate_connections(
        source_connection, target_connection, use_cache
    )

    # Load and validate mappings
    mapping_data = load_and_validate_mappings(
        mapping_file, source_dhis2, target_dhis2, different_org_units
    )

    # Extract source data
    source_data = extract_source_data(
        source_dhis2,
        dataset_id,
        start_date,
        end_date,
        max_periods_per_request,
        max_org_unit_per_request,
    )

    # Validate org units
    # validated_data = validate_org_units(
    #    source_data, target_dhis2, mapping_data, different_org_units
    # )

    # Transform data values
    transformed_data, transform_stats = transform_data_values(source_data, mapping_data)

    # Post to target
    post_results = post_to_target(target_dhis2, transformed_data, dry_run)

    # Generate summary
    generate_summary(transform_stats, post_results, dry_run)


def validate_connections(
    source_connection: DHIS2Connection,
    target_connection: DHIS2Connection,
    use_cache: bool,
) -> tuple[DHIS2, DHIS2]:
    """Validate source and target DHIS2 connections.

    Returns:
        tuple[DHIS2, DHIS2]: Source and target DHIS2 client instances.
    """
    current_run.log_info("Validating DHIS2 connections...")

    # Initialize clients
    source_dhis2 = get_dhis2_client(source_connection, use_cache)
    target_dhis2 = get_dhis2_client(target_connection, use_cache)

    # Test source connection
    try:
        source_dhis2.meta.system_info()
        current_run.log_info(f"✓ Source DHIS2 connection successful: {source_dhis2.api.url}")
    except Exception as e:
        current_run.log_error(f"✗ Source DHIS2 connection failed: {e!s}")
        raise

    # Test target connection
    try:
        target_dhis2.meta.system_info()
        current_run.log_info(f"✓ Target DHIS2 connection successful: {target_dhis2.api.url}")
    except Exception as e:
        current_run.log_error(f"✗ Target DHIS2 connection failed: {e!s}")
        raise

    return source_dhis2, target_dhis2


def load_and_validate_mappings(
    mapping_file: str,
    source_dhis2: DHIS2,
    target_dhis2: DHIS2,
    different_org_units: bool = False,
) -> dict[str, dict[str, str]]:
    """Load and validate mapping file.

    Returns:
        dict[str, dict[str, str]]: Validated mapping data.
    """
    current_run.log_info("Loading and validating mapping file...")

    # Load mapping file
    try:
        with Path(f"{workspace.files_path}/{mapping_file}").open(encoding="utf-8") as f:
            mapping_data = json.load(f)
    except Exception as e:
        current_run.log_error(f"Error loading mapping file: {e!s}")
        raise

    # Validate structure
    if not validate_mapping_structure(mapping_data, different_org_units):
        raise ValueError("Invalid mapping file structure")

    # Validate data elements
    de_mapping = mapping_data.get("dataElements", {})
    if de_mapping:
        current_run.log_info(f"Validating {len(de_mapping)} data element mappings...")

        # Check source data elements
        source_des = list(de_mapping.keys())
        source_exists = check_objects_exist(source_dhis2, "dataElement", source_des)
        missing_source = [de for de, exists in source_exists.items() if not exists]
        if missing_source:
            current_run.log_warning(f"Data elements not found in source DHIS2: {missing_source}")

        # Check target data elements
        target_des = list(de_mapping.values())
        target_exists = check_objects_exist(target_dhis2, "dataElement", target_des)
        missing_target = [de for de, exists in target_exists.items() if not exists]
        if missing_target:
            current_run.log_warning(f"Data elements not found in target DHIS2: {missing_target}")

    # Validate category option combos
    coc_mapping = mapping_data.get("categoryOptionCombos", {})
    if coc_mapping:
        current_run.log_info(f"Validating {len(coc_mapping)} category option combo mappings...")

        # Check source COCs
        source_cocs = list(coc_mapping.keys())
        source_exists = check_objects_exist(source_dhis2, "categoryOptionCombo", source_cocs)
        missing_source = [coc for coc, exists in source_exists.items() if not exists]
        if missing_source:
            current_run.log_warning(
                f"Category option combos not found in source DHIS2: {missing_source}"
            )

        # Check target COCs
        target_cocs = list(coc_mapping.values())
        target_exists = check_objects_exist(target_dhis2, "categoryOptionCombo", target_cocs)
        missing_target = [coc for coc, exists in target_exists.items() if not exists]
        if missing_target:
            current_run.log_warning(
                f"Category option combos not found in target DHIS2: {missing_target}"
            )

    # Validate organization units if different_org_units is enabled
    if different_org_units:
        ou_mapping = mapping_data.get("orgUnits", {})
        if ou_mapping:
            current_run.log_info(f"Validating {len(ou_mapping)} organization unit mappings...")

            # Check source org units
            source_ous = list(ou_mapping.keys())
            source_exists = check_objects_exist(source_dhis2, "organisationUnit", source_ous)
            missing_source = [ou for ou, exists in source_exists.items() if not exists]
            if missing_source:
                current_run.log_warning(
                    f"Organization units not found in source DHIS2: {missing_source}"
                )

            # Check target org units
            target_ous = list(ou_mapping.values())
            target_exists = check_objects_exist(target_dhis2, "organisationUnit", target_ous)
            missing_target = [ou for ou, exists in target_exists.items() if not exists]
            if missing_target:
                current_run.log_warning(
                    f"Organization units not found in target DHIS2: {missing_target}"
                )
        else:
            current_run.log_error("different_org_units is enabled but no orgUnits mapping found")
            raise ValueError("Missing orgUnits mapping when different_org_units is enabled")

    current_run.log_info("✓ Mapping validation completed")
    return mapping_data


def get_dataset_org_units(dhis: DHIS2, dataset_id: str) -> list[str]:
    """Retrieve the list of organization unit IDs associated with a given dataset.

    Args:
        dhis (DHIS2): The DHIS2 client object used to interact with the DHIS2 API.
        dataset_id (str): The ID of the dataset.

    Returns:
        list[str]: A list of organization unit IDs linked to the specified dataset.
    """
    response = dhis.api.get(
        f"dataSets/{dataset_id}.json", params={"fields": "organisationUnits[id]"}
    )
    return [ou["id"] for ou in response.get("organisationUnits", [])]


def isodate_to_period_type(date: str, period_type: str) -> Period:
    """Converts an ISO date string to a DHIS2-compatible period string based on the specified period type.

    Args:
        date (str): The ISO date string in the format "YYYY-MM-DD".
        period_type (str): The DHIS2 period type. Supported values include:
            - "Daily": Converts to a daily period (e.g., "20230101").
            - "Weekly": Converts to a weekly period starting on Monday (e.g., "2023W1").
            - "WeeklyMonday", "WeeklyTuesday", ..., "WeeklySunday": Converts to a weekly period
              aligned to the specified weekday.
            - "Monthly": Converts to a monthly period (e.g., "202301").
            - "BiMonthly": Converts to a bi-monthly period (e.g., "202301" for Jan-Feb).
            - "Quarterly": Converts to a quarterly period (e.g., "2023Q1").
            - "SixMonthly": Converts to a six-monthly period (e.g., "2023S1" for Jan-Jun).
            - "SixMonthlyApril": Converts to a six-monthly period starting in April (e.g., "2023AprilS1").
            - "Yearly": Converts to a yearly period (e.g., "2023").
            - "FinancialApril": Converts to a financial year starting in April (e.g., "2023April").
            - "FinancialJuly": Converts to a financial year starting in July (e.g., "2023July").
            - "FinancialOct": Converts to a financial year starting in October (e.g., "2023Oct").

    Returns:
        Period: A DHIS2-compatible period object created from the generated period string.

    Raises:
        ValueError: If the provided period type is unsupported.
    """  # noqa: E501
    """Converts an ISO date to a DHIS2-compatible period string with support for weekly anchors."""

    dt = datetime.strptime(date, "%Y-%m-%d")

    if period_type == "Daily":
        period_str = dt.strftime("%Y%m%d")

    elif period_type.startswith("Weekly"):
        # For weekly periods, use ISO calendar week
        iso_year, iso_week, _ = dt.isocalendar()
        if period_type != "Weekly" and period_type != "WeeklyMonday":
            # If the period type specifies a weekday, append it
            period_str = f"{iso_year}{period_type.replace('Weekly', '')[:3]}W{iso_week}"
        else:
            period_str = f"{iso_year}W{iso_week}"  # No leading zero

    elif period_type == "Monthly":
        period_str = dt.strftime("%Y%m")

    elif period_type == "BiMonthly":
        period_str = f"{dt.year}0{(dt.month - 1) // 2 + 1}"

    elif period_type == "Quarterly":
        period_str = f"{dt.year}Q{(dt.month - 1) // 3 + 1}"

    elif period_type == "SixMonthly":
        period_str = f"{dt.year}S{1 if dt.month <= 6 else 2}"

    elif period_type == "SixMonthlyApril":
        if 4 <= dt.month <= 9:
            period_str = f"{dt.year}AprilS1"
        else:
            ref_year = dt.year if dt.month >= 4 else dt.year - 1
            period_str = f"{ref_year}AprilS2"

    elif period_type == "Yearly":
        period_str = f"{dt.year}"

    elif period_type == "FinancialApril":
        fy = dt.year if dt.month >= 4 else dt.year - 1
        period_str = f"{fy}April"

    elif period_type == "FinancialJuly":
        fy = dt.year if dt.month >= 7 else dt.year - 1
        period_str = f"{fy}July"

    elif period_type == "FinancialOct":
        fy = dt.year if dt.month >= 10 else dt.year - 1
        period_str = f"{fy}Oct"

    else:
        raise ValueError(f"Unsupported DHIS2 period type: {period_type}")

    return period_from_string(period_str)


def get_datasets_as_dict(dhis: DHIS2) -> dict[dict]:
    """Get datasets metadata.

    Args:
        dhis (DHIS2): The DHIS2 connection object.

    Returns:
        dict[dict]: Dictionary of dataset metadata by ID.
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


def extract_source_data(
    source_dhis2: DHIS2,
    dataset_id: str,
    start_date: str,
    end_date: str,
    max_periods_per_request: int,
    max_org_unit_per_request: int,
) -> pl.DataFrame:
    """Extract data values from source DHIS2 instance.

    Uses period-based queries for better performance by generating a list of periods
    and querying each individually rather than using date ranges.

    Returns:
        pl.DataFrame: Extracted data values.
    """
    current_run.log_info(f"Extracting data from dataset {dataset_id}...")
    dataset_org_units = get_dataset_org_units(source_dhis2, dataset_id)
    datasets = get_datasets_as_dict(source_dhis2)
    period_type = datasets[dataset_id]["periodType"]
    current_run.log_info(f"  - Dataset period type: {period_type}")
    source_dhis2.data_value_sets.MAX_ORG_UNITS = max_org_unit_per_request
    source_dhis2.data_value_sets.MAX_PERIODS = max_periods_per_request
    # Convert ISO dates to DHIS2 periods
    start_period = isodate_to_period_type(start_date, period_type)
    end_period = isodate_to_period_type(end_date, period_type)

    # Generate list of periods for more efficient DHIS2 queries
    periods = get_range(start_period, end_period)
    periods_str = [str(p) for p in periods]
    current_run.log_info(f"  - Querying {len(periods)} periods: {periods[0]} to {periods[-1]}")
    try:
        # Use data_value_sets.get() with periods list
        # The patched api.get() handles empty responses automatically
        result = source_dhis2.data_value_sets.get(
            datasets=[dataset_id],
            periods=periods_str,
            org_units=dataset_org_units,
        )
        data_values_list = result if isinstance(result, list) else []

        # Convert to DataFrame with schema inference
        if len(data_values_list) == 0:
            current_run.log_warning("No data values returned from API")
            data_values = pl.DataFrame()
        else:
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

        if len(data_values) > 0:
            current_run.log_info(f"✓ Extracted {len(data_values)} data values in bulk")
            current_run.log_info(
                f"  - Unique org units: {data_values['organisation_unit_id'].n_unique()}"
            )
            current_run.log_info(
                f"  - Unique data elements: {data_values['data_element_id'].n_unique()}"
            )
            current_run.log_info(f"  - Date range: {start_date} to {end_date}")
        else:
            current_run.log_warning("No data values extracted from source dataset")
        return data_values

    except Exception as e:
        current_run.log_error(f"Error extracting data: {e!s}")
        raise


def validate_org_units(
    data_values: pl.DataFrame,
    target_dhis2: DHIS2,
    mapping_data: dict[str, dict[str, str]],
    different_org_units: bool = False,
) -> pl.DataFrame:
    """Validate that org units exist in target DHIS2 instance.

    Returns:
        pl.DataFrame: Filtered data with valid org units only.
    """
    current_run.log_info("Validating organization units...")

    # Get unique org units from data
    unique_org_units = data_values["organisation_unit_id"].unique().to_list()
    current_run.log_info(f"Checking {len(unique_org_units)} organization units...")

    # If different_org_units is enabled, we need to check target org units after mapping
    if different_org_units:
        ou_mapping = mapping_data.get("orgUnits", {})
        if ou_mapping:
            # Map source org units to target org units
            mapped_org_units = [ou_mapping.get(ou) for ou in unique_org_units if ou in ou_mapping]
            # Remove None values (unmapped org units)
            mapped_org_units = [ou for ou in mapped_org_units if ou is not None]

            current_run.log_info(f"Mapped {len(mapped_org_units)} org units to target system")

            # Check existence of mapped org units in target
            if mapped_org_units:
                org_unit_exists = check_objects_exist(
                    target_dhis2, "organisationUnit", mapped_org_units
                )
                missing_target_org_units = [
                    ou for ou, exists in org_unit_exists.items() if not exists
                ]

                if missing_target_org_units:
                    current_run.log_warning(
                        f"Missing mapped org units in target: {len(missing_target_org_units)}"
                    )
                    for ou in missing_target_org_units[:5]:  # Log first 5
                        current_run.log_warning(f"  - {ou}")
                    if len(missing_target_org_units) > 5:
                        current_run.log_warning(
                            f"  ... and {len(missing_target_org_units) - 5} more"
                        )

            # Filter data for source org units that have valid mappings
            valid_source_org_units = [ou for ou in unique_org_units if ou in ou_mapping]
            filtered_data = data_values.filter(
                pl.col("organisation_unit_id").is_in(valid_source_org_units)
            )

            current_run.log_info(f"✓ Filtered to {len(filtered_data)} data values")
            current_run.log_info(f"  - Valid source org units: {len(valid_source_org_units)}")
            invalid_count = len(unique_org_units) - len(valid_source_org_units)
            current_run.log_info(f"  - Invalid source org units: {invalid_count}")

            return filtered_data

        current_run.log_error("different_org_units is enabled but no orgUnits mapping found")
        raise ValueError("Missing orgUnits mapping when different_org_units is enabled")

    # Original logic for same org units
    # Check existence in target
    org_unit_exists = check_objects_exist(target_dhis2, "organisationUnit", unique_org_units)

    # Filter data for existing org units
    existing_org_units = [ou for ou, exists in org_unit_exists.items() if exists]
    missing_org_units = [ou for ou, exists in org_unit_exists.items() if not exists]

    if missing_org_units:
        current_run.log_warning(f"Missing org units in target: {len(missing_org_units)}")
        for ou in missing_org_units[:5]:  # Log first 5
            current_run.log_warning(f"  - {ou}")
        if len(missing_org_units) > 5:
            current_run.log_warning(f"  ... and {len(missing_org_units) - 5} more")

    # Filter data
    filtered_data = data_values.filter(pl.col("organisation_unit_id").is_in(existing_org_units))

    current_run.log_info(f"✓ Filtered to {len(filtered_data)} data values")
    current_run.log_info(f"  - Valid org units: {len(existing_org_units)}")
    current_run.log_info(f"  - Invalid org units: {len(missing_org_units)}")

    return filtered_data


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
    current_run.log_info(f"{transformed_data.head(1)}")
    current_run.log_info("✓ Transformation completed")
    current_run.log_info(f"  - Original records: {stats['original_count']}")
    current_run.log_info(f"  - Final records (after null values filtered): {stats['final_count']}")
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
    current_run.log_info(f"  - Mapped org units: {stats['mapped_org_units']}")
    current_run.log_info(f"  - Unmapped org units: {stats['unmapped_org_units']}")

    return transformed_data, stats


def pretty_dhis2_error(err):
    r = getattr(err, "response", None)
    if not r:
        current_run.log_info("No response on error.")
        return

    current_run.log_info(f"HTTP {r.status_code} {r.reason} — URL: {r.url}")
    # Try JSON first, then fall back to text
    try:
        body = r.json()
    except ValueError:
        print(r.text[:4000])
        return

    print(json.dumps(body, indent=2, ensure_ascii=False))

    # DHIS2 puts useful bits in a few shapes depending on version:
    resp = body.get("response") or body.get("importConflicts") or {}
    # 2.3x+/2.4x styles:
    conflicts = resp.get("conflicts") or body.get("conflicts") or []
    import_count = resp.get("importCount") or body.get("importCount")
    status = body.get("status") or resp.get("status")

    if import_count:
        current_run.log_info(f"importCount: {import_count}")
    if conflicts:
        current_run.log_info("conflicts:")
        for c in conflicts:
            # can be {"object":"...","value":"..."} or {"errorCode":"...","message":"..."}
            obj = c.get("object") or c.get("objectIndex") or ""
            val = c.get("value") or c.get("message") or c
            current_run.log_info(f"  - {obj}: {val}")


def post_to_target(
    target_dhis2: DHIS2,
    transformed_data: pl.DataFrame,
    dry_run: bool,
) -> dict[str, Any]:
    """Post transformed data to target DHIS2 instance.

    Returns:
        dict[str, Any]: Import results from DHIS2 API.
    """
    current_run.log_info(f"Posting data to target DHIS2 (dry_run={dry_run})...")

    if len(transformed_data) == 0:
        current_run.log_warning("No data to post after transformation")
        return {"status": "no_data", "imported": 0, "updated": 0, "ignored": 0}

    # Prepare payload
    payload = prepare_data_value_payload(transformed_data, target_dhis2)
    current_run.log_info(f"Prepared {len(payload)} data values for posting")

    # Check if data elements are in datasets
    check_datasets_associated(target_dhis2, payload)

    # Configuration
    chunk_size = 500
    total_records = len(payload)
    num_chunks = (total_records + chunk_size - 1) // chunk_size

    current_run.log_info(
        f"Splitting {total_records} records into {num_chunks} chunks of {chunk_size} records each"
    )

    # Initialize aggregated results
    aggregated_results = {
        "imported": 0,
        "updated": 0,
        "ignored": 0,
        "deleted": 0,
        "conflicts": [],
    }

    # Track failed chunks
    failed_chunks = []

    # Build query parameters
    params = {
        "importStrategy": "CREATE_AND_UPDATE",
        "dryRun": "true" if dry_run else "false",
        "skipValidation": "false",  # Enable validation to get detailed conflicts
    }

    # Process chunks
    for i in range(0, total_records, chunk_size):
        chunk = payload[i : i + chunk_size]
        chunk_num = (i // chunk_size) + 1

        current_run.log_info(f"Posting chunk {chunk_num}/{num_chunks} ({len(chunk)} records)...")

        # Prepare DHIS2 API payload for this chunk
        dhis2_payload = {"dataValues": chunk}

        try:
            # Post to DHIS2 API
            response = target_dhis2.api.session.post(
                f"{target_dhis2.api.url}/dataValueSets",
                json=dhis2_payload,
                params=params,
            )
            print(response.status_code)
            print(response.json())
            # Parse response
            response_dict = response.json()

            # Extract import counts from response
            if "response" in response_dict:
                import_summary = response_dict["response"]
                import_count = import_summary.get("importCount", {})

                # Aggregate counts
                aggregated_results["imported"] += import_count.get("imported", 0)
                aggregated_results["updated"] += import_count.get("updated", 0)
                aggregated_results["ignored"] += import_count.get("ignored", 0)
                aggregated_results["deleted"] += import_count.get("deleted", 0)

                # Collect conflicts
                chunk_conflicts = import_summary.get("conflicts", [])
                if chunk_conflicts:
                    aggregated_results["conflicts"].extend(chunk_conflicts)
                    response_dict = response.json() if response else None
                    error_response = _get_response_value_errors(response_dict, chunk=chunk)
                    if response_dict:
                        # Log error but continue processing
                        # Store failed chunk with error details
                        failed_chunks.append(
                            {
                                "chunk_number": chunk_num,
                                "Errors_import": error_response,
                            }
                        )
                current_run.log_info(
                    f"  ✓ Chunk {chunk_num} - Imported: {import_count.get('imported', 0)}, "
                    f"Updated: {import_count.get('updated', 0)}, "
                    f"Ignored: {import_count.get('ignored', 0)}"
                )

        except requests.exceptions.RequestException as e:
            response_dict = response.json() if response else None
            error_response = _get_response_value_errors(response_dict, chunk=chunk)
            if response_dict:
                # Log error but continue processing
                current_run.log_error(f"  ✗ Chunk {chunk_num} failed: {e!s}")
                # Store failed chunk with error details
                failed_chunks.append(
                    {
                        "chunk_number": chunk_num,
                        "error": str(e),
                        # "data_values": chunk,
                        "response": error_response,
                    }
                )

    # Handle failed chunks
    if failed_chunks:
        current_run.log_warning(f"⚠ {len(failed_chunks)} chunk(s) failed to post")

        # Save failed chunks to file
        failed_chunks_file = (
            Path(f"{workspace.files_path}/pipelines/dhis2_to_dhis2_data_elements")
            / "failed_chunks.json"
        )
        with failed_chunks_file.open("w", encoding="utf-8") as f:
            json.dump(failed_chunks, f, indent=2)

        current_run.log_warning(f"Failed chunks saved to: {failed_chunks_file}")
        current_run.add_file_output(failed_chunks_file.as_posix())

        # Calculate total records in failed chunks
        failed_records = sum(len(fc["data_values"]) for fc in failed_chunks)
        current_run.log_warning(f"Total failed records: {failed_records}/{total_records}")
    else:
        current_run.log_info("✓ All chunks posted successfully")

    # Log final aggregated results
    current_run.log_info(
        f"Total Results - Imported: {aggregated_results['imported']}, "
        f"Updated: {aggregated_results['updated']}, "
        f"Ignored: {aggregated_results['ignored']}, "
        f"Failed chunks: {len(failed_chunks)}"
    )

    # Log details about ignored records if any
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
                remaining = len(conflicts) - 10
                current_run.log_warning(f"  ... and {remaining} more conflicts")
        else:
            current_run.log_warning(
                f"Found {aggregated_results['ignored']} ignored records but no conflict details"
            )

    # Return aggregated results in expected format
    return {
        "status": "SUCCESS",
        "imported": aggregated_results["imported"],
        "updated": aggregated_results["updated"],
        "ignored": aggregated_results["ignored"],
        "deleted": aggregated_results["deleted"],
        "conflicts": aggregated_results["conflicts"],
    }


def _get_response_value_errors(response: requests.Response, chunk: list | None) -> dict | None:
    """Collect relevant data for error logs.

    Returns:
        dict | None: A dictionary containing relevant error data, or None if no errors are found.
    """
    if response is None:
        return None

    if len(chunk) == 0 or chunk is None:
        return None

    try:
        out = {}
        for k in ["responseType", "status", "description", "importCount", "dataSetComplete"]:
            out[k] = response.get(k)
        if response.get("conflicts"):
            out["rejected_datapoints"] = []
            for i in response.get("rejectedIndexes", []):
                out["rejected_datapoints"].append(chunk[i])
            out["conflicts"] = {}
            for conflict in response["conflicts"]:
                out["conflicts"]["object"] = conflict.get("object")
                out["conflicts"]["objects"] = conflict.get("objects")
                out["conflicts"]["value"] = conflict.get("value")
                out["conflicts"]["errorCode"] = conflict.get("errorCode")
        return out
    except AttributeError:
        return None


def check_datasets_associated(target_dhis2, payload):
    unique_des = set(dv["dataElement"] for dv in payload)
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


def generate_summary(
    transform_stats: dict[str, Any],
    post_results: dict[str, Any],
    dry_run: bool,
) -> None:
    """Generate pipeline execution summary."""
    current_run.log_info("Generating pipeline summary...")

    # Create summary
    summary = {
        "pipeline": "dhis2_to_dhis2_data_elements",
        "execution_time": datetime.now().isoformat(),
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
            "mapped_org_units": transform_stats.get("mapped_org_units", 0),
            "unmapped_org_units": transform_stats.get("unmapped_org_units", 0),
        },
        "import": {
            "imported": post_results.get("imported", 0),
            "updated": post_results.get("updated", 0),
            "ignored": post_results.get("ignored", 0),
            "conflicts": post_results.get("conflicts", []),
        },
    }

    # Export summary
    output_dir = Path(workspace.files_path) / "pipelines" / "dhis2_to_dhis2_data_elements"
    output_dir /= datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_file = output_dir / "pipeline_summary.json"
    with summary_file.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    current_run.add_file_output(summary_file.as_posix())
    current_run.log_info(f"✓ Pipeline summary saved to {summary_file}")

    # Log final summary
    current_run.log_info("=== PIPELINE SUMMARY ===")
    current_run.log_info(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    current_run.log_info(f"Extracted: {summary['extraction']['original_records']} records")
    current_run.log_info(f"Transformed: {summary['extraction']['final_records']} records")
    current_run.log_info(f"Imported: {summary['import']['imported']} records")
    current_run.log_info(f"Updated: {summary['import']['updated']} records")
    current_run.log_info(f"Ignored: {summary['import']['ignored']} records")

    current_run.log_info("========================")


if __name__ == "__main__":
    dhis2_to_dhis2_data_elements()
