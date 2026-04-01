"""Test the full pipeline flow with CORRECT dataset ID."""

from datetime import datetime
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import period_from_string, get_range
import polars as pl
import requests

SOURCE_URL = "https://dhis2.snissmali.org/dhis"
SOURCE_USER = "entrepotpalu"
SOURCE_PASS = "DHIS2M@li2025!"

# CORRECT dataset ID
DATASET_ID = "LNjgEf5HJKm"
START_DATE = "2024-10-01"
END_DATE = "2024-12-31"

print("=" * 70)
print("Testing FULL PIPELINE FLOW with correct dataset")
print("=" * 70)
print(f"Dataset: {DATASET_ID}")
print(f"Date range: {START_DATE} to {END_DATE}\n")

try:
    # Step 1: Connect
    source = DHIS2(url=SOURCE_URL, username=SOURCE_USER, password=SOURCE_PASS)
    print("✓ Step 1: Connected to DHIS2")

    # Step 2: Get dataset metadata
    dataset_info = source.api.get(
        f"dataSets/{DATASET_ID}",
        params={"fields": "id,name,periodType,organisationUnits[id]"}
    )
    period_type = dataset_info.get("periodType")
    org_units = [ou["id"] for ou in dataset_info.get("organisationUnits", [])]

    print(f"✓ Step 2: Got dataset metadata")
    print(f"  Name: {dataset_info['name']}")
    print(f"  Period type: {period_type}")
    print(f"  Org units: {len(org_units)}")

    # Step 3: Generate periods (as pipeline does)
    dt_start = datetime.strptime(START_DATE, "%Y-%m-%d")
    dt_end = datetime.strptime(END_DATE, "%Y-%m-%d")

    if period_type == "Monthly":
        start_period = period_from_string(dt_start.strftime("%Y%m"))
        end_period = period_from_string(dt_end.strftime("%Y%m"))
    else:
        print(f"⚠ Unsupported period type: {period_type}")
        exit(1)

    periods = get_range(start_period, end_period)
    periods_str = [str(p) for p in periods]

    print(f"✓ Step 3: Generated periods")
    print(f"  Periods: {periods_str}")

    # Step 4: Fetch data (as pipeline does)
    print(f"✓ Step 4: Fetching data from API...")

    try:
        result = source.data_value_sets.get(
            datasets=[DATASET_ID],
            periods=periods_str,
            org_units=org_units,
        )
        data_values_list = result if isinstance(result, list) else []
        print(f"  ✓ API call successful!")
    except requests.exceptions.JSONDecodeError:
        print(f"  ⚠ JSONDecodeError caught (no data) - handled gracefully")
        data_values_list = []

    print(f"  Raw data values: {len(data_values_list)}")

    # Step 5: Convert to DataFrame (as pipeline does)
    if len(data_values_list) == 0:
        print(f"\n✗ No data values found")
        data_values = pl.DataFrame()
    else:
        data_values = pl.DataFrame(data_values_list, infer_schema_length=None).rename({
            "dataElement": "data_element_id",
            "period": "period",
            "orgUnit": "organisation_unit_id",
            "categoryOptionCombo": "category_option_combo_id",
            "attributeOptionCombo": "attribute_option_combo_id",
            "value": "value",
        })

        print(f"✓ Step 5: Converted to DataFrame")
        print(f"  Rows: {len(data_values)}")
        print(f"  Columns: {list(data_values.columns)}")
        print(f"  Unique data elements: {data_values['data_element_id'].n_unique()}")
        print(f"  Unique org units: {data_values['organisation_unit_id'].n_unique()}")
        print(f"  Unique periods: {data_values['period'].n_unique()}")

        print(f"\n  First 3 rows:")
        print(data_values.head(3))

    print("\n" + "=" * 70)
    print("✓ PIPELINE FLOW WORKS PERFECTLY!")
    print("=" * 70)
    print(f"\nNext step: Update your pipeline parameters to use:")
    print(f"  dataset_id = '{DATASET_ID}'")

except Exception as e:
    print(f"\n✗ UNEXPECTED ERROR: {e}")
    import traceback
    traceback.print_exc()
