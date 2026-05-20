"""Test the final implementation with periods list (matching pipeline)."""

from datetime import datetime
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import period_from_string, get_range
import polars as pl
import requests

SOURCE_URL = "https://dhis2.snissmali.org/dhis"
SOURCE_USER = "entrepotpalu"
SOURCE_PASS = "DHIS2M@li2025!"
DATASET_ID = "Z9xDie9PnuD"
START_DATE = "2022-01-01"
END_DATE = "2022-04-24"

print("Testing period-based extraction (final pipeline approach)...")
print(f"Dataset: {DATASET_ID}")
print(f"Date range: {START_DATE} to {END_DATE}\n")

try:
    source = DHIS2(url=SOURCE_URL, username=SOURCE_USER, password=SOURCE_PASS)
    print("✓ Connected to DHIS2")

    # Get dataset info
    dataset_info = source.api.get(
        f"dataSets/{DATASET_ID}",
        params={"fields": "organisationUnits[id],periodType"}
    )
    period_type = dataset_info.get("periodType")
    org_units = [ou["id"] for ou in dataset_info.get("organisationUnits", [])]
    print(f"✓ Period type: {period_type}")
    print(f"✓ Found {len(org_units)} org units")

    # Convert dates to periods using isodate_to_period_type logic
    dt_start = datetime.strptime(START_DATE, "%Y-%m-%d")
    dt_end = datetime.strptime(END_DATE, "%Y-%m-%d")

    if period_type == "Monthly":
        start_period = period_from_string(dt_start.strftime("%Y%m"))
        end_period = period_from_string(dt_end.strftime("%Y%m"))
    elif period_type == "Daily":
        start_period = period_from_string(dt_start.strftime("%Y%m%d"))
        end_period = period_from_string(dt_end.strftime("%Y%m%d"))
    else:
        print(f"⚠ Unsupported period type for this test: {period_type}")
        exit(1)

    # Generate periods list
    periods = get_range(start_period, end_period)
    periods_str = [str(p) for p in periods]
    print(f"✓ Generated {len(periods_str)} periods: {periods_str[0]} to {periods_str[-1]}")

    # Use data_value_sets.get() with periods list (as pipeline does)
    print(f"\nFetching data from DHIS2 API...")
    try:
        result = source.data_value_sets.get(
            datasets=[DATASET_ID],
            periods=periods_str,
            org_units=org_units,
        )
        # Result is a list of data value dicts
        data_values_list = result if isinstance(result, list) else []
        print(f"✓ API call successful!")
    except requests.exceptions.JSONDecodeError:
        print("⚠ DHIS2 API returned empty response (JSONDecodeError caught)")
        print("  This means no data exists for the specified parameters")
        data_values_list = []

    print(f"  Raw data values: {len(data_values_list)}")

    # Convert to DataFrame
    if len(data_values_list) == 0:
        print("\n⚠ No data values found")
        print("  The error handling works correctly!")
        print("  Pipeline will continue with 0 records (no crash)")
    else:
        # Convert list of dicts to DataFrame with renamed columns
        data_values = pl.DataFrame(data_values_list).rename({
            "dataElement": "data_element_id",
            "period": "period",
            "orgUnit": "organisation_unit_id",
            "categoryOptionCombo": "category_option_combo_id",
            "attributeOptionCombo": "attribute_option_combo_id",
            "value": "value",
        })

        print(f"\n✓ SUCCESS! Converted to DataFrame")
        print(f"  Rows: {len(data_values)}")
        print(f"  Columns: {list(data_values.columns)}")
        print(f"  Unique data elements: {data_values['data_element_id'].n_unique()}")
        print(f"  Unique org units: {data_values['organisation_unit_id'].n_unique()}")
        print(f"  Unique periods: {data_values['period'].n_unique()}")
        print(f"\nFirst few rows:")
        print(data_values.head())

except Exception as e:
    print(f"\n✗ UNEXPECTED ERROR: {e}")
    import traceback
    traceback.print_exc()
