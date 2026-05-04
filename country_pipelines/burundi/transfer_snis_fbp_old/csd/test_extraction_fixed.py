"""Test the fixed extract_dataset call with datetime parameters."""

from datetime import datetime
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import extract_dataset

# Your exact parameters
SOURCE_URL = "https://dhis2.snissmali.org/dhis"
SOURCE_USER = "entrepotpalu"
SOURCE_PASS = "DHIS2M@li2025!"
DATASET_ID = "Z9xDie9PnuD"
START_DATE = "2025-01-01"
END_DATE = "2025-04-24"

print("Testing extract_dataset with datetime parameters...")
print(f"Dataset: {DATASET_ID}")
print(f"Date range: {START_DATE} to {END_DATE}\n")

try:
    # Connect
    source = DHIS2(url=SOURCE_URL, username=SOURCE_USER, password=SOURCE_PASS)
    print("✓ Connected to DHIS2")

    # Get org units
    dataset_info = source.api.get(
        f"dataSets/{DATASET_ID}", params={"fields": "organisationUnits[id]"}
    )
    org_units = [ou["id"] for ou in dataset_info.get("organisationUnits", [])]
    print(f"✓ Found {len(org_units)} org units assigned to dataset")

    # Convert to datetime (as the fixed pipeline does)
    start_datetime = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_datetime = datetime.strptime(END_DATE, "%Y-%m-%d")
    print(f"✓ Converted dates to datetime objects")

    # Extract data with datetime parameters (FIXED VERSION)
    print(f"\nExtracting data...")
    data = extract_dataset(
        dhis2=source,
        dataset=DATASET_ID,
        start_date=start_datetime,
        end_date=end_datetime,
        org_units=org_units,
    )

    print(f"\n✓ SUCCESS! Extracted {len(data)} rows")

    if len(data) > 0:
        print(f"\nDataframe info:")
        print(f"  Columns: {list(data.columns)}")
        print(f"  Unique data elements: {data['data_element_id'].n_unique()}")
        print(f"  Unique org units: {data['organisation_unit_id'].n_unique()}")
        print(f"  Unique periods: {data['period'].n_unique()}")
        print(f"\nFirst few rows:")
        print(data.head())
    else:
        print("\n⚠ No data found (this might be expected if no data exists for this period)")
        print("  The extraction worked without errors, just no data returned")

except Exception as e:
    print(f"\n✗ ERROR: {e}")
    import traceback

    traceback.print_exc()
