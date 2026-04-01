"""Test using extract_dataset with datetime objects."""

from datetime import datetime
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import extract_dataset

SOURCE_URL = "https://dhis2.snissmali.org/dhis"
SOURCE_USER = "entrepotpalu"
SOURCE_PASS = "DHIS2M@li2025!"
DATASET_ID = "Z9xDie9PnuD"
START_DATE = "2022-01-01"
END_DATE = "2022-04-24"

print("Testing extract_dataset with datetime objects...")
print(f"Dataset: {DATASET_ID}")
print(f"Date range: {START_DATE} to {END_DATE}\n")

try:
    source = DHIS2(url=SOURCE_URL, username=SOURCE_USER, password=SOURCE_PASS)
    print("✓ Connected to DHIS2")

    # Get org units
    dataset_info = source.api.get(
        f"dataSets/{DATASET_ID}",
        params={"fields": "organisationUnits[id]"}
    )
    org_units = [ou["id"] for ou in dataset_info.get("organisationUnits", [])]
    print(f"✓ Found {len(org_units)} org units")

    # Convert to datetime
    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")

    print(f"\nCalling extract_dataset with datetime objects...")
    data = extract_dataset(
        dhis2=source,
        dataset=DATASET_ID,
        start_date=start_dt,
        end_date=end_dt,
        org_units=org_units
    )

    print(f"\n✓ SUCCESS!")
    print(f"  Extracted {len(data)} rows")

    if len(data) > 0:
        print(f"  Columns: {list(data.columns)}")
        print(f"\nFirst few rows:")
        print(data.head())
    else:
        print("\n⚠ No data found (but no error!)")

except Exception as e:
    print(f"\n✗ ERROR: {e}")
    import traceback
    traceback.print_exc()
