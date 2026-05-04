"""Diagnose the JSONDecodeError by testing the exact API call."""

import sys
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import period_from_string, get_range
from datetime import datetime

# Your exact parameters
SOURCE_URL = "https://dhis2.snissmali.org/dhis"
SOURCE_USER = "entrepotpalu"
SOURCE_PASS = "DHIS2M@li2025!"
DATASET_ID = "Z9xDie9PnuD"
START_DATE = "2022-01-01"
END_DATE = "2022-04-24"

print("=" * 60)
print("DHIS2 Dataset Extraction Diagnosis")
print("=" * 60)
print(f"Source: {SOURCE_URL}")
print(f"Dataset: {DATASET_ID}")
print(f"Date range: {START_DATE} to {END_DATE}")
print()

try:
    # Connect to source
    print("Step 1: Testing connection...")
    source = DHIS2(url=SOURCE_URL, username=SOURCE_USER, password=SOURCE_PASS)
    system_info = source.meta.system_info()
    print(f"✓ Connected (DHIS2 version {system_info.get('version')})")
    print()

    # Check dataset exists
    print("Step 2: Checking if dataset exists...")
    try:
        dataset_info = source.api.get(
            f"dataSets/{DATASET_ID}",
            params={"fields": "id,name,periodType,organisationUnits[id]"}
        )
        dataset_name = dataset_info.get('name')
        period_type = dataset_info.get('periodType')
        org_units = [ou['id'] for ou in dataset_info.get('organisationUnits', [])]

        print(f"✓ Dataset found: {dataset_name}")
        print(f"  Period type: {period_type}")
        print(f"  Org units assigned: {len(org_units)}")
        print()

        if len(org_units) == 0:
            print("✗ ERROR: Dataset has NO organization units assigned!")
            print("  This will cause the API to return empty/error response")
            sys.exit(1)

        # Convert dates to periods
        print("Step 3: Converting dates to DHIS2 periods...")
        dt = datetime.strptime(START_DATE, "%Y-%m-%d")

        # Simple period conversion based on period type
        if period_type == "Monthly":
            start_period = period_from_string(dt.strftime("%Y%m"))
            end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")
            end_period = period_from_string(end_dt.strftime("%Y%m"))
        elif period_type == "Daily":
            start_period = period_from_string(dt.strftime("%Y%m%d"))
            end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")
            end_period = period_from_string(end_dt.strftime("%Y%m%d"))
        else:
            print(f"✓ Period type: {period_type}")
            start_period = period_from_string(START_DATE.replace("-", ""))
            end_period = period_from_string(END_DATE.replace("-", ""))

        periods = get_range(start_period, end_period)
        periods_str = [str(p) for p in periods]

        print(f"✓ Generated {len(periods_str)} periods")
        print(f"  First period: {periods_str[0]}")
        print(f"  Last period: {periods_str[-1]}")
        print()

        # Test API call with minimal params
        print("Step 4: Testing API call (first period, first 3 org units)...")
        test_period = periods_str[0]
        test_org_units = org_units[:3]

        print(f"  Period: {test_period}")
        print(f"  Org units: {test_org_units}")

        # Build the exact API URL
        params = {
            "dataSet": DATASET_ID,
            "period": test_period,
            "orgUnit": test_org_units
        }

        try:
            # Make the raw API call to see the actual response
            response = source.api.session.get(
                f"{SOURCE_URL}/api/dataValueSets",
                params=params
            )

            print(f"  HTTP Status: {response.status_code}")
            print(f"  Content-Type: {response.headers.get('Content-Type', 'Unknown')}")
            print(f"  Response length: {len(response.text)} bytes")
            print()

            if response.status_code != 200:
                print(f"✗ ERROR: API returned status {response.status_code}")
                print(f"  Response preview: {response.text[:500]}")
                sys.exit(1)

            # Try to parse JSON
            try:
                data = response.json()
                print(f"✓ Valid JSON response")
                print(f"  Data values: {len(data.get('dataValues', []))}")

                if len(data.get('dataValues', [])) == 0:
                    print("\n⚠ WARNING: No data values found for this period/org units")
                    print("  This might be expected if there's no data")
                    print("  But if you expect data, check:")
                    print("    - Date range is correct")
                    print("    - Org units have data entry permissions")
                    print("    - Data has been entered for these periods")

            except ValueError as e:
                print(f"✗ ERROR: Response is not valid JSON!")
                print(f"  Error: {e}")
                print(f"  Response preview:")
                print(f"  {response.text[:1000]}")
                print()
                print("DIAGNOSIS: The DHIS2 API is returning HTML/text instead of JSON")
                print("Common causes:")
                print("  1. Invalid dataset ID")
                print("  2. No data access permissions")
                print("  3. Server error or timeout")
                print("  4. Invalid period format for this dataset type")
                sys.exit(1)

        except Exception as e:
            print(f"✗ ERROR during API call: {e}")
            raise

    except Exception as e:
        if "404" in str(e) or "not found" in str(e).lower():
            print(f"✗ ERROR: Dataset '{DATASET_ID}' not found!")
            print()
            print("Listing available datasets (first 10):")
            datasets = source.api.get("dataSets", params={"fields": "id,name", "pageSize": 10})
            for ds in datasets.get("dataSets", []):
                print(f"  {ds['id']}: {ds['name']}")
            sys.exit(1)
        else:
            raise

except Exception as e:
    print(f"\n✗ UNEXPECTED ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()
print("=" * 60)
print("✓ All checks passed! The pipeline should work.")
print("=" * 60)
