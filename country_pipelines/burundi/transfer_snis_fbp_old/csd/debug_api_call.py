"""Debug the actual DHIS2 API call to see the real error."""

from datetime import datetime
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import period_from_string, get_range
import requests

SOURCE_URL = "https://dhis2.snissmali.org/dhis"
SOURCE_USER = "entrepotpalu"
SOURCE_PASS = "DHIS2M@li2025!"
DATASET_ID = "Z9xDie9PnuD"

# Use dates where data exists (October 2024 to September 2025)
START_DATE = "2024-10-01"
END_DATE = "2025-09-30"

print("=" * 70)
print("Debugging DHIS2 API Call")
print("=" * 70)
print(f"Dataset: {DATASET_ID}")
print(f"Date range: {START_DATE} to {END_DATE}")
print()

try:
    source = DHIS2(url=SOURCE_URL, username=SOURCE_USER, password=SOURCE_PASS)
    print("✓ Connected to DHIS2\n")

    # Get dataset info
    dataset_info = source.api.get(
        f"dataSets/{DATASET_ID}",
        params={"fields": "organisationUnits[id],periodType"}
    )
    period_type = dataset_info.get("periodType")
    org_units = [ou["id"] for ou in dataset_info.get("organisationUnits", [])]

    print(f"Dataset info:")
    print(f"  Period type: {period_type}")
    print(f"  Org units: {len(org_units)}")
    print()

    # Generate periods
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

    print(f"Generated periods:")
    print(f"  Count: {len(periods_str)}")
    print(f"  Range: {periods_str[0]} to {periods_str[-1]}")
    print(f"  All periods: {periods_str}")
    print()

    # Test 1: Use data_value_sets.get() and catch the full error
    print("-" * 70)
    print("Test 1: Using data_value_sets.get() with all org units")
    print("-" * 70)
    try:
        result = source.data_value_sets.get(
            datasets=[DATASET_ID],
            periods=periods_str,
            org_units=org_units,
        )
        print(f"✓ SUCCESS!")
        print(f"  Type: {type(result)}")
        print(f"  Length: {len(result) if isinstance(result, list) else 'N/A'}")
        if isinstance(result, list) and len(result) > 0:
            print(f"  First item keys: {list(result[0].keys())}")
            print(f"  Sample: {result[0]}")
    except Exception as e:
        print(f"✗ ERROR: {type(e).__name__}: {e}")
        print()
        print("Full traceback:")
        import traceback
        traceback.print_exc()
        print()

    # Test 2: Try with fewer org units
    print("-" * 70)
    print("Test 2: Using data_value_sets.get() with first 10 org units")
    print("-" * 70)
    try:
        result = source.data_value_sets.get(
            datasets=[DATASET_ID],
            periods=periods_str[:3],  # Only first 3 periods
            org_units=org_units[:10],  # Only first 10 org units
        )
        print(f"✓ SUCCESS!")
        print(f"  Data values: {len(result) if isinstance(result, list) else 'N/A'}")
    except Exception as e:
        print(f"✗ ERROR: {type(e).__name__}: {e}")

    # Test 3: Direct API call to see raw response
    print()
    print("-" * 70)
    print("Test 3: Direct API call with dhis2.api.get()")
    print("-" * 70)

    # Build params as data_value_sets.get() would
    params = {
        "dataSet": DATASET_ID,
        "period": periods_str[:1],  # Just first period
        "orgUnit": org_units[:5],   # Just first 5 org units
    }

    print(f"API URL: {SOURCE_URL}/api/dataValueSets")
    print(f"Params: {params}")
    print()

    try:
        # Use session directly to see raw response
        response = source.api.session.get(
            f"{SOURCE_URL}/api/dataValueSets",
            params=params
        )

        print(f"HTTP Status: {response.status_code}")
        print(f"Content-Type: {response.headers.get('Content-Type')}")
        print(f"Content-Length: {len(response.content)} bytes")
        print(f"Response text length: {len(response.text)} chars")
        print()

        if response.status_code != 200:
            print(f"Response text:")
            print(response.text[:1000])
        else:
            if len(response.text) == 0:
                print("✗ Empty response (0 bytes) - this is the problem!")
            else:
                try:
                    data = response.json()
                    print(f"✓ Valid JSON")
                    print(f"  Keys: {list(data.keys())}")
                    if 'dataValues' in data:
                        print(f"  Data values: {len(data.get('dataValues', []))}")
                        if len(data.get('dataValues', [])) > 0:
                            print(f"  Sample: {data['dataValues'][0]}")
                except Exception as json_err:
                    print(f"✗ JSON parse error: {json_err}")
                    print(f"Response preview: {response.text[:500]}")

    except Exception as e:
        print(f"✗ Direct API call failed: {e}")
        import traceback
        traceback.print_exc()

    # Test 4: Try dhis2.api.get() wrapper
    print()
    print("-" * 70)
    print("Test 4: Using dhis2.api.get() wrapper")
    print("-" * 70)
    try:
        result = source.api.get("dataValueSets", params=params)
        print(f"✓ SUCCESS via api.get()")
        print(f"  Type: {type(result)}")
        print(f"  Keys: {list(result.keys()) if isinstance(result, dict) else 'N/A'}")
    except Exception as e:
        print(f"✗ ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

except Exception as e:
    print(f"\n✗ UNEXPECTED ERROR: {e}")
    import traceback
    traceback.print_exc()
