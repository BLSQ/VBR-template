"""Test different API parameter formats to find the right one."""

import requests
from requests.auth import HTTPBasicAuth

SOURCE_URL = "https://dhis2.snissmali.org/dhis"
SOURCE_USER = "entrepotpalu"
SOURCE_PASS = "DHIS2M@li2025!"
DATASET_ID = "Z9xDie9PnuD"

auth = HTTPBasicAuth(SOURCE_USER, SOURCE_PASS)
base_url = f"{SOURCE_URL}/api/dataValueSets"

# Test different parameter formats
test_cases = [
    {
        "name": "Single org unit as string",
        "params": {
            "dataSet": DATASET_ID,
            "period": "202201",
            "orgUnit": "Acmsnp3Az3K"
        }
    },
    {
        "name": "Multiple org units (list in params)",
        "params": {
            "dataSet": DATASET_ID,
            "period": "202201",
            "orgUnit": ["Acmsnp3Az3K", "hsmxhnNtd0F"]
        }
    },
    {
        "name": "Start/end date instead of period",
        "params": {
            "dataSet": DATASET_ID,
            "startDate": "2022-01-01",
            "endDate": "2022-01-31",
            "orgUnit": "Acmsnp3Az3K"
        }
    },
    {
        "name": "Children parameter with parent org unit",
        "params": {
            "dataSet": DATASET_ID,
            "period": "202201",
            "orgUnit": "Acmsnp3Az3K",
            "children": "true"
        }
    },
]

print("Testing different API parameter formats...")
print("=" * 70)

for i, test in enumerate(test_cases, 1):
    print(f"\nTest {i}: {test['name']}")
    print(f"Params: {test['params']}")

    try:
        response = requests.get(base_url, params=test['params'], auth=auth, timeout=10)

        print(f"  Status: {response.status_code}")
        print(f"  Content-Type: {response.headers.get('Content-Type', 'Unknown')}")
        print(f"  Response size: {len(response.text)} bytes")

        if response.status_code == 200:
            if len(response.text) > 0:
                try:
                    data = response.json()
                    num_values = len(data.get('dataValues', []))
                    print(f"  ✓ Valid JSON - {num_values} data values")
                    if num_values > 0:
                        print(f"    Sample: {data['dataValues'][0]}")
                except:
                    print(f"  ✗ Invalid JSON")
                    print(f"    Preview: {response.text[:200]}")
            else:
                print(f"  ✗ Empty response")
        else:
            print(f"  ✗ Error: {response.text[:200]}")

    except Exception as e:
        print(f"  ✗ Exception: {e}")

print("\n" + "=" * 70)
print("\nNow testing with DHIS2 toolbox extract_dataset function...")

from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import extract_dataset

try:
    source = DHIS2(url=SOURCE_URL, username=SOURCE_USER, password=SOURCE_PASS)

    # Get org units
    dataset_info = source.api.get(
        f"dataSets/{DATASET_ID}",
        params={"fields": "organisationUnits[id]"}
    )
    org_units = [ou['id'] for ou in dataset_info.get('organisationUnits', [])][:5]

    print(f"Using extract_dataset with:")
    print(f"  Dataset: {DATASET_ID}")
    print(f"  Periods: ['202201']")
    print(f"  Org units: {len(org_units)} (first 5 from dataset)")

    data = extract_dataset(
        dhis2=source,
        dataset=DATASET_ID,
        periods=["202201"],
        org_units=org_units
    )

    print(f"  ✓ Success! Extracted {len(data)} rows")
    if len(data) > 0:
        print(f"    Columns: {data.columns}")
        print(f"    Preview:\n{data.head()}")
    else:
        print(f"    ⚠ No data values found (might be expected if no data exists)")

except Exception as e:
    print(f"  ✗ Error: {e}")
    import traceback
    traceback.print_exc()
