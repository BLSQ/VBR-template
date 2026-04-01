"""Test different parameter formats for DHIS2 API."""

from openhexa.toolbox.dhis2 import DHIS2
import requests
from requests.auth import HTTPBasicAuth

SOURCE_URL = "https://dhis2.snissmali.org/dhis"
SOURCE_USER = "entrepotpalu"
SOURCE_PASS = "DHIS2M@li2025!"
DATASET_ID = "Z9xDie9PnuD"

source = DHIS2(url=SOURCE_URL, username=SOURCE_USER, password=SOURCE_PASS)
auth = HTTPBasicAuth(SOURCE_USER, SOURCE_PASS)

# Get one org unit
dataset_info = source.api.get(
    f"dataSets/{DATASET_ID}",
    params={"fields": "organisationUnits[id]"}
)
org_units = [ou["id"] for ou in dataset_info.get("organisationUnits", [])]

print(f"Testing different parameter formats for DHIS2 dataValueSets API")
print(f"Dataset: {DATASET_ID}")
print(f"Using org units: {org_units[:3]}")
print("=" * 70)

# Test different ways to pass multiple periods and org units
test_cases = [
    {
        "name": "Single period, single org unit (baseline)",
        "params": {
            "dataSet": DATASET_ID,
            "period": "202410",
            "orgUnit": org_units[0]
        }
    },
    {
        "name": "List of periods (Python list)",
        "params": {
            "dataSet": DATASET_ID,
            "period": ["202410", "202411"],
            "orgUnit": org_units[0]
        }
    },
    {
        "name": "Comma-separated periods",
        "params": {
            "dataSet": DATASET_ID,
            "period": "202410,202411",
            "orgUnit": org_units[0]
        }
    },
    {
        "name": "Multiple period parameters",
        "url_manual": f"{SOURCE_URL}/api/dataValueSets?dataSet={DATASET_ID}&period=202410&period=202411&orgUnit={org_units[0]}"
    },
    {
        "name": "List of org units (Python list)",
        "params": {
            "dataSet": DATASET_ID,
            "period": "202410",
            "orgUnit": org_units[:3]
        }
    },
    {
        "name": "Comma-separated org units",
        "params": {
            "dataSet": DATASET_ID,
            "period": "202410",
            "orgUnit": ",".join(org_units[:3])
        }
    },
    {
        "name": "Multiple orgUnit parameters",
        "url_manual": f"{SOURCE_URL}/api/dataValueSets?dataSet={DATASET_ID}&period=202410&orgUnit={org_units[0]}&orgUnit={org_units[1]}"
    },
]

for i, test in enumerate(test_cases, 1):
    print(f"\nTest {i}: {test['name']}")
    print("-" * 70)

    try:
        if "url_manual" in test:
            # Manual URL construction
            response = requests.get(test["url_manual"], auth=auth, timeout=10)
            print(f"  URL: {test['url_manual']}")
        else:
            # Using params dict
            response = requests.get(
                f"{SOURCE_URL}/api/dataValueSets",
                params=test["params"],
                auth=auth,
                timeout=10
            )
            print(f"  Actual URL: {response.url}")

        print(f"  Status: {response.status_code}")
        print(f"  Content-Length: {len(response.content)} bytes")

        if response.status_code == 200:
            if len(response.content) == 0:
                print(f"  ✗ Empty response")
            else:
                try:
                    data = response.json()
                    num_values = len(data.get('dataValues', []))
                    print(f"  ✓ Valid JSON - {num_values} data values")
                    if num_values > 0:
                        print(f"    Sample keys: {list(data['dataValues'][0].keys())}")
                except:
                    print(f"  ✗ Invalid JSON")
                    print(f"    Preview: {response.text[:200]}")
        else:
            print(f"  ✗ HTTP {response.status_code}")
            print(f"    Error: {response.text[:200]}")

    except Exception as e:
        print(f"  ✗ Exception: {e}")

print("\n" + "=" * 70)
print("\nConclusion: The format that works will show 'Valid JSON' with data values")
