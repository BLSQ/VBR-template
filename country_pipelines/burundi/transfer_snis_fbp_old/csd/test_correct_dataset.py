"""Test with the CORRECT dataset ID from the browser test."""

from openhexa.toolbox.dhis2 import DHIS2
import requests
from requests.auth import HTTPBasicAuth

SOURCE_URL = "https://dhis2.snissmali.org/dhis"
SOURCE_USER = "entrepotpalu"
SOURCE_PASS = "DHIS2M@li2025!"

# The CORRECT dataset ID from your browser test
CORRECT_DATASET_ID = "LNjgEf5HJKm"
WRONG_DATASET_ID = "Z9xDie9PnuD"

print("=" * 70)
print("Testing with CORRECT dataset ID from browser")
print("=" * 70)

source = DHIS2(url=SOURCE_URL, username=SOURCE_USER, password=SOURCE_PASS)
auth = HTTPBasicAuth(SOURCE_USER, SOURCE_PASS)

# Test 1: Check both datasets
print("\n1. Comparing both dataset IDs:")
print("-" * 70)

for ds_id in [WRONG_DATASET_ID, CORRECT_DATASET_ID]:
    try:
        ds = source.api.get(f"dataSets/{ds_id}", params={"fields": "id,name,periodType"})
        print(f"  {ds_id}: {ds['name']} ({ds['periodType']})")
    except Exception as e:
        print(f"  {ds_id}: ✗ Error - {e}")

# Test 2: Use the exact same parameters as your browser test
print("\n2. Testing browser URL parameters:")
print("-" * 70)
print(f"  Dataset: {CORRECT_DATASET_ID}")
print(f"  OrgUnit: xAEl7OMTJJz")
print(f"  Period: 202410")
print(f"  Children: False")

# Test with requests directly (as browser does)
response = requests.get(
    f"{SOURCE_URL}/api/dataValueSets",
    params={
        "dataSet": CORRECT_DATASET_ID,
        "orgUnit": "xAEl7OMTJJz",
        "period": "202410",
        "children": "False"
    },
    auth=auth,
    timeout=30
)

print(f"\n  Status: {response.status_code}")
print(f"  Content-Length: {len(response.content)} bytes")

if response.status_code == 200 and len(response.content) > 0:
    data = response.json()
    print(f"  ✓ SUCCESS! Found {len(data.get('dataValues', []))} data values")
    if len(data.get('dataValues', [])) > 0:
        print(f"  Sample data value:")
        sample = data['dataValues'][0]
        print(f"    - dataElement: {sample.get('dataElement')}")
        print(f"    - period: {sample.get('period')}")
        print(f"    - orgUnit: {sample.get('orgUnit')}")
        print(f"    - value: {sample.get('value')}")
else:
    print(f"  ✗ Failed: {response.text[:200]}")

# Test 3: Use source_dhis2.data_value_sets.get()
print("\n3. Testing with source_dhis2.data_value_sets.get():")
print("-" * 70)

try:
    result = source.data_value_sets.get(
        datasets=[CORRECT_DATASET_ID],
        periods=["202410"],
        org_units=["xAEl7OMTJJz"],
    )

    print(f"  ✓ SUCCESS!")
    print(f"  Type: {type(result)}")
    print(f"  Length: {len(result) if isinstance(result, list) else 'N/A'}")

    if isinstance(result, list) and len(result) > 0:
        print(f"  Sample item keys: {list(result[0].keys())}")
        print(f"  Sample: {result[0]}")

except Exception as e:
    print(f"  ✗ Error: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Get all org units for the correct dataset
print("\n4. Getting org units for correct dataset:")
print("-" * 70)

try:
    ds = source.api.get(
        f"dataSets/{CORRECT_DATASET_ID}",
        params={"fields": "id,name,organisationUnits[id,name]"}
    )
    org_units = ds.get('organisationUnits', [])
    print(f"  Dataset: {ds['name']}")
    print(f"  Total org units: {len(org_units)}")
    print(f"  First 5:")
    for ou in org_units[:5]:
        print(f"    - {ou['id']}: {ou['name']}")

    # Check if xAEl7OMTJJz is in the list
    ou_ids = [ou['id'] for ou in org_units]
    if "xAEl7OMTJJz" in ou_ids:
        print(f"\n  ✓ xAEl7OMTJJz is in the dataset's org units")
    else:
        print(f"\n  ⚠ xAEl7OMTJJz is NOT in the dataset's org units")

except Exception as e:
    print(f"  ✗ Error: {e}")

# Test 5: Test with multiple periods and org units
print("\n5. Testing with multiple periods and org units (pipeline style):")
print("-" * 70)

try:
    ds = source.api.get(
        f"dataSets/{CORRECT_DATASET_ID}",
        params={"fields": "organisationUnits[id]"}
    )
    all_org_units = [ou['id'] for ou in ds.get('organisationUnits', [])]

    result = source.data_value_sets.get(
        datasets=[CORRECT_DATASET_ID],
        periods=["202410", "202411", "202412"],
        org_units=all_org_units[:50],  # First 50 org units
    )

    print(f"  ✓ SUCCESS with {len(result) if isinstance(result, list) else 0} data values")

except requests.exceptions.JSONDecodeError:
    print(f"  ✗ JSONDecodeError - no data for these parameters")
except Exception as e:
    print(f"  ✗ Error: {e}")

print("\n" + "=" * 70)
print("CONCLUSION:")
print(f"  The issue was using WRONG dataset ID: {WRONG_DATASET_ID}")
print(f"  The CORRECT dataset ID is: {CORRECT_DATASET_ID}")
print("=" * 70)
