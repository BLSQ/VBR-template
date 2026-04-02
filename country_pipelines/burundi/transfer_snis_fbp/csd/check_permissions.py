"""Check user permissions and verify data existence."""

from openhexa.toolbox.dhis2 import DHIS2
import requests
from requests.auth import HTTPBasicAuth

SOURCE_URL = "https://dhis2.snissmali.org/dhis"
SOURCE_USER = "entrepotpalu"
SOURCE_PASS = "DHIS2M@li2025!"
DATASET_ID = "Z9xDie9PnuD"

print("=" * 70)
print("Checking User Permissions and Data Existence")
print("=" * 70)

source = DHIS2(url=SOURCE_URL, username=SOURCE_USER, password=SOURCE_PASS)
auth = HTTPBasicAuth(SOURCE_USER, SOURCE_PASS)

# 1. Check current user info
print("\n1. Current User Info:")
print("-" * 70)
try:
    me = source.api.get("me", params={"fields": "id,username,userRoles[id,name]"})
    print(f"  Username: {me.get('username')}")
    print(f"  User ID: {me.get('id')}")
    print(f"  Roles:")
    for role in me.get('userRoles', []):
        print(f"    - {role.get('name')}")
except Exception as e:
    print(f"  ✗ Error: {e}")

# 2. Check dataset details
print("\n2. Dataset Details:")
print("-" * 70)
try:
    dataset = source.api.get(
        f"dataSets/{DATASET_ID}",
        params={"fields": "id,name,periodType,access,dataSetElements[dataElement[id,name]]"}
    )
    print(f"  Name: {dataset.get('name')}")
    print(f"  Period Type: {dataset.get('periodType')}")
    print(f"  Access:")
    access = dataset.get('access', {})
    if access:
        print(f"    - Read: {access.get('read', False)}")
        print(f"    - Write: {access.get('write', False)}")
        print(f"    - Data read: {access.get('data', {}).get('read', False)}")
        print(f"    - Data write: {access.get('data', {}).get('write', False)}")
    print(f"  Data Elements: {len(dataset.get('dataSetElements', []))}")
    if len(dataset.get('dataSetElements', [])) > 0:
        print(f"    First 3:")
        for de in dataset.get('dataSetElements', [])[:3]:
            print(f"      - {de['dataElement']['id']}: {de['dataElement']['name']}")
except Exception as e:
    print(f"  ✗ Error: {e}")

# 3. Try to query data directly using analytics API (alternative)
print("\n3. Try Analytics API (alternative data access):")
print("-" * 70)
try:
    # Get first data element from dataset
    dataset = source.api.get(
        f"dataSets/{DATASET_ID}",
        params={"fields": "dataSetElements[dataElement[id]]"}
    )
    data_elements = [de['dataElement']['id'] for de in dataset.get('dataSetElements', [])]

    if len(data_elements) > 0:
        de_id = data_elements[0]
        print(f"  Testing with data element: {de_id}")

        # Try analytics API
        params = {
            "dimension": f"dx:{de_id}",
            "dimension": "pe:202410",
            "filter": f"ou:LEVEL-1",  # Country level
        }

        try:
            response = requests.get(
                f"{SOURCE_URL}/api/analytics",
                params=params,
                auth=auth,
                timeout=10
            )
            print(f"  Analytics API Status: {response.status_code}")
            if response.status_code == 200 and len(response.content) > 0:
                data = response.json()
                print(f"  ✓ Analytics returns: {len(data.get('rows', []))} rows")
            else:
                print(f"  ✗ No data or error")
        except Exception as e:
            print(f"  ✗ Error: {e}")
except Exception as e:
    print(f"  ✗ Error: {e}")

# 4. Check if there's ANY data for this dataset in the system
print("\n4. Search for ANY existing data values:")
print("-" * 70)
try:
    # Use a very broad query to see if any data exists
    response = requests.get(
        f"{SOURCE_URL}/api/dataValueSets",
        params={
            "dataSet": DATASET_ID,
            "startDate": "2020-01-01",
            "endDate": "2026-12-31",
        },
        auth=auth,
        timeout=30
    )
    print(f"  Status: {response.status_code}")
    print(f"  Content Length: {len(response.content)} bytes")

    if response.status_code == 200 and len(response.content) > 0:
        try:
            data = response.json()
            num_values = len(data.get('dataValues', []))
            print(f"  ✓ Found {num_values} data values total")
            if num_values > 0:
                sample = data['dataValues'][0]
                print(f"  Sample data value:")
                print(f"    - Data Element: {sample.get('dataElement')}")
                print(f"    - Period: {sample.get('period')}")
                print(f"    - Org Unit: {sample.get('orgUnit')}")
                print(f"    - Value: {sample.get('value')}")
        except:
            print(f"  ✗ Response is not valid JSON")
    else:
        print(f"  ✗ No data found with broad date range query")

except Exception as e:
    print(f"  ✗ Error: {e}")

print("\n" + "=" * 70)
print("Diagnosis:")
print("  If 'No data found' - the dataset truly has no data")
print("  If 'Access: Read=False' - permission issue")
print("  If errors - authentication or connection issue")
print("=" * 70)
