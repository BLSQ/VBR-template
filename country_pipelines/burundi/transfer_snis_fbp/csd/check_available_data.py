"""Check what data is actually available for this dataset."""

import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta

SOURCE_URL = "https://dhis2.snissmali.org/dhis"
SOURCE_USER = "entrepotpalu"
SOURCE_PASS = "DHIS2M@li2025!"
DATASET_ID = "Z9xDie9PnuD"

auth = HTTPBasicAuth(SOURCE_USER, SOURCE_PASS)

print("Checking what data is available for dataset Z9xDie9PnuD...")
print("=" * 70)

# Get dataset info
response = requests.get(
    f"{SOURCE_URL}/api/dataSets/{DATASET_ID}",
    params={"fields": "id,name,periodType,organisationUnits[id,name]"},
    auth=auth
)
dataset = response.json()
print(f"\nDataset: {dataset['name']}")
print(f"Period type: {dataset['periodType']}")
print(f"Org units: {len(dataset['organisationUnits'])}")

# Get a few org units
sample_orgs = dataset['organisationUnits'][:3]
print(f"\nSample org units:")
for org in sample_orgs:
    print(f"  - {org['id']}: {org['name']}")

# Try different time periods to find where data exists
print(f"\nSearching for data across different periods...")
print("-" * 70)

# Test periods from recent to older
test_periods = []
now = datetime.now()

# Try last 24 months
for i in range(24):
    date = now - timedelta(days=30*i)
    period = date.strftime("%Y%m")
    if period not in test_periods:
        test_periods.append(period)

found_data = []

for period in test_periods[:12]:  # Check last 12 months
    # Try with first org unit
    org_id = sample_orgs[0]['id']

    try:
        response = requests.get(
            f"{SOURCE_URL}/api/dataValueSets",
            params={
                "dataSet": DATASET_ID,
                "period": period,
                "orgUnit": org_id
            },
            auth=auth,
            timeout=10
        )

        if response.status_code == 200 and len(response.text) > 0:
            try:
                data = response.json()
                num_values = len(data.get('dataValues', []))
                if num_values > 0:
                    found_data.append((period, org_id, num_values))
                    print(f"  {period}: ✓ {num_values} values (org: {org_id})")
            except:
                pass
        elif response.status_code != 200:
            print(f"  {period}: HTTP {response.status_code}")

    except Exception as e:
        print(f"  {period}: Error - {e}")

print("\n" + "=" * 70)

if found_data:
    print(f"\n✓ Found data in {len(found_data)} periods:")
    for period, org, count in found_data[:5]:
        print(f"  Period {period}: {count} values")

    print(f"\nRecommendation: Use a period where data exists")
    latest_period = found_data[0][0]
    print(f"  Latest period with data: {latest_period}")
    print(f"  As date: {latest_period[:4]}-{latest_period[4:]}-01")
else:
    print("\n✗ No data found in recent periods (last 12 months)")
    print("\nPossible reasons:")
    print("  1. Data has not been entered for this dataset")
    print("  2. Data is older than 12 months")
    print("  3. User doesn't have access to view data")
    print("  4. Dataset is not actively used")
    print("\nTry:")
    print("  - Check with data managers when data was last entered")
    print("  - Verify your user has data view permissions")
    print("  - Try a different dataset that's actively used")
