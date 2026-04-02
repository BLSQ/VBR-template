"""Check the actual 409 error message."""

import requests
from requests.auth import HTTPBasicAuth

SOURCE_URL = "https://dhis2.snissmali.org/dhis"
SOURCE_USER = "entrepotpalu"
SOURCE_PASS = "DHIS2M@li2025!"
DATASET_ID = "Z9xDie9PnuD"

auth = HTTPBasicAuth(SOURCE_USER, SOURCE_PASS)

print("Checking HTTP 409 error details...")
print("=" * 70)

response = requests.get(
    f"{SOURCE_URL}/api/dataValueSets",
    params={
        "dataSet": DATASET_ID,
        "startDate": "2024-10-01",
        "endDate": "2024-10-31",
    },
    auth=auth,
    timeout=30
)

print(f"HTTP Status: {response.status_code}")
print(f"Content-Type: {response.headers.get('Content-Type')}")
print(f"Content-Length: {len(response.content)} bytes")
print()
print("Response body:")
print(response.text)
print()

if response.status_code == 409:
    try:
        error_data = response.json()
        print("Parsed error:")
        import json
        print(json.dumps(error_data, indent=2))
    except:
        print("Could not parse as JSON")
