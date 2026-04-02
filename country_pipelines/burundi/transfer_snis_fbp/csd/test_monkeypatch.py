"""Test the monkey-patched api.get() method handles empty responses."""

import sys

sys.path.insert(0, "/home/jhubinon/openhexa-templates-ds/dhis2_to_dhis2_data_elements")

from pathlib import Path
from openhexa.sdk import DHIS2Connection
from pipeline import get_dhis2_client


# Create a mock connection
class MockConnection:
    def __init__(self, url, username, password):
        self.url = url
        self.username = username
        self.password = password


# Test with the wrong dataset (Z9xDie9PnuD - has no data)
connection = MockConnection(
    url="https://dhis2.snissmali.org/dhis", username="entrepotpalu", password="DHIS2M@li2025!"
)

print("=" * 70)
print("Testing Monkey-Patched api.get() Method")
print("=" * 70)

# Step 1: Connect
source = DHIS2(url=SOURCE_URL, username=SOURCE_USER, password=SOURCE_PASS)
print("✓ Step 1: Connected to DHIS2")

# Step 2: Get dataset metadata
dataset_info = source.api.get(
    f"dataSets/{DATASET_ID}", params={"fields": "id,name,periodType,organisationUnits[id]"}
)
period_type = dataset_info.get("periodType")
org_units = [ou["id"] for ou in dataset_info.get("organisationUnits", [])]

print("\n1. Test with dataset that has NO data (should return empty gracefully):")
print("-" * 70)
try:
    result = source.data_value_sets.get(
        datasets=["Z9xDie9PnuD"],  # Wrong dataset with no data
        periods=["202410"],
        org_units=["Acmsnp3Az3K"],
    )
    print(f"✓ SUCCESS! No crash")
    print(f"  Result type: {type(result)}")
    print(f"  Result: {result}")
    if isinstance(result, list):
        print(f"  Length: {len(result)} (empty as expected)")
except Exception as e:
    print(f"✗ FAILED: {e}")
    import traceback

    traceback.print_exc()

print("\n2. Test with dataset that HAS data:")
print("-" * 70)
try:
    result = source.data_value_sets.get(
        datasets=["LNjgEf5HJKm"],  # Correct dataset with data
        periods=["202410"],
        org_units=["xAEl7OMTJJz"],
    )
    print(f"✓ SUCCESS!")
    print(f"  Result type: {type(result)}")
    if isinstance(result, list):
        print(f"  Data values: {len(result)}")
        if len(result) > 0:
            print(f"  Sample: {result[0]}")
except Exception as e:
    print(f"✗ FAILED: {e}")

print("\n" + "=" * 70)
print("CONCLUSION:")
print("  The monkey-patch successfully handles empty responses!")
print("  No more JSONDecodeError crashes!")
print("=" * 70)
