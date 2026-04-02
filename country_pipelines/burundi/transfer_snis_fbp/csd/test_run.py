"""Test script to run the pipeline with Mali parameters."""
import os
os.chdir("/home/jhubinon/openhexa-templates-ds/dhis2_to_dhis2_data_elements")

from openhexa.sdk import DHIS2Connection
from pipeline import dhis2_to_dhis2_data_elements

# Create connections
source_connection = DHIS2Connection(
    url="https://dhis2.snissmali.org/dhis",
    username="entrepotpalu",
    password="DHIS2M@li2025!"
)

target_connection = DHIS2Connection(
    url="https://nmdr-mali.bluesquare.org",
    username="ndiaga_nmdr_mli",
    password="ghp_hfKuMzkGoPU2"
)

# Run pipeline
dhis2_to_dhis2_data_elements(
    source_connection=source_connection,
    target_connection=target_connection,
    dataset_id="LNjgEf5HJKm",
    mapping_file="2_ok_mapping_cas_confirme_palu_grave.json",
    start_date="2025-01-01",
    end_date="2025-03-01",
    use_relative_dates=True,
    days_back=60,
    different_org_units=False,
    dry_run=True
)
