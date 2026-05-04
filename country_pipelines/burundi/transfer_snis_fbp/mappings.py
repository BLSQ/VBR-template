from typing import Any

import polars as pl
from openhexa.sdk import current_run
from openhexa.toolbox.dhis2 import DHIS2

import config
from api import get_ccs_for_de, get_cocs_for_cc
from utils import coc_is_valid_for_de, validate_and_transform


def prepare_data_value_payload(data_values: pl.DataFrame, client: DHIS2) -> list[dict[str, Any]]:
    """Prepare data values for DHIS2 API payload.

    Returns:
        list[dict[str, Any]]: List of validated data value dictionaries for the API.
    """
    missing_columns = [col for col in config.column_mapping if col not in data_values.columns]
    if missing_columns:
        current_run.log_error(f"Missing required columns: {missing_columns}")
        raise ValueError(f"Missing required columns: {missing_columns}")

    data_values = data_values.rename(config.column_mapping)
    payload = data_values.select(list(config.column_mapping.values())).to_dicts()

    value_types = {}
    de_to_cc = get_ccs_for_de(
        client, sorted({item["dataElement"] for item in payload if "dataElement" in item})
    )
    cc_to_coc = get_cocs_for_cc(client, sorted(set(de_to_cc.values())))

    valid_payload = []
    for item in payload:
        if any(value is None for value in item.values()):
            current_run.log_info(f"Skipping item with None values: {item}")
            continue
        if coc_is_valid_for_de(item, de_to_cc, cc_to_coc):
            value = validate_and_transform(item, client, value_types)
            if value is not None:
                item["value"] = value
                valid_payload.append(item)

    return valid_payload
