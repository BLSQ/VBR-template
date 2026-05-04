max_org_unit_per_request = 20
max_periods_per_request = 5
de_chunk = 200
cc_chunk = 100
chunk_size_post = 500
use_cache = False

column_mapping = {
    "data_element_id": "dataElement",
    "organisation_unit_id": "orgUnit",
    "period": "period",
    "category_option_combo_id": "categoryOptionCombo",
    "value": "value",
    "attribute_option_combo_id": "attributeOptionCombo",
}


pipeline_input = "pipelines/transfer_snis_fpb/mapping_files"
