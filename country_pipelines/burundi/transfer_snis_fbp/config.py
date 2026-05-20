max_org_unit_per_request = 20
max_periods_per_request = 5
chunk_size_post = 500
use_cache = False


att_default = "HllvX50cXC0"

column_mapping = {
    "data_element_id": "dataElement",
    "organisation_unit_id": "orgUnit",
    "period": "period",
    "category_option_combo_id": "categoryOptionCombo",
    "value": "value",
    "attribute_option_combo_id": "attributeOptionCombo",
}


pipeline_input = "pipelines/transfer_snis_fbp/mapping_files"

REQUIRED_ID_COLUMNS = [
    "ds_id_snis",
    "de_id_snis",
    "coc_id_snis_mfp",
    "coc_id_snis_cam",
    "coc_id_snis_fbp",
    "de_id_fbp_dec",
    "coc_id_fbp_mfp",
    "coc_id_fbp_cam",
    "coc_id_fbp_total",
]
