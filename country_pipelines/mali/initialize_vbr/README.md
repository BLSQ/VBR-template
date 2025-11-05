# Initialize VBR Pipeline (Mali)

## Summary

This pipeline extracts, cleans, and processes quantitative data from DHIS2 and Hesabu for VBR. It extracts the declared, validated and tariff values for all of the services and all payment modes for the specified periods. It generates CSV files and a serialized simulation environment for use in the next pipeline.

---

## Parameters

- **period**: Last period to extract data from (format: `yyyymm` or `yyyyQx`).
- **window**: Number of months to consider for extraction. Ensure the window is large enough to cover all periods you wish to extract.
- **model_name**: Name for saving the simulation environment and output files.
- **dhis_con**: Name of the DHIS2 connection. These must match the connection names configured in OpenHexa.
- **hesabu_con**: Name of the Hesabu connection. These must match the connection names configured in OpenHexa.
- **selection_provinces**: If true, only data for specified provinces is saved (provinces listed in the input JSON).
- **extract**: If true, re-extract all data from DHIS2, even if previously extracted.
- **bool_hesabu_construct**: If true, construct the Hesabu descriptor from the Hesabu instance.

---

## Inputs

- **setup.json**  
  Contains configuration for extraction. It includes:
  - The ID for the DHIS2 VBR Mali program. (which includes all of the registered centers)
  - The ID for the Hesabu VBR Mali program. (which will allow us to access its configuration)
  - For the quantitative data that we extract, mappings between the extracted column names and the standard ones.
  - For the contract data that we extract, mappings between the extracted column names and the standard ones.
  - A list of the provinces to consider (if `selection_provinces` is true). To restrict VBR to specific provinces, modify this list. (Note that this is only used if `selection_provinces` is true).

---

## Outputs

- **quantity_data.csv**  
  Per service, organizational unit, and period: declared, validated, and tariff values, plus indicators and subsidy calculations. It is saved under `data/quantity_data/`, prefixed with the model name. Once we have more data, we will want to explore this file. It will allow us to check that the data saved in DHIS2 makes sense.
- **contracts.csv**  
  Data on contracts and organizational units extracted from DHIS2. It is saved under `data/contracts/`, with the model name as a prefix.
- **service inconsistencies**  
  Information on inconsistencies between the services defined in Hesabu and in the DHIS2 dataset. It is saved as Parquet files under `data/inconsistencies/`, with the model name as a prefix.
- **orgunits inconsistencies**  
  Information on inconsistencies between the organizational units defined in Hesabu and in the DHIS2 contracts. It is saved as Parquet files under `data/inconsistencies/`, with the model name as a prefix.
- **Simulation environment (.pickle)**  
  Serialized Python objects (`GroupOrgUnits` and `Orgunit`) for use in the next pipeline (`run_vbr`). It is saved under `data/initialization_simulation/`, with the model name as a prefix.

Additionally, for each payment mode and period, the following files are saved:
- **summary file**: Per each of the payment modes and periods, we save a CSV file of the raw extracted data values (declared, validated, tariff). It is saved under `packages/`.
- **indicators file**: Per each of the payment modes, periods and indicators (declared, validated, tariff), we save a CSV file of the raw extracted data values under `packages/`.

---

## Files

- **pipeline.py**: Main script orchestrating the extraction, cleaning, and saving of data.
- **orgunit.py**: Contains the `Orgunit`, `GroupOrgUnits`, and `VBR` classes used to structure and serialize the simulation environment.
- **toolbox.py**: Utility functions for saving/loading CSV, JSON, Parquet files, extracting datasets, calculating indicators, and handling dates.

---

## Process

1. **Setup and Connections**
   - In `get_dhis2()` we establish a connection to DHIS2.
   - In `get_hesabu()` we establish a connection to Hesabu.
   - In `get_setup()` we load the setup configuration.
   - In `get_period_dicts()` we construct a dictionary that informs us which periods we need to extract data from, based on the parameters given by the user. We have the periods in both monthly and quarterly format.

2. **Fetch relevant metadata**
   - In `get_organisation_units()` we extract organizational unit metadata from DHIS2 using the specified program ID. We will use this data to extract the relevant VBR data for the correct organizational units.
   - In `fetch_contracts()` we extract the contract data for the relevant DHIS2 program. We will save the contract data to a CSV file for later use. We will use to know which organizational units have VBR contracts and their information. 

3. **Construct the Extraction Configuration**
   - In `fetch_hesabu_descriptor()` we either load or fetch the Hesabu descriptor. This is a JSON file that contains all of the structure of the payment rules and packages defined in Hesabu. We will use it to know which dataElements to extract from DHIS2.
   - In `construct_config_extraction()` we build a dictionary that will allow us to extract the relevant data from DHIS2. It contains, per each of the payment modes, the relevant dataSet, its frequency and the organizational Units linked to it for each of the three indicators (declared, validated, tariff). In order to construct it....
       - We extract from DHIS2 a list of all of the datasets with the dataElements that they contain. 
       - We iterate over the payment modes defined in the Hesabu descriptor. For each of them, we iterate over the three indicators (declared, validated, tariff). For each of the indicators, we find a list of the dataElements that contain their information. We then find the dataset that contains those dataElements, and the organizational units linked to that dataset.
       - We then make sure, for each of the payments, that we have the information for all of the relevant indicators. We also check for inconsistencies between the Hesabu descriptor, the DHIS2 datasets and the contracts. We save those inconsistencies to Parquet files for later exploration.

4. **Extract Data from DHIS2**
   - In `extract_data_values()` we extract the relevant data from DHIS2, using the configuration constructed in the previous step. 
   - We iterate over each of the payment modes and periods. For each of them, we iterate over the three indicators (declared, validated, tariff). Using their dataSet id (and its frequency and organizational units), we extract all of the relevant dataElements and their values from DHIS2. We then save this extracted data to CSV files (we will have one CSV file per period, payment mode and indicator).
   - Then, per each of the payment modes and periods, we merge the information of the three indicators together, constructing a dataframe that has one row per service, with columns indicating the values for the declared, validated and tariff values. In order to do this merge, we use a dataframe that links the dataElements for the declared, validated and tariff values to each service. We save this merged data to CSV files (one per payment mode and period). In order to merge the three indicators together:
      - We will also assume that the tariff values are at country level.
      - Since the declared values are montly and the validated values are quarterly, we will add up the relevant months for the declared values and construct a quarterly dataframe. 
   - Note that we will not necessarily extract the data twice. If the `extract` parameter is false, we will first check if the relevant CSV files already exist. If they do, we will load them instead of re-extracting them.
   - We also construct a DataFrame listing inconsistencies, such as services present in DHIS2 datasets but not defined in Hesabu.

5. **Prepare Quantity Data**
   - In `prepare_quantity_data()` we combine all extracted data into a single DataFrame. We merge this DataFrame with the contract data and standardize columns using mappings from the setup. We handle missing contract information by merging with the organizational unit pyramid.
   - We calculate additional indicators (e.g., gain from verification, subsidies). These are calculated both in this function and in `calcul_ecarts()`. If at some point we want to change the way we calculate these indicators, or add some additional ones, we might want to change these functions. 


6. **Clean Quantity Data**
   - In `clean_quantity_data()` we clean the combined quantity data by: 
      - Removing rows where both declared and validated values are zero.
      - Removing rows where declared is NaN.
      - Setting validated to zero if it is NaN.
Currently, cleaning is minimal. As more data becomes available, consider enhancing this function.

7. **Save Simulation Environment**
   - In `save_simulation_environment()` we structure and save the simulation environment for downstream VBR simulation. We will save everything into a serialized `.pickle` file. We will have a `VBR` object, constructed by `GroupOrgUnits` objects (either a national or provincial level), which in turn contain `Orgunit` objects. Each `Orgunit` object will have its relevant information (contracts, quantity data, etc).

---

## Important Notes

- Quality data is not currently processed by this pipeline. 
- This pipeline was created when there was not a lot of data in DHIS2. We might want to enhance it once we get more data. The main modifications we might want to do are:
- More thorough cleaning of the quantity data. (once we have had a more thorough look at the data).
- More indicators being calculated. (once we know more what indicators will be useful for the analysis).
- The pipeline produces a log file, which tells important information about the process. 

---