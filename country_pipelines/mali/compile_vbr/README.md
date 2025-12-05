# Compile VBR results (Mali)

## Summary
This pipeline compiles the results from multiple VBR simulation environments into a single consolidated dataset. It compiles the quantity data (extracted by the first pipeline `initialize_vbr`); as well as the statistics and verification data (calculated by the second pipeline `simulate_vbr`).
If indicated by the user, apart from creating a csv with the compiled results, it also saves the results to a database.

---
## Parameters

- **extraction_folder**: The name of the folder where the results from the simulations are stored.
- **quantity_db**: Name of the database table where to save the compiled quantity data. If None, the data will not be saved to a database.
- **reduced_verification_db**: Name of the database table where to save the compiled reduced verification data. If None, the data will not be saved to a database.
- **detailed_verification_db**: Name of the database table where to save the compiled detailed verification data. If None, the data will not be saved to a database.
- **simulation_statistics_db**: Name of the database table where to save the compiled statistics data. If None, the data will not be saved to a database.

---
## Inputs

- **quantity_data.csv files**: The pipeline will compile all of the quantity data files produced by the first pipeline `initialize_vbr` into a single consolidated file. These files are stored in `pipelines/initialize_vbr/data/quantity_data/`
- **simulation_statistcs files**: The pipeline will compile all of the simulation statistics files produced by the second pipeline `simulate_vbr` into a single consolidated file. These files are stored in `pipelines/run_vbr/extraction_folder/data/simulation_statistics/`. Note that `extraction_folder` is the name of the folder specified by the user in the parameters (it must be the same as the one used in the `run_vbr` pipeline).
- **verification information files**: The pipeline will compile all of the verification information files produced by the second pipeline `simulate_vbr` into a single consolidated file. These files are stored in `pipelines/run_vbr/extraction_folder/data/verification_information/`. Note that `extraction_folder` is the name of the folder specified by the user in the parameters (it must be the same as the one used in the `run_vbr` pipeline).

---
## Outputs

The pipeline will create 4 compilated output files, stored in `pipelines/run_vbr/extraction_folder/compiled_data/`. Note that `extraction_folder` is the name of the folder specified by the user in the parameters (it must be the same as the one used in the `run_vbr` pipeline).

- **quantity_data_compiled.csv**: A csv file containing the compiled quantity data from all of the data extractions. A column with the name of the model of the extraction is added to each of the compiled files.
- **simulation_statistics_db.csv**: A csv file containing the compiled simulation statistics data from all of the simulations. Columns with the values of the parameters of the simulations are added to each of the compiled files.
-- **verification_detailed.csv**: A csv file containing the compiled verification data from all of the simulations. Columns with the values of the parameters of the simulations are added to each of the compiled files.
-- **verification_reduced.csv**: A csv file containing the compiled verification data from all of the simulations. The data is summarized: we only keep the information about which centers where verified. Columns with the values of the parameters of the simulations are added to each of the compiled files.

If the user indicates database table names in the parameters, the pipeline will also save each of the compiled datasets to the specified database tables.

---
## Files

- **pipeline.py**: Main script orchestrating the reading, compiling, and saving of data.
- **config.py**: Contains some of the the hard-coded constants used in the pipeline.

---
## Process

(1) **paths**: We define the paths were we will store the data
(2) **simulation_stats**: 
    - We chose the files that contain the simulation statistics data using regex
    - We get the parameters of each of the simulations from the file names
    - We read the files and add the parameters as columns to each of the dataframes
    - We concatenate all of the dataframes into a single dataframe
    - We save the compiled dataframe to a csv file
    - If indicated by the user, we save the compiled dataframe to a database table
(3) **verification**: 
    - We chose the files that contain the verification data using regex
    - We get the parameters of each of the simulations from the file names
    - We read the files and add the parameters as columns to each of the dataframes
    - We create the reduced verification dataframe by summarizing the detailed verification data (`summarize_verification`)
    - We concatenate all of the dataframes into a single dataframe
    - We save the compiled dataframe to a csv file
    - If indicated by the user, we save the compiled dataframe to a database table
(4) **quantity**: 
    - We chose the files that contain the quantity data using regex
    - We read the files and add the model as a column to each of the dataframes
    - We concatenate all of the dataframes into a single dataframe
    - We save the compiled dataframe to a csv file
    - If indicated by the user, we save the compiled dataframe to a database table

---
## Important notes
- The dashboard reads the data from the databases. If you want to actualize the dashboard, you need to provide the relevant database names.
- The pipeline produces a log file, which tells important information about the process. 