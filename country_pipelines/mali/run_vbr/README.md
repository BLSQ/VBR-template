# Run VBR Pipeline (Mali)

## Summary

This pipeline simulates the VBR process for centers in Mali. Using the data extracted from Hesabu and DHIS2 by the first pipeline (`initialize_vbr`), it determines which centers should be verified for each period. In order to do that, it first assigns each center a risk category, checking if the center is eligible for verification. It also generates 3 summary files, which can be used to interpret the results of the simulation.

The pipeline has 2 possible uses. 
(1) We can use it to examine how different characterizations and parameters lead to different results. This will allow us to choose the best possible configuration for Mali VBR.
(2) We can use it during the actual implementation of VBR in Mali, to determine which centers should be verified each period.

---

## Parameters

- **model**: Name of the initialization file (from the first pipeline).
- **folder**: Output folder name for results.
- **mois_start**: Start month of the simulation.
- **year_start**: Start year of the simulation.
- **mois_fin**: End month of the simulation.
- **year_fin**: End year of the simulation.
- **frequence**: Frequency of verification (`quarter` or `month`).
- **window**: Number of months considered in the simulation window.
- **nb_period_verif**: Minimum number of verification visits in the past (in months) for eligibility.
- **prix_verif**: Cost of verification per center (euros).
- **paym_method_nf**: Payment method for non-verified centers (`complet` or `tauxval`).
- **proportion_selection_bas_risque**: Percentage of low risk centers selected for verification.
- **proportion_selection_moyen_risque**: Percentage of moderate risk centers selected for verification.
- **proportion_selection_haut_risque**: Percentage of high risk centers selected for verification.
- **quantity_risk_calculation**: Risk calculation method (`ecart_median` or `ecart_moyen` or `verifgain`).
- **seuil_max_bas_risk**: Threshold for low risk centers. We use it only if `quantity_risk_calculation` is `ecart`.
- **seuil_max_moyen_risk**: Threshold for moderate risk centers. We use it only if `quantity_risk_calculation` is `ecart`.
- **verification_gain_low**: Minimum verification gain for low risk centers. We use it only if `quantity_risk_calculation` is `verifgain`.
- **verification_gain_mod**: Minimum verification gain for moderate risk centers. 

---

## Inputs

- **Simulation environment (.pickle)**  
  Produced by the initialization pipeline, contains all quantitative data and configuration for each organizational unit. When we run the first pipeline several times, we will create different initialization files. It is very important we choose the correct one, using the `model` parameter.

---

## Outputs

The pipeline will produce 3 output CSV files. All of them are stored under `pipelines/run_vbr/{folder}/data/`. Each of them will allow you to analyze the results of the simulation from a different perspective.

- **verification_information CSVs**  
  One file per each of the periods. One row per health center. This file contains detailed information about what happened in each center: how much money was spent, its risk category, if it was eligible, if it was verified, etc. We will use this file when we want to have a detailed view of what is happening in each center. Right now, the information it stores is pretty basic. Once we have more data, and we want to examine different results, we might want to change information it stores. We then will change the function `get_verification_information()` to add the new calculations and the variable `list_cols_df_verification` in `config.py` to add the names of the columns to store them in. 
- **simulation_statistics CSVs**  
  One file per each of the periods. One row per group (province or national). This file contains aggregated statistics about the verification process in each group, such as total verification cost, number of verified centers, etc. We will use this file when we want to have a summarized view of what is happening in each group. Right now, the information it stores is pretty basic. Once we have more data, and we want to examine different results, we might want to change information it stores. We then will change the function `get_statistics()` to add the new calculations and the variable `list_cols_df_stats` in `config.py` to add the names of the columns to store them in.
- **service_information CSVs**  
  One file per each of the periods. One row per health service and service. This file contains information about which centers are verified and about the taux validation / écart for each service. It contains the information that we will want to push back to DHIS2 once the VBR is in place. 

---

## Files

- **pipeline.py**: Main script orchestrating the simulation, risk categorization, eligibility checks, and output generation.
- **orgunit.py**: Contains the `Orgunit`, `GroupOrgUnits`, and `VBR` classes used to structure simulation data and perform calculations. It is the same as in the initialization pipeline.
- **vbr_custom.py**: It contains the custom functions for the risk categorization. The functions stored here are the ones that we are most likely to change. 
- **config.py**: Defines the columns for output CSV files.

---

## Process

1. **Load Simulation Environment**
   - Loads the pre-processed simulation environment (`.pickle`) containing all organizational units and their quantitative data, in `get_environment()`.

2. **Set VBR Configuration**
   - We are going to use a VBR object, that will contain all of the simulation parameters (which remain constant along periods and organizational units) as attributes. We set these attributes in `set_vbr_values`, calling different methods of the VBR object. 
   - We create a dictionary with the proportions in `get_proportions()`.

3. **Determine Simulation Periods**
   - We want to create a list of periods that we want to run the verification for. 
   - We define the start and end periods using the input parameters and the frequency of verification in `get_month_or_quarter()`.
   - We generate the list of periods using the `get_date_series()` method of the `Orgunit` class.

4. **Create the folders**
   - We create a lot of files containing the summaries of the verification process. We define the overall names in `create_folders()`.
   - We create the group names in `create_file_names()`.


5. **Run the actual simulation**
   - For each period (`simulate_period()`):
     - We create the file names. (`create_file_names()`).
     - We run the simulation for all of the groups. (`simulate_period_group()`):
     - We save the `simulation_statistics` as a csv. (remember that this file is at period level)

   - For each group (`simulate_period_group()`):
     - We run the simulation for all of the organizational units in the group. (`simulate_period_orgunits()`).
     - We save the `service_information` as a csv. (remember that this file is at period and group level)
     - We save the `verification_information` as a csv. (remember that this file is at period level)

   - For each organizational unit in the group (`simulate_period_ou()`): 
     - We set the organizational unit values for the period. (`set_ou_values()`).
        - Note that the way we are defining the values for each period is not set in stone. If we want the ecart, or the taux validation to be calculated in a different way, we can change it here.
        - I am especially concerned about the way we calculate the benefit from verifying/not verifying the centers, inside the method `get_benefice_vbr_window`. Right now, we use the same 6 months to calculate the gain and to calculate the taux -- ideally, we would calculate the taux with older months. We might want to change this once we have more data. 
    - We check if the organizational unit is eligible for verification. (`eligible_for_vbr()` in `vbr_custom.py`).
        - Note that the way we are defining the eligibility is not set in stone. If we want to add new conditions, we can do it here.
    - We categorize the organizational unit based on its risk. (`categorize_quantity()` in `vbr_custom.py`).
        - Note that the way we are categorizing the risk is not set in stone. In fact, once we have more data, we will probably want to try different methods. We can do it here.
    - We decide if the organizational unit will be verified or not. (`set_verification()`), based on their risk category.

---
## Ways of characterizing risk
Right now, the pipeline has 3 methods for characterizing the risk of the centers. They are implemented in the function `categorize_quantity()` in `vbr_custom.py`. We might want to change them, or add new ones, once we have more data.
- **Ecart médian (`ecart_median`)**: We calculate the median of the absolute differences between declared and validated quantities in the past verification periods. We then compare this median to two thresholds (`seuil_max_bas_risk` and `seuil_max_moyen_risk`) to categorize the risk as low, medium, or high.
- **Ecart moyen (`ecart_moyen`)**: Similar to the previous method, but we use the mean of the absolute differences instead of the median.
- **Gain de vérification (`verifgain`)**: We calculate the average financial gain from verification in past periods. We then compare this gain to two thresholds (`verification_gain_low` and `verification_gain_mod`) to categorize the risk as low, medium, or high.    

---
## How to use the pipeline for choosing the risk categorization method
One of the goals of the pipeline is to help us choose the best risk categorization method for Mali VBR. 
To do that, what we need to do is:
- Run the pipeline several times, changing the risk categorization method and its parameters each time.
- Compile the results of the runs using the third pipeline, `compile_results`. This pipeline will create a database with all of the results concatenated.
- Then, we can go to the dashboard that uses this database and analyze the results.

When looking at the results, we will want to focus on:
- The amount of money we are saving for VBR.
- If we are still verifying centers that a very big difference between declared and validated values.

When we are looking at this, we might realize that we want to change the way we:
- Characterize the risk of the centers. We might want to try different methods. Then, we can modify the function `categorize_quantity` in `vbr_custom.py`, adding a new possibility to the else-if structure.
    - We might also want to add new parameters to the pipeline that will allow us to tune-in the new risk categorization method.
    - We might also want to add new attributes to the orgunit object, allowing us to characterize it. We then can add a new method in the `Orgunit` class in `orgunit.py` that will calculate this attribute, and call it from the `set_ou_values` method in the `pipeline.py` file.
- We might want to change the characteristics of the centers that are always verified. We can do this by adding a new condition in the `eligible_for_vbr` method in `vbr_custom.py`.

Note that modifying these functions will be easier and better once we have more data. 

---

## Important Notes

- This pipeline was produced using a very small pool of data. Once we have more, we will want to enhance it to make sure that it is as catered to the Mali use case as possible.
- The pipeline produces a log file, which tells important information about the process.

---