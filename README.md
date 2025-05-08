# OVERVIEW
This repository contains:

## Templates for the two pipelines necessay for VBR. 

- The first pipeline (initialize_vbr) gets the relevant data from DHIS2/Hesabu, and creates cvs/a pickle file containing the quality indicators and the declared/verified/validated quantities per service. 

- The second pipeline (run_vbr) processes the outputs from the first pipeline. It creates aggregates for services/centers/provinces, such as the average rate of validation or how much money would verification win/loose.

## The VBR package.

This package (rbv-package) contains some necessary functions to do the VBR processing with. It is published in pip.

## The DRC pipelines

In PMNS_PBF, we have stored the pipelines necessary to do VBR in DRC.

# IMPROVEMENTS

- When looking at data for DRC, there were some OUs that had no data for the second semester of 2024. We should look at whether this is correct or not.

- When we do the processing in the second pipeline, we add up the money we would save by doing VBR: (amount of money the center gets with VBR - amount of money the center gets with systematic verification - how much it costs to verify the center). Nevertheless, there are some cases in which the center has no declared/verified values. In these cases, we still add up (- how much it costs to verify the center). We should look into whether we should do this or not.

- There are some values (taux_validation, ecart, etc.) that we calculate by doing (first) a median over the different services (second) a median over those medians. These means that we give the same importance to services that have a lot of data and services that have less data. We should look into maybe not doing this.

- The VBR rules for Burundi are fairly strict -- with one service lying a bit, we qualify the center as high risk. We should probably look into relaxing this requirement.

- When we get the data from Hesabu/DHIS2, there are a lot of tarifs that come in blank. The money related to the services paired with those tarifs is not taken into account in the analysis. This is not very good (its like half of the tarifs), and we should look into why we are not pulling this data from Hesabu/DHIS2. 

