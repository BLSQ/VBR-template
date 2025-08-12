# OVERVIEW
This repository contains:

## Templates for the three pipelines necessay for VBR. 

- The first pipeline (initialize_vbr) gets the relevant data from DHIS2/Hesabu, and creates cvs/a pickle file containing the quality indicators and the declared/verified/validated quantities per service. 

- The second pipeline (run_vbr) processes the outputs from the first pipeline. It creates aggregates for services/centers/provinces, such as the average rate of validation or how much money would verification win/loose.

- The third pipeline (push_vbr) pushes the results of the simulation created in the second pipeline back into DHIS2

## The VBR package.

This package (rbv-package) contains some necessary functions to do the VBR processing with. It is published in pip.

## Country pipelines

The particular pipelines that were developped for countries. 


