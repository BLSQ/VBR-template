from RBV_package import rbv_environment as rbv

import pandas as pd

import config


def get_service_information(self):
    """
    Create a DataFrame with the information per service.
    Note: the method get_gain_verif_for_period_verif has a lot of information per service.
    If we get the information from there, we can probably create a more complete .csv

    Returns
    -------
    df : pd.DataFrame
        DataFrame with the information per service.
    """
    rows = []

    for ou in self.members:
        list_services = list(ou.quantite_window["service"].unique())

        for service in list_services:
            taux_validation = ou.quantite_window[ou.quantite_window.service == service][
                "taux_validation"
            ].median()
            if pd.isnull(taux_validation):
                taux_validation = ou.taux_validation

            if isinstance(ou.ecart_median_per_service, pd.DataFrame):
                ecart = ou.ecart_median_per_service[
                    ou.ecart_median_per_service["service"] == service
                ]["ecart_median"].median()

                if pd.isnull(ecart):
                    ecart = ou.ecart_median

            else:
                ecart = pd.NA

            new_row = (
                ou.period,
                ou.id,
                ou.category_centre,
                rbv.hot_encode(not ou.is_verified),
                ou.risk,
                service,
                taux_validation,
                ecart,
            )

            rows.append(new_row)

    df = pd.DataFrame(rows, columns=config.list_cols_df_services)
    return df
