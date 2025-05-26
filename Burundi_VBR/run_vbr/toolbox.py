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


def get_verification_information(self):
    """
    Create a pandas dataframe with the information about the center and whether it will be verified or not.
    """
    rows = []
    list_cols_df_verification = config.list_cols_df_verification + self.qualite_indicators

    for ou in self.members:
        new_row = (
            [ou.period]
            + [ou.id]
            + ou.identifier_verification
            + [ou.is_verified]
            + [
                ou.diff_subsidies_decval_median_period,
                ou.diff_subsidies_tauxval_median_period,
                ou.benefice_vbr,
                ou.taux_validation,
                ou.subside_dec_period,
                ou.subside_val_period,
                ou.subside_taux_period,
                ou.ecart_median,
                ou.risk,
                ou.quality_high_risk,
                ou.quality_mod_risk,
                ou.quality_low_risk,
                ou.nb_services_moyen_risk,
                ou.nb_services,
            ]
            + [ou.indicator_scores.get(i, pd.NA) for i in self.qualite_indicators]
        )
        rows.append(new_row)

    self.df_verification = pd.DataFrame(rows, columns=list_cols_df_verification)
