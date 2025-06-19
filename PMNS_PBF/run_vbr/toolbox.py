import pandas as pd

list_cols_df_verification = [
    "period",
    "ou_id",
    "level_2_uid",
    "level_2_name",
    "level_3_uid",
    "level_3_name",
    "level_4_uid",
    "level_4_name",
    "level_5_uid",
    "level_5_name",
    "level_6_uid",
    "level_6_name",
    "bool_verified",
    "diff_in_subsidies_decval_period",
    "diff_in_subsidies_tauxval_period",
    "benefice_complet_vbr",
    "taux_validation",
    "subside_dec_period",
    "subside_val_period",
    "subside_taux_period",
    "ecart_median_per_services",
    "ecart_median_general",
    "ecart_average_general",
    "categorie_risque",
    "high_risk_quality_indicators",
    "middle_risk_quality_indicators",
    "low_risk_quality_indicators",
]


def get_ecart_median(self):
    """
    Get the median of the ecart, in general and per service.

    The ecart is a number from 0 to 1 that represents the difference between
        the declared, verified and validated values.
    The closer to 0, the better the center is doing.
    """
    self.ecart_median_per_service = (
        self.quantite_window.groupby("service", as_index=False)["weighted_ecart_dec_val"]
        .median()
        .rename(columns={"weighted_ecart_dec_val": "ecart_median"})
    )
    self.ecart_median = self.ecart_median_per_service["ecart_median"].median()
    self.ecart_median_gen = self.quantite_window["weighted_ecart_dec_val"].median()
    self.ecart_avg_gen = self.quantite_window["weighted_ecart_dec_val"].mean()


def get_verification_information(self):
    """
    Create a pandas dataframe with the information about the center and whether it will be verified or not.
    """
    rows = []
    list_cols_df_verification_comp = list_cols_df_verification + self.qualite_indicators

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
                ou.ecart_median_gen,
                ou.ecart_avg_gen,
                ou.risk,
                ou.quality_high_risk,
                ou.quality_mod_risk,
                ou.quality_low_risk,
            ]
            + [ou.indicator_scores.get(i, pd.NA) for i in self.qualite_indicators]
        )
        rows.append(new_row)

    self.df_verification = pd.DataFrame(rows, columns=list_cols_df_verification_comp)
