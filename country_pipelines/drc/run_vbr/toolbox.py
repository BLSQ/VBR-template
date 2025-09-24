import pandas as pd
import config_toolbox as config


def get_verification_information(self):
    """
    Create a pandas dataframe with the information about the center and whether it will be verified or not.
    """
    rows = []
    list_cols_df_verification = config.list_cols_df_verification

    for ou in self.members:
        new_row = (
            [ou.period]
            + [ou.id]
            + ou.identifier_verification
            + [ou.is_verified]
            + [ou.dhis2_is_not_verified]
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
            ]
        )
        rows.append(new_row)

        self.df_verification = pd.DataFrame(rows, columns=list_cols_df_verification)


def get_statistics(self, period):
    """
    Create the statistics for the period and Group of Organizational Units

    Parameters
    ----------
    period: str
        The date we are running the simulation for.

    Returns
    -------
    stats: pd.DataFrame
        The statistics for the period and Group of Organizational Units.
    """
    verified_centers = self.df_verification.bool_verified_simulation
    vbr_beneficial = self.df_verification["benefice_complet_vbr"] < 0

    nb_centers = len(self.members)
    nb_centers_verified = self.df_verification[verified_centers].shape[0]
    nb_centers_not_verified_dhis2 = self.df_verification[
        self.df_verification.bool_not_verified_dhis2
    ].shape[0]

    high_risk = len([ou.id for ou in self.members if ou.risk == "high" or ou.risk == "uneligible"])
    mod_risk = len([ou.id for ou in self.members if "moderate" in ou.risk])
    low_risk = len([ou.id for ou in self.members if ou.risk == "low"])

    cost_verification_vbr = self.cout_verification_centre * nb_centers_verified
    cost_verification_syst = self.cout_verification_centre * nb_centers

    subsides_vbr = (
        self.df_verification[verified_centers]["subside_val_period"].sum()
        + self.df_verification[~verified_centers]["subside_taux_period"].sum()
    )
    subsides_syst = self.df_verification["subside_val_period"].sum()

    cout_total_vbr = subsides_vbr + cost_verification_vbr
    cout_total_syst = subsides_syst + cost_verification_syst

    ratio_verif_costtotal_vbr = cost_verification_vbr / cout_total_vbr
    ratio_verif_costtotal_syst = cost_verification_syst / cout_total_syst

    nb_centre_vbr_made_money = len(
        self.df_verification[(~verified_centers) & vbr_beneficial]["ou_id"].unique()
    )
    nb_centre_vbr_lost_money = len(
        self.df_verification[(~verified_centers) & (~vbr_beneficial)]["ou_id"].unique()
    )

    money_won_by_vbr = self.df_verification[(~verified_centers) & vbr_beneficial][
        "benefice_complet_vbr"
    ].sum()

    money_lost_by_vbr = self.df_verification[(~verified_centers) & (~vbr_beneficial)][
        "benefice_complet_vbr"
    ].sum()

    gain_unverified_centers_for_vbr = self.df_verification[~verified_centers][
        "diff_in_subsidies_tauxval_period"
    ].mean()
    gain_verified_centers_for_vbr = self.df_verification[verified_centers][
        "diff_in_subsidies_tauxval_period"
    ].mean()

    new_row = (
        self.name,
        period,
        nb_centers,
        high_risk,
        mod_risk,
        low_risk,
        nb_centers_verified,
        nb_centers_not_verified_dhis2,
        cost_verification_vbr,
        cost_verification_syst,
        subsides_vbr,
        subsides_syst,
        cout_total_vbr,
        cout_total_syst,
        ratio_verif_costtotal_vbr,
        ratio_verif_costtotal_syst,
        nb_centre_vbr_made_money,
        nb_centre_vbr_lost_money,
        money_won_by_vbr,
        money_lost_by_vbr,
        gain_unverified_centers_for_vbr,
        gain_verified_centers_for_vbr,
    )

    return new_row
