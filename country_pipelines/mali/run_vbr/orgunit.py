import pandas as pd

from RBV_package import dates

import config

from openhexa.toolbox.dhis2.periods import Month, Quarter

from statistics import mean
from collections.abc import Generator


class VBR:
    """
    A class for the Verification Based on Risk.

    Attributes
    ----------
    cout_verification_centre: int
        The cost of verifying a center. It is inputed by the user.
    groups: list of GroupOrgUnits
        List of GroupOrgUnits objects that are part of the VBR.
    nb_periods: int
        The minimum number of months with dec-val data (during the observation window)
        to be eligible for non-verification.
    paym_method_nf: str
        The payment method for non-verified centers.
        It can be "complet" or "tauxval".
    period_type: str
        The frequency of verification. It can be "month" or "quarter".
    proportions: dict
        The percentage of centers to verify per risk category.
    quantity_risk_calculation: str
        The method to calculate the quantity risk. It can be "ecart" or "verifgain".
    seuil_max_bas_risk: float
        The maximum threshold for low risk.
        (Only relevant if quantity_risk_calculation == "ecart")
    seuil_max_moyen_risk: float
        The maximum threshold for moderate risk.
        (Only relevant if quantity_risk_calculation == "ecart")
    verification_gain_low: float
        The verification gain threshold for low risk.
        (Only relevant if quantity_risk_calculation == "verifgain")
    verification_gain_mod: float
        The verification gain threshold for moderate risk.
        (Only relevant if quantity_risk_calculation == "verifgain")
    window: int
        The number of months to use for the observation.
    """

    def __init__(self):
        self.groups = []

    def set_frequence(self, freq: str) -> None:
        """
        Define the period_type of the data in the Organizational Unit.

        Parameters
        ----------
        freq : str
            Frequency of the simulation, either "mois" or "trimestre". It is inputed by the user.
        """
        if freq == "quarter":
            self.period_type = "quarter"
        else:
            self.period_type = "month"

    def set_payment_method(self, paym_method_nf: str) -> None:
        """
        Define the payment method for non-verified centers.
        """
        self.paym_method_nf = paym_method_nf

    def set_nb_verif_min_per_window(self, nb_periods: int) -> None:
        """
        Define the minimum number of months with dec-val data (during the observation window) to be eligible for VBR.
        """
        self.nb_periods = nb_periods

    def set_proportions(self, proportions: dict[str, float]) -> None:
        """
        Set the proportions of the different risk categories.

        Parameters
        ----------
        proportions: dict
            The proportions of the different risk categories.
            The keys are the risk categories and the values are the proportions.
        """
        self.proportions = proportions

    def set_cout_verification(self, cout_verification_centre: int) -> None:
        """
        Set how much it costs to verify a center.

        Parameters
        ----------
        cout_verification_centre: float
            The cost of verifying a center. It is inputed by the user.
        """
        self.cout_verification_centre = cout_verification_centre

    def set_window(self, window: int) -> None:
        """
        Select the quantitative and the qualitative data for the observation window.

        Parameters
        ----------
        window : int
            The number of months you want to use for the observation.
        """
        self.window = max([window, 3])

    def set_quantity_risk_calculation(self, quantity_risk_calculation: str) -> None:
        """
        Define the method to calculate the quantity risk.
        """
        self.quantity_risk_calculation = quantity_risk_calculation

    def set_seuil_max_bas_risk(self, seuil_max_bas_risk: float) -> None:
        """
        Define the maximum threshold for low risk.
        """
        self.seuil_max_bas_risk = seuil_max_bas_risk

    def set_seuil_max_moyen_risk(self, seuil_max_moyen_risk: float) -> None:
        """
        Define the maximum threshold for moderate risk.
        """
        self.seuil_max_moyen_risk = seuil_max_moyen_risk

    def set_verification_gain_low(self, verification_gain_low: int) -> None:
        """
        Define the verification gain threshold for low risk.
        """
        self.verification_gain_low = verification_gain_low

    def set_verification_gain_mod(self, verification_gain_mod: int) -> None:
        """
        Define the verification gain threshold for moderate risk.
        """
        self.verification_gain_mod = verification_gain_mod


class Orgunit:
    """
    A class for the organizational unit.

    Attributes
    ----------
    benefice_vbr_period: np.float64
        In the simulation period, amount of money won with VBR
            subsidies based on validated values plus cost of verification
            minus subsidies based on declared values (possibly adjusted by taux)
        If it is bigger than zero, then we should do VBR.
    benefice_vbr_window: np.float64
        Calculating the median over the observation window, amount of money won with VBR
            subsidies based on validated values plus cost of verification
            minus subsidies based on declared values (possibly adjusted by taux)
        If it is bigger than zero, then we should do VBR.
    diff_subsidies_decval_period: np.float64
        The median of:
            the difference in subsidies that the center would recieve based on the declared and  validated values.
        Its calculated for the period of the simulation.
    diff_subsidies_tauxval_period: np.float64
        The median of:
            the difference in subsidies that the center would recieve based on the declared*taux and validated values.
        Its calculated for the period of the simulation.
    ecart_median_per_service_window: pd.DataFrame
        A dataframe with the median ecart per service, calculated over the observation window.
    ecart_median_window: np.float64
        The median of the ecart for the center, calculated over the observation window and services.
        The closer to 0, the better the center is doing.
    ecart_moyen_window: np.float64
        The mean of the ecart for the center, calculated over the observation window and services.
    id: str
        The id of the organizational unit.
    is_verified: bool
        True if the center will be verified, False otherwise.
    locations: list
        List with the ID/names of the level 2, 3, 4, 5 and 6 of the center.
    month: str
        The month we are running the simulation for.
        (if period_type == "month", it is the same as self.period)
    period: str
        The period (month or quarter) we are running the simulation for.
    quantite: pd.DataFrame
        The quantitative data for the center.
    quantite_period: pd.DataFrame
        The quantitative data for the period we are running the simulation for.
    quantite_window: pd.DataFrame
        Quantitative data for the observation window.
    quarter: str
        The quarter we are running the simulation for.
        (if period_type == "quarter", it is the same as self.period)
    risk: str
        The overall risk of the center.
    risk_quantite: str
        The quantity risk of the center.
    subside_dec_period: np.float64
        The total subside the center would get based only on the declared values.
        It only takes into account the period we are running the simulation for.
    subside_taux_period: np.float64
        The total subside the center would get based on the declared values,
        but taking into account the taux of the center.
        It only takes into account the period we are running the simulation for.
    subside_val_period: np.float64
        The total subside the center would get based on the validated values.
        It only takes into account the period we are running the simulation for.
    taux_validation_median_window: np.float64
        The median of the taux_validation for the center,
        calculated over the observation window and service.
        The closer to 1, the better the center is doing.
    taux_validation_moyen_window: np.float64
        The mean of the taux_validation for the center,
        calculated over the observation window and service.
    taux_validation_par_service_window: pd.DataFrame
        A dataframe with the median taux_validation per service, calculated over the observation window.
    """

    def __init__(self, ou_id, uneligible_vbr, quantite):
        self.id = ou_id

        if uneligible_vbr:
            self.risk = "uneligible"
        else:
            self.risk = "unknown"

        self.initialize_quantite(quantite)

        self.period = ""
        self.month = ""
        self.quarter = ""

        self.subside_dec_period = None
        self.subside_val_period = None
        self.subside_taux_period = None
        self.benefice_vbr_period = None
        self.benefice_vbr_window = None
        self.diff_subsidies_decval_period = None
        self.diff_subsidies_tauxval_period = None

        self.ecart_mean_per_service_window = pd.DataFrame()
        self.ecart_median_window = None
        self.taux_validation_par_service_window = pd.DataFrame()
        self.taux_validation_median_window = None

        self.is_verified = "unknown"

        self.locations = list(
            self.quantite[
                [
                    "level_1_uid",
                    "level_1_name",
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
                ]
            ].values[0]
        )

    def initialize_quantite(self, quantite: pd.DataFrame) -> None:
        """
        Initialize the quantity data.
        """
        self.quantite = quantite.sort_values(by=["ou", "service", "quarter", "month"])
        if "level_6_uid" not in self.quantite.columns:
            self.quantite.loc[:, "level_6_uid"] = pd.NA
            self.quantite.loc[:, "level_6_name"] = pd.NA

        if pd.api.types.is_numeric_dtype(self.quantite["month"]):
            self.quantite["month"] = self.quantite["month"].astype("Int64").astype(str)

        self.quantite_period = pd.DataFrame()
        self.quantite_window = pd.DataFrame()
        self.risk_quantite = "unknown"

    def set_verification(self, is_verified: bool) -> None:
        """
        Define whether the center will be verified or not.

        Parameters
        ----------
        is_verified: bool
            True if the center will be verified, False otherwise.
        """
        self.is_verified = is_verified

    def set_window_df(self, vbr_object: VBR) -> None:
        """
        Select the quantitative data for the observation window.

        Parameters
        ----------
        vbr_object : VBR
            The VBR object with all the configuration.
        """

        range = [
            str(elem)
            for elem in get_date_series(
                str(months_before(self.period, vbr_object.window, vbr_object.period_type)),
                str(months_before(self.period, 4, vbr_object.period_type)),
                vbr_object.period_type,
            )
        ]
        self.quantite_window = self.quantite[self.quantite[vbr_object.period_type].isin(range)]

    def set_period_df(self, vbr_object: VBR) -> None:
        """
        Select the quantitative data for the period we are running the simulation for.

        Parameters
        ----------
        vbr_object : VBR
            The VBR object.
        """
        self.quantite_period = self.quantite[self.quantite[vbr_object.period_type] == self.period]

    def set_period_verification(self, vbr_object: VBR, period: str) -> None:
        """
        Define the date we are running the verification for.

        Parameters
        ----------
        vbr_object : VBR
            The VBR object.
        period : str
            The date we are running the simulation for.
        """
        self.period = str(period)
        if vbr_object.period_type == "quarter":
            self.quarter = str(period)
            self.month = str(dates.quarter_to_months(period))
        else:
            self.month = str(period)
            self.quarter = str(dates.month_to_quarter(period))

    def calculate_subsidies_period(self, mode: str) -> None:
        """
        Calculate, for the period of the simulation, how much money we win with verification.

        Parameters
        ----------
        mode: str
            The payment method for non-verified centers.
            It can be "complet" or "tauxval".
        """
        if self.quantite_period.shape[0] > 0:
            (
                self.subside_dec_period,
                self.subside_val_period,
                self.subside_taux_period,
            ) = 0, 0, 0
            list_services = self.quantite_period.service.unique()
            for service in list_services:
                quantite_period_service = self.quantite_period[
                    self.quantite_period.service == service
                ].copy()

                self.calculate_mult_factor(mode, quantite_period_service, service)

                quantite_period_service["subside_sans_verification_taux"] = (
                    quantite_period_service["subside_sans_verification"]
                    * quantite_period_service["multiplication_factor"]
                )

                self.subside_dec_period += quantite_period_service[
                    "subside_sans_verification"
                ].sum()

                self.subside_val_period += quantite_period_service[
                    "subside_avec_verification"
                ].sum()

                self.subside_taux_period += quantite_period_service[
                    "subside_sans_verification_taux"
                ].sum()

        else:
            self.subside_dec_period = pd.NA
            self.subside_taux_period = pd.NA
            self.subside_val_period = pd.NA

    def calculate_gain_verification_window(
        self, cout: int, frequence: str, payment_mode: str
    ) -> None:
        """
        Calculate the gains from verification over the observation window.

        NOTE: THIS CALCULATION IS NOT THE BEST. WE USE THE TAUX OF THE WINDOW TO CALCULATE
        THE SUBSIDIES FOR NON-VERIFIED CENTERS. IN REALITY, WE SHOULD USE THE TAUX OF THE 2 QUARTERS
        BEFORE. (WE DO NOT DO THIS YET BECAUSE THERE IS NOT ENOUGH DATA).


        Parameters
        ----------
        cout : int
            The cost of verification.
        frequence : str
            The frequency of the observation (e.g., "monthly", "quarterly").
        payment_mode : str
            The payment method for non-verified centers.
            It can be "complet" or "tauxval".
        """
        if self.quantite_window.shape[0] == 0:
            self.benefice_vbr_window = pd.NA
            return

        list_periods = self.quantite_window[frequence].unique()
        gains_vbr = []

        for period in list_periods:
            gain_vbr_period = cout
            quantite_period = self.quantite_window[self.quantite_window[frequence] == period].copy()
            list_services = quantite_period.service.unique()
            for service in list_services:
                quantite_period_service = quantite_period[quantite_period.service == service].copy()

                self.calculate_mult_factor(payment_mode, quantite_period_service, service)
                subside_non_ver = (
                    quantite_period_service["subside_sans_verification"]
                    * quantite_period_service["multiplication_factor"]
                ).sum()
                subside_ver = quantite_period_service["subside_avec_verification"].sum()
                gain_vbr_period_service = -subside_non_ver + subside_ver
                gain_vbr_period += gain_vbr_period_service

            gains_vbr.append(gain_vbr_period)

        self.benefice_vbr_window = mean(gains_vbr)

    def calculate_mult_factor(
        self, mode: str, quantite_period_service: pd.DataFrame, service: str
    ) -> None:
        """
        For non-verified centers, calculate the factor we will use to give subsidies.
        """
        if mode == "tauxval":
            taux_validation_service = self.taux_validation_par_service_window.loc[
                self.taux_validation_par_service_window["service"] == service, "taux_validation"
            ]

            if taux_validation_service.empty:
                # This means that the service was not present in the center in the window period,
                # but it is in the period of verification.
                quantite_period_service["multiplication_factor"] = (
                    self.taux_validation_median_window
                )
            else:
                quantite_period_service["multiplication_factor"] = taux_validation_service.median()
        elif mode == "complet":
            quantite_period_service["multiplication_factor"] = 1
        else:
            raise Exception(f"Payment method {mode} not recognized.")

    def calculate_diff_subsidies_period(self, vbr_object: VBR) -> None:
        """
        Define some quantities about the cost/gain of verification for the period of the simulation.

        Parameters
        ----------
        vbr_object : VBR
            The VBR object containing information about the configuration.
        """
        if (
            pd.isnull(self.subside_taux_period)
            or pd.isnull(self.subside_val_period)
            or pd.isnull(self.subside_dec_period)
        ):
            self.diff_subsidies_tauxval_period = pd.NA
            self.diff_subsidies_decval_period = pd.NA
            self.benefice_vbr_period = pd.NA
        else:
            self.diff_subsidies_tauxval_period = self.subside_taux_period - self.subside_val_period
            self.diff_subsidies_decval_period = self.subside_dec_period - self.subside_val_period
            if vbr_object.paym_method_nf == "complet":
                self.benefice_vbr_period = (
                    -self.diff_subsidies_decval_period + vbr_object.cout_verification_centre
                )
            elif vbr_object.paym_method_nf == "tauxval":
                self.benefice_vbr_period = (
                    -self.diff_subsidies_tauxval_period + vbr_object.cout_verification_centre
                )
            else:
                raise Exception(f"Payment method {vbr_object.paym_method_nf} not recognized.")

    def calculate_ecart_window(self):
        """
        Get the average of the ecart, in general and per service.
        (We calculate the average because we have little periods)

        The ecart is a number from 0 to 1 that represents the difference between
            the declared, verified and validated values.
        The closer to 0, the better the center is doing.
        """
        if "weighted_ecart_dec_val" in self.quantite_window.columns:
            col = "weighted_ecart_dec_val"
        elif "ecart_dec_val" in self.quantite_window.columns:
            col = "ecart_dec_val"
        else:
            raise Exception("No ecart column found in the quantitative data.")

        self.ecart_mean_per_service_window = (
            self.quantite_window.groupby("service", as_index=False)[col]
            .mean()
            .reset_index()
            .rename(columns={col: "ecart_mean"})
        )
        # Since we have so little data, it makes more sense to do the median directly.
        self.ecart_median_window = self.quantite_window[col].median()  # Here we keep the median
        self.ecart_moyen_window = self.quantite_window[col].mean()  # Here we calculate the mean

    def calculate_taux_window(self):
        """
        Get the mean of the taux validation, in general and per service.
        (The taux validation is 1 - (dec - val)/dec).
        The closer to 1, the better the center is doing.

        If there is no data, we say that none of the centers are verified.
        """
        self.taux_validation_par_service_window = self.quantite_window.groupby(
            "service", as_index=False
        )["taux_validation"].mean()

        # Since we have so little data, it makes more sense to do the median directly.
        self.taux_validation_median_window = self.quantite_window["taux_validation"].median()
        self.taux_validation_moyen_window = self.quantite_window["taux_validation"].mean()

    def mix_risks(self):
        """
        Mix the different risk levels to get an overall risk level for the center.
        """
        risks = [self.risk, self.risk_quantite]

        if "uneligible" in risks:
            self.risk = "uneligible"
        elif "high" in risks:
            self.risk = "high"
        elif "moderate" in risks:
            self.risk = "moderate"
        else:
            self.risk = "low"


class GroupOrgUnits:
    """
    A class for the group of organizational units.

    Attributes
    ----------
    df_verification: pd.DataFrame
        Dataframe with a summary of the verification information for each center in the group
    members: list of Orgunit
        List of Orgunit objects that are part of the group.
    name: str
        The name of the group of organizational Units.
        (Either a province or "national")
    """

    def __init__(self, name):
        self.name = name

        self.members = []
        self.df_verification = pd.DataFrame()

    def add_ou(self, ou: Orgunit) -> None:
        """
        Add an organizational unit to the list of members.

        Parameters
        ----------
        ou: Orgunit
            The organizational unit to add.
        """
        self.members.append(ou)

    def get_verification_information(self):
        """
        Create the verification information DataFrame.
        It has one row per center, and has all of the relevant information about what
        the simulation calculated for it.
        """
        rows = []
        list_cols_df_verification = config.list_cols_df_verification

        for ou in self.members:
            if ou.is_verified:
                if ou.benefice_vbr_period is pd.NA:
                    category = pd.NA
                elif ou.benefice_vbr_period > 0:
                    category = "Bénéfique & Vérifié"
                else:
                    category = "Non bénéfique & Vérifié"
            else:
                if ou.benefice_vbr_period is pd.NA:
                    category = pd.NA
                elif ou.benefice_vbr_period > 0:
                    category = "Bénéfique & Non vérifié"
                else:
                    category = "Non bénéfique & Non vérifié"

            new_row = (
                [ou.id]
                + [ou.period]
                + ou.locations
                + [ou.risk, ou.is_verified, ou.benefice_vbr_period, category]
                + [
                    ou.subside_dec_period,
                    ou.subside_val_period,
                    ou.subside_taux_period,
                    ou.diff_subsidies_decval_period,
                    ou.diff_subsidies_tauxval_period,
                ]
                + [
                    ou.benefice_vbr_window,
                    ou.taux_validation_median_window,
                    ou.taux_validation_moyen_window,
                    ou.ecart_median_window,
                    ou.ecart_moyen_window,
                ]
            )
            rows.append(new_row)

        self.df_verification = pd.DataFrame(rows, columns=list_cols_df_verification)

    def get_service_information(self):
        """
        Create the service information DataFrame.
        It has one row per center and service, along with the verification, the taux and the ecart.
        It has the information we will want to push back to DHIS2.

        Returns
        -------
        df : pd.DataFrame
            DataFrame with the information per service.
        """
        rows = []

        for ou in self.members:
            list_services = list(ou.quantite_period["service"].unique())

            for service in list_services:
                if isinstance(ou.taux_validation_par_service_window, pd.DataFrame):
                    taux_validation = ou.taux_validation_par_service_window[
                        ou.taux_validation_par_service_window["service"] == service
                    ]["taux_validation"].mean()

                    if pd.isnull(taux_validation):
                        taux_validation = ou.taux_validation_median_window

                else:
                    taux_validation = pd.NA

                if isinstance(ou.ecart_mean_per_service_window, pd.DataFrame):
                    ecart = ou.ecart_mean_per_service_window[
                        ou.ecart_mean_per_service_window["service"] == service
                    ]["ecart_median"].mean()

                    if pd.isnull(ecart):
                        ecart = ou.ecart_median_window

                else:
                    ecart = pd.NA

                new_row = (
                    ou.period,
                    ou.id,
                    not ou.is_verified,
                    ou.risk,
                    service,
                    taux_validation,
                    ecart,
                )

                rows.append(new_row)

        df = pd.DataFrame(rows, columns=config.list_cols_df_services)
        return df

    def get_statistics(self, vbr_object: VBR, period: str) -> tuple:
        """
        Create the statistics for the group of organizational units.
        It has one row per period and group of organizational units.

        Parameters
        ----------
        vbr_object: VBR
            The VBR object with all the configuration.
        period: str
            The date we are running the simulation for.

        Returns
        -------
        stats: tuple
            The row with the statistics for the group of organizational units.
        """
        verified_centers = self.df_verification["Is the center verified?"]
        vbr_beneficial = (
            self.df_verification["Benefice complet VBR (if positive, VBR is beneficial) - period"]
            > 0
        )

        nb_centers = len(self.members)
        nb_centers_verified = self.df_verification[verified_centers].shape[0]

        uneligible = len([ou.id for ou in self.members if ou.risk == "uneligible"])
        high_risk = len([ou.id for ou in self.members if ou.risk == "high"])
        mod_risk = len([ou.id for ou in self.members if "moderate" in ou.risk])
        low_risk = len([ou.id for ou in self.members if ou.risk == "low"])

        cost_verification_vbr = vbr_object.cout_verification_centre * nb_centers_verified
        cost_verification_syst = vbr_object.cout_verification_centre * nb_centers

        if vbr_object.paym_method_nf == "complet":
            subsides_vbr = (
                self.df_verification[verified_centers][
                    "Subsidies based in validated values in the period"
                ].sum()
                + self.df_verification[~verified_centers][
                    "Subsidies based in declared values in the period"
                ].sum()
            )
        elif vbr_object.paym_method_nf == "tauxval":
            subsides_vbr = (
                self.df_verification[verified_centers][
                    "Subsidies based in validated values in the period"
                ].sum()
                + self.df_verification[~verified_centers][
                    "Subsidies based in declared values times the taux in the period"
                ].sum()
            )
        else:
            raise Exception(f"Payment method {vbr_object.paym_method_nf} not recognized.")
        subsides_syst = self.df_verification[
            "Subsidies based in validated values in the period"
        ].sum()

        cout_total_vbr = subsides_vbr + cost_verification_vbr
        cout_total_syst = subsides_syst + cost_verification_syst

        ratio_verif_costtotal_vbr = cost_verification_vbr / cout_total_vbr
        ratio_verif_costtotal_syst = cost_verification_syst / cout_total_syst

        nb_unverified_centre_vbr_made_money = len(
            self.df_verification[(~verified_centers) & vbr_beneficial]["ID"].unique()
        )
        nb_unverified_centre_vbr_lost_money = len(
            self.df_verification[(~verified_centers) & (~vbr_beneficial)]["ID"].unique()
        )

        gain_vbr = cout_total_syst - cout_total_vbr

        new_row = (
            self.name,
            str(period),
            nb_centers,
            uneligible,
            high_risk,
            mod_risk,
            low_risk,
            nb_centers_verified,
            cost_verification_vbr,
            cost_verification_syst,
            subsides_vbr,
            subsides_syst,
            cout_total_vbr,
            cout_total_syst,
            ratio_verif_costtotal_vbr,
            ratio_verif_costtotal_syst,
            nb_unverified_centre_vbr_made_money,
            nb_unverified_centre_vbr_lost_money,
            gain_vbr,
        )

        return new_row


def months_before(date: str, lag: int, freq: str) -> str:
    """
    Get the month that is "lag" months before a given date.

    Parameters
    ----------
    date: str
        The given month (e.g. 201804/2018Q2).
    lag: int
        The number of months before (e.g. 6).
    freq: str
        The frequency type ("month" or "quarter").

    Returns
    -------
    int
        The month/quarter corresponding to the period that is "lag" months before "date"
        (e.g. 201710/2017Q4).
    """
    if freq == "quarter" or freq == "trimestre":
        year, quarter = date.split("Q")
        year = int(year)
        month = int(quarter) * 3
        total_months = year * 12 + month - lag
        new_year = total_months // 12
        new_month = total_months % 12
        if new_month == 0:
            new_year -= 1
            new_month = 12
        new_quarter = (new_month - 1) // 3 + 1
        return f"{new_year}Q{new_quarter}"

    elif freq == "month":
        date_int = int(date)
        year = date_int // 100
        m = date_int % 100
        total_months = year * 12 + m - lag
        new_year = total_months // 12
        new_m = total_months % 12
        if new_m == 0:
            new_year -= 1
            new_m = 12
        return str(new_year * 100 + new_m)

    else:
        raise ValueError("Frequency must be either 'month' or 'quarter'.")


def get_date_series(start: str, end: str, type: str) -> Generator:
    """
    Get a list of consecutive months or quarters between two dates.

    Parameters:
    --------------
    start: int
        The starting date (e.g. 201811)
    end: int
        The ending date (e.g. 201811)
    type: str
        The type of period to generate ("month" or "quarter").

    Returns
    ------
    range: list
        A list of consecutive months or quarters between the start and end dates.
    """
    if type == "trimestre" or type == "quarter":
        q1 = Quarter.from_string(start)
        q2 = Quarter.from_string(end)
        range = q1.range(q2)
    else:
        m1 = Month.from_string(start)
        m2 = Month.from_string(end)
        range = m1.range(m2)
    return range
