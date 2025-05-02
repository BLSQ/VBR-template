import pandas as pd

from RBV_package import dates

from RBV_package import config_package as config


class GroupOrgUnits:
    """
    A class for the group of organizational units.

    Attributes
    ----------
    cout_verification_centre: int
        The cost of verifying a center.
        It is inputed by the user.
    df_verification: pd.DataFrame
        Contains the summary results of the VBR per group.
    members: list of Orgunit
        List of Orgunit objects that are part of the group.
    name: str
        The name of the group of organizational Units.
    proportions: dict
        The probability of being verified per risk category.
    qualite_indicators: list
        The list of the indicators that are used to calculate the quality of the centers.
        It is not currently used.
    """

    def __init__(self, name, qualite_indicators):
        self.name = name
        self.qualite_indicators = qualite_indicators

        self.members = []
        self.cout_verification_centre = None
        self.proportions = {}
        self.df_verification = pd.DataFrame()

    def set_cout_verification(self, cout_verification_centre):
        """
        Set how much it costs to verify a center.

        Parameters
        ----------
        cout_verification_centre: float
            The cost of verifying a center. It is inputed by the user.
        """
        self.cout_verification_centre = cout_verification_centre

    def add_ou(self, ou):
        """
        Add an organizational unit to the list of members.

        Parameters
        ----------
        ou: Orgunit
            The organizational unit to add.
        """
        self.members.append(ou)

    def set_proportions(self, proportions):
        """
        Set the proportions of the different risk categories.

        Parameters
        ----------
        proportions: dict
            The proportions of the different risk categories.
            The keys are the risk categories and the values are the proportions.
        """
        self.proportions = proportions

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
                ]
                + [ou.indicator_scores.get(i, pd.NA) for i in self.qualite_indicators]
            )
            rows.append(new_row)

        self.df_verification = pd.DataFrame(rows, columns=list_cols_df_verification)

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

                new_row = (
                    ou.period,
                    ou.id,
                    ou.category_centre,
                    hot_encode(not ou.is_verified),
                    service,
                    taux_validation,
                )

                rows.append(new_row)

        df = pd.DataFrame(rows, columns=config.list_cols_df_services)
        return df

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
        verified_centers = self.df_verification.bool_verified
        vbr_beneficial = self.df_verification["benefice_complet_vbr"] < 0

        nb_centers = len(self.members)
        nb_centers_verified = self.df_verification[verified_centers].shape[0]

        high_risk = len(
            [ou.id for ou in self.members if ou.risk == "high" or ou.risk == "uneligible"]
        )
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

        num_qual_indicator_high_risk_unverified = (
            self.df_verification[~verified_centers]["high_risk_quality_indicators"]
            .map(lambda x: len(x.split("--")) if isinstance(x, str) and x else 0)
            .mean()
        )
        num_qual_indicator_high_risk_verified = (
            self.df_verification[verified_centers]["high_risk_quality_indicators"]
            .map(lambda x: len(x.split("--")) if isinstance(x, str) and x else 0)
            .mean()
        )
        num_qual_indicator_mod_risk_unverified = (
            self.df_verification[~verified_centers]["middle_risk_quality_indicators"]
            .map(lambda x: len(x.split("--")) if isinstance(x, str) and x else 0)
            .mean()
        )
        num_qual_indicator_mod_risk_verified = (
            self.df_verification[verified_centers]["middle_risk_quality_indicators"]
            .map(lambda x: len(x.split("--")) if isinstance(x, str) and x else 0)
            .mean()
        )
        num_qual_indicator_low_risk_unverified = (
            self.df_verification[~verified_centers]["low_risk_quality_indicators"]
            .map(lambda x: len(x.split("--")) if isinstance(x, str) and x else 0)
            .mean()
        )
        num_qual_indicator_low_risk_verified = (
            self.df_verification[verified_centers]["low_risk_quality_indicators"]
            .map(lambda x: len(x.split("--")) if isinstance(x, str) and x else 0)
            .mean()
        )

        new_row = (
            self.name,
            period,
            nb_centers,
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
            nb_centre_vbr_made_money,
            nb_centre_vbr_lost_money,
            money_won_by_vbr,
            money_lost_by_vbr,
            gain_unverified_centers_for_vbr,
            gain_verified_centers_for_vbr,
            num_qual_indicator_high_risk_unverified,
            num_qual_indicator_high_risk_verified,
            num_qual_indicator_mod_risk_unverified,
            num_qual_indicator_mod_risk_verified,
            num_qual_indicator_low_risk_unverified,
            num_qual_indicator_low_risk_verified,
        )

        return new_row


class Orgunit:
    """
    A class for the organizational unit.

    Attributes
    ----------
    benefice_vbr: np.float64
        Amount of money won per center with VBR. It is calculated as:
        (amount of money the center gets with VBR_taux - amount of money the center gets with systematic verification)
        minus (how much it costs to verify the center)
        If it is bigger than zero, then we should not do VBR.
    category_centre: str
        The category of the center.
    diff_subsidies_decval_median: np.float64
        The median of:
            the difference in subsidies that the center would recieve based on the declared or validated values.
        It is calculated as: median((declared - validated) * tarif)
        The bigger, the more extra subsidies the center would get if it wasn't verified.
        Its calculated for all of the observation window.
    diff_subsidies_decval_median_period: np.float64
        The median of:
            the difference in subsidies that the center would recieve based on the declared or validated values.
        It is calculated as: median((declared - validated) * tarif)
        The bigger, the more extra subsidies the center would get if it wasn't verified.
        Its calculated for the period of the simulation.
    diff_subsidies_tauxval_median_period: np.float64
        The median of:
            the difference in subsidies that the center would recieve without verification (calculated with taux)
            and with verification.
        The bigger, the more extra subsidies the center would get if it wasn't verified.
        Its calculated for the period of the simulation.
    ecart_median: np.float64
        The median of the ecart for the center.
        The ecart measures the difference between the declared, verified and validated values.
        0.4*(ecart_dec_ver) + 0.6*(ecart_ver_val) with
        ecart_dec_ver = (dec - ver) / ver & ecart_ver_val = (ver - val) / ver
        The closer to 1, the more the center is lying.
    ecart_median_per_service: pd.DataFrame
        The median of the ecart for each service for the center.
        The ecart measures the difference between the declared, verified and validated values.
        0.4*(ecart_dec_ver) + 0.6*(ecart_ver_val) with
        ecart_dec_ver = (dec - ver) / ver & ecart_ver_val = (ver - val) / ver
        The closer to 1, the more the center is lying.
    id: str
        The id of the organizational unit.
    identifier_verification: list
        List with the ID/names of the level 2, 3, 4, 5 and 6 of the center.
    is_verified: bool
        True if the center will be verified, False otherwise.
    month: str
        The month we are running the simulation for.
        (if period_type == "month", it is the same as self.period)
    nb_periods :int
        Minimum number of months with dec-val data (during the observation window) to be eligible for VBR.
        It is inputed by the user.
    nb_periods_verified: array
        Periods in which the Organizational Unit has been verified.
    nb_services_risky: int
        Number of services that are not at low risk.
    period: str
        The date we are running the simulation for.
    period_type: str
        Frequency of the simulation, either "month" or "quarter".
        It is inputed by the user.
    qualite: pd.DataFrame
        The qualitative data for the center.
        Right now we don't do anything with it.
    qualite_indicators: list
        The list of the indicators that are used to calculate the quality of the centers.
        Right now we don't do anything with them.
    qualite_window: pd.DataFrame
        Qualitative data for the observation window.
        Right now we don't do anything with it.
    quantite: pd.DataFrame
        The quantitative data for the center.
    quantite_window: pd.DataFrame
        Quantitative data for the observation window.
    quarter: str
        The quarter we are running the simulation for.
        (if period_type == "quarter", it is the same as self.period)
    risk: str
        The overall risk of the center.
    risk_gain_median: str
        The risk of the center based on how much we win by verifying it.
    risk_quantite: str
        The quantity risk of the center.
    subside_dec_period: np.float64
        The total subside the center would get based only on the declared values.
        It is calcualted for the period of the simulation.
    subside_taux_period: np.float64
        The total subside the center would get based on the declared values, but taking into account the taux of the center.
        (The subside the center would get without verification)
        It only takes into account the period we are running the simulation for.
    subside_val_period: np.float64
        The total subside the center would get based on the validated values.
        (The subside the center would get with verification)
        It only takes into account the period we are running the simulation for.
    taux_validation: np.float64
        The median of the taux_validation for the center.
        (The taux validation is 1 - (dec - val)/dec. The closer to one, the more the center tells the truth)
    taux_validation_par_service: pd.DataFrame
        The median of the taux_validation for each service for the center.
        (The taux validation is 1 - (dec - val)/dec. The closer to one, the more the center tells the truth)
    """

    def __init__(self, ou_id, quantite, qualite, qualite_indicators, uneligible_vbr):
        self.id = ou_id

        if uneligible_vbr:
            self.risk = "uneligible"
            self.category_centre = "pca"
        else:
            self.risk = "unknown"
            self.category_centre = "pma"

        self.quantite = quantite
        self.qualite = qualite

        self.initialize_quantite()
        self.initialize_qualite()

        self.qualite_indicators = qualite_indicators

        self.identifier_verification = list(
            self.quantite[
                [
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

        self.period_type = ""
        self.period = ""
        self.month = ""
        self.quarter = ""
        self.nb_periods = None

        self.quantite_window = pd.DataFrame()
        self.qualite_window = pd.DataFrame()

        self.nb_periods_verified = None
        self.nb_services_risky = None

        self.subside_dec_period = None
        self.subside_val_period = None
        self.subside_taux_period = None
        self.diff_subsidies_tauxval_median_period = None
        self.benefice_vbr = None
        self.diff_subsidies_decval_median = None
        self.diff_subsidies_decval_median_period = None

        # Quality indicators
        self.indicator_scores = {}
        self.general_quality = 0
        self.hygiene = 0
        self.finance = 0
        self.quality_high_risk = ""
        self.quality_mod_risk = ""
        self.quality_low_risk = ""
        self.risk_quality = ""

    def initialize_quantite(self):
        """
        Initialize the quantity data.
        """
        self.quantite = self.quantite.sort_values(by=["ou", "service", "quarter", "month"])
        if "level_6_uid" not in self.quantite.columns:
            self.quantite.loc[:, "level_6_uid"] = pd.NA
            self.quantite.loc[:, "level_6_name"] = pd.NA
            self.qualite.loc[:, "level_6_uid"] = pd.NA
            self.qualite.loc[:, "level_6_name"] = pd.NA
        self.quantite.loc["month"] = self.quantite["month"].astype(str)

    def initialize_qualite(self):
        """
        Initialize the quality data.
        """
        self.qualite = self.qualite.sort_values(by=["ou", "indicator", "quarter"])
        self.qualite = self.qualite.drop_duplicates(["ou", "indicator", "quarter"])
        self.qualite["month"] = self.qualite["month"].astype(str)

    def set_verification(self, is_verified):
        """
        Define whether the center will be verified or not.

        Paramenters
        ----------
        is_verified: bool
            True if the center will be verified, False otherwise.
        """
        self.is_verified = is_verified

    def set_frequence(self, freq):
        """
        Define the period_type of the data in the Organizational Unit.

        Parameters
        ----------
        freq : str
            Frequency of the simulation, either "mois" or "trimestre". It is inputed by the user.
        """
        if freq == "trimestre":
            self.period_type = "quarter"
        else:
            self.period_type = "month"

    def set_window(self, window):
        """
        Select the quantitative and the qualitative data for the observation window.

        Parameters
        ----------
        window : int
            The number of months you want to use for the observation.
        """
        window = max([window, 3])
        if self.period_type == "quarter":
            range = [
                str(elem)
                for elem in dates.get_date_series(
                    str(dates.months_before(self.month, window + 2)),
                    str(dates.months_before(self.month, 3)),
                    "month",
                )
            ]
        else:
            range = [
                str(elem)
                for elem in dates.get_date_series(
                    str(dates.months_before(self.month, window)),
                    str(dates.months_before(self.month, 1)),
                    "month",
                )
            ]
        self.quantite_window = self.quantite[self.quantite["month"].isin(range)]
        self.qualite_window = self.qualite[self.qualite["month"].isin(range)]

    def set_nb_verif_min_per_window(self, nb_periods):
        """
        Define the minimum number of months with dec-val data (during the observation window) to be eligible for VBR.
        """
        self.nb_periods = nb_periods

    def set_month_verification(self, period):
        """
        Define the date we are running the verification for.
        """
        self.period = str(period)
        if self.period_type == "quarter":
            self.quarter = str(period)
            self.month = str(dates.quarter_to_months(period))
        else:
            self.month = str(period)
            self.quarter = str(dates.month_to_quarter(period))

    def get_gain_verif_for_period_verif(self, taux_validation):
        """
        Calculate the gains from verification.
        For non-verified centers, we use the taux_validation to calculate the subsidies.

        Parameters
        ----------
        taux_validation: float
            The taux validation for the center.
        """
        quantite_period_total = self.quantite[self.quantite[self.period_type] == self.period].copy()

        if quantite_period_total.shape[0] > 0:
            (
                self.subside_dec_period,
                self.subside_val_period,
                self.subside_taux_period,
            ) = 0, 0, 0
            list_services = quantite_period_total.service.unique()

            for service in list_services:
                quantite_period_service = quantite_period_total[
                    quantite_period_total.service == service
                ].copy()

                self.calculate_mult_factor(taux_validation, quantite_period_service, service)

                quantite_period_service["subside_sans_verification_method_dpdt"] = (
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
                    "subside_sans_verification_method_dpdt"
                ].sum()

        else:
            self.subside_dec_period = pd.NA
            self.subside_taux_period = pd.NA
            self.subside_val_period = pd.NA

    def calculate_mult_factor(self, taux_validation, quantite_period_service, service):
        """
        For non-verified centers, calcualte the factor we will use to give subsidies.
        """
        if taux_validation < 1:
            taux_validation_filtered = self.taux_validation_par_service[
                self.taux_validation_par_service.service == service
            ]["taux_validation"]

            if taux_validation_filtered.empty:
                quantite_period_service["multiplication_factor"] = self.taux_validation_par_service[
                    "taux_validation"
                ].mean()
            else:
                quantite_period_service["multiplication_factor"] = taux_validation_filtered.iloc[0]
        else:
            quantite_period_service["multiplication_factor"] = 1

    def mix_risks(self, use_quality_for_risk):
        """
        We have 3 risks.
        risk_gain_median: str
            The risk of the center based on how much we win by verifying it.
        risk_quality: str
            The quality risk of the center.
        risk_quantite: str
            The quantity risk of the center.

        We combine them to get the overall risk of the center.

        Parameters
        ----------
        use_quality_for_risk: bool
            If True, we use the quality risk to calculate the overall risk of the center.
        """
        if use_quality_for_risk:
            risks = [self.risk_gain_median, self.risk_quantite, self.risk_quality]
        else:
            risks = [self.risk_gain_median, self.risk_quantite]

        if "uneligible" in risks:
            self.risk = "uneligible"
        elif "high" in risks:
            self.risk = "high"
        elif "moderate_1" in risks:
            self.risk = "moderate_1"
        elif "moderate_2" in risks:
            self.risk = "moderate_2"
        elif "moderate_3" in risks:
            self.risk = "moderate_3"
        elif "moderate" in risks:
            self.risk = "moderate"
        else:
            self.risk = "low"

    def define_gain_quantities(self, cout_verification_centre):
        """
        Define some quantities about the cost/gain of verification

        Parameters
        ----------
        cout_verification_centre: int
            The cost of verifying a center. It is inputed by the user.
        """
        self.diff_subsidies_tauxval_median_period = (
            self.subside_taux_period - self.subside_val_period
        )
        self.diff_subsidies_decval_median_period = self.subside_dec_period - self.subside_val_period

        if pd.isnull(self.diff_subsidies_tauxval_median_period):
            self.benefice_vbr = pd.NA
        else:
            self.benefice_vbr = self.diff_subsidies_tauxval_median_period - cout_verification_centre

    def get_diff_subsidies_decval_median(self):
        """
        Get the median of the diff_subsidies_decval_median
        This informs about how much money is saved on subsidies with verification. It is calculated as:
        (declared - validated) * tarif
        """
        self.diff_subsidies_decval_median = (
            self.quantite_window.groupby(self.period_type, as_index=False)["gain_verif"]
            .sum()["gain_verif"]
            .median()
        )

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

    def get_taux_validation_median(self):
        """
        Get the median of the taux validation, in general and per service.
        (The taux validation is 1 - (dec - val)/dec).
        The closer to 1, the better the center is doing.

        If there is no data, we say that none of the centers are verified.
        """
        self.taux_validation_par_service = self.quantite_window.groupby("service", as_index=False)[
            "taux_validation"
        ].median()

        self.taux_validation = self.taux_validation_par_service["taux_validation"].median()

        if self.taux_validation is pd.NA:
            self.taux_validation = 0


def hot_encode(condition):
    """
    Hot encode the condition

    Parameters
    ----------
    condition: bool
        The condition to hot encode.

    Returns
    -------
    int
        1 if condition is True, 0 otherwise.
    """
    if condition:
        return 1
    else:
        return 0


def calcul_ecarts(q):
    """
    Calculate the relations between the declared, verified and validated values.

    Parameters
    ----------
    q : pd.DataFrame
        DataFrame containing the quantitative information for the particular Organizational Unit

    Returns
    -------
    q: pd.DataFrame
        The same DataFrame with the new columns added.
    """
    q["ecart_dec_ver"] = q.apply(
        lambda x: abs(x.dec - x.ver) / x.ver if x.ver != 0 else x.dec,
        axis=1,
    )
    q["ecart_ver_val"] = q.apply(
        lambda x: abs(x.ver - x.val) / x.ver if x.ver != 0 else 0,
        axis=1,
    )
    q["taux_validation"] = q.apply(
        lambda x: min([1, 1 - (x.dec - x.val) / x.dec]) if x.dec != 0 else pd.NA,
        axis=1,
    )
    q["weighted_ecart_dec_val"] = 0.4 * q["ecart_dec_ver"] + 0.6 * q["ecart_ver_val"]
    q["ecart_dec_val"] = q.apply(
        lambda x: abs(x.dec - x.val) / x.ver if x.ver != 0 else 0,
        axis=1,
    )
    return q
