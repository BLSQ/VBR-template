import pandas as pd

from RBV_package import dates

from RBV_package import config_package as config


class Orgunit:
    """
    A class for the organizational unit.

    Attributes
    ----------
    benefice_vbr: np.float64
        Amount of money won per center with VBR. It is calculated as:
            subsidies based on declared values (possibly adjusted by taux)
            minus subsidies based on validated values
            minus cost of verification.
        If it is bigger than zero, then we should not do VBR.
    diff_subsidies_decval_median_period: np.float64
        The median of:
            the difference in subsidies that the center would recieve based on the declared and  validated values.
        The bigger, the more extra subsidies the center would get if it wasn't verified.
        Its calculated for the period of the simulation.
    diff_subsidies_tauxval_median_period: np.float64
        The median of:
            the difference in subsidies that the center would recieve based on the declared*taux and validated values.
        The bigger, the more extra subsidies the center would get if it wasn't verified.
        Its calculated for the period of the simulation.
    id: str
        The id of the organizational unit.
    month: str
        The month we are running the simulation for.
        (if period_type == "month", it is the same as self.period)
    nb_periods: int
        Minimum number of months with dec-val data (during the observation window) to be eligible for VBR.
        It is inputed by the user.
    nb_periods_verified: array
        Number of periods in which the Organizational Unit has been verified.
    nb_services: int
        Number of services in the Organizational Unit.
    period: str
        The period (month or quarter) we are running the simulation for.
    period_type: str
        Frequency of the verification, either "month" or "quarter". It is inputed by the user.
    quantite: pd.DataFrame
        The quantitative data for the center.
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


    Attributes - old (unrevised)
    ----------

    diff_subsidies_decval_median: np.float64
        The median of:
            the difference in subsidies that the center would recieve based on the declared or validated values.
        It is calculated as: median((declared - validated) * tarif)
        The bigger, the more extra subsidies the center would get if it wasn't verified.
        Its calculated for all of the observation window.

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

    identifier_verification: list
        List with the ID/names of the level 2, 3, 4, 5 and 6 of the center.
    is_verified: bool
        True if the center will be verified, False otherwise.
    nb_services_risky: int
        Number of services that are not at low risk.
    nb_services_moyen_risk: int
        Number of services whose seuil is under the medium risk threshold.
    qualite: pd.DataFrame
        The qualitative data for the center.
        Right now we don't do anything with it.
    qualite_indicators: list
        The list of the indicators that are used to calculate the quality of the centers.
        Right now we don't do anything with them.
    qualite_window: pd.DataFrame
        Qualitative data for the observation window.
        Right now we don't do anything with it.
    risk_gain_median: str
        The risk of the center based on how much we win by verifying it.
    taux_validation: np.float64
        The median of the taux_validation for the center.
        (The taux validation is 1 - (dec - val)/dec. The closer to one, the more the center tells the truth)
    taux_validation_par_service: pd.DataFrame
        The median of the taux_validation for each service for the center.
        (The taux validation is 1 - (dec - val)/dec. The closer to one, the more the center tells the truth)
    """

    def __init__(self, ou_id, uneligible_vbr, quantite, qualite=None, qualite_indicators=None):
        self.id = ou_id

        if uneligible_vbr:
            self.risk = "uneligible"
        else:
            self.risk = "unknown"

        self.initialize_quantite(quantite)
        self.initialize_qualite(qualite, qualite_indicators)

        self.period_type = ""
        self.period = ""
        self.month = ""
        self.quarter = ""

        self.nb_periods = None
        self.nb_periods_verified = None

        self.nb_services = None
        self.subside_dec_period = None
        self.subside_val_period = None
        self.subside_taux_period = None
        self.diff_subsidies_tauxval_median_period = None
        self.diff_subsidies_decval_median_period = None
        self.benefice_vbr = None

    def initialize_quantite(self, quantite):
        """
        Initialize the quantity data.
        """
        self.quantite = quantite.sort_values(by=["ou", "service", "quarter", "month"])
        if "level_6_uid" not in self.quantite.columns:
            self.quantite.loc[:, "level_6_uid"] = pd.NA
            self.quantite.loc[:, "level_6_name"] = pd.NA
        try:
            self.quantite.loc["month"] = self.quantite["month"].astype("Int64").astype(str)
        except ValueError:
            self.quantite["month"] = self.quantite["month"].astype(str)

        self.quantite_window = pd.DataFrame()
        self.risk_quantite = "unknown"

    def initialize_qualite(self, qualite, qualite_indicators):
        """
        Initialize the quality data.

        NOTE: THIS IS NOT REVISED.
        """
        if qualite.empty:
            if qualite_indicators is not None:
                raise ValueError(
                    "If qualite_indicators is provided, qualite dataframe must also be provided."
                )
            return

        if qualite_indicators is None:
            if not qualite.empty:
                raise ValueError(
                    "If qualite dataframe is provided, qualite_indicators must also be provided."
                )
            return

        self.qualite_indicators = qualite_indicators
        self.qualite = qualite.sort_values(by=["ou", "indicator", "quarter"])
        self.qualite = self.qualite.drop_duplicates(["ou", "indicator", "quarter"])

        if "level_6_uid" not in self.qualite.columns:
            self.qualite.loc[:, "level_6_uid"] = pd.NA
            self.qualite.loc[:, "level_6_name"] = pd.NA

        try:
            self.qualite.loc["month"] = self.qualite["month"].astype("Int64").astype(str)
        except ValueError:
            self.qualite["month"] = self.qualite["month"].astype(str)

        self.indicator_scores = {}
        self.general_quality = 0
        self.hygiene = 0
        self.finance = 0
        self.quality_high_risk = ""
        self.quality_mod_risk = ""
        self.quality_low_risk = ""
        self.risk_quality = ""
        self.qualite_window = pd.DataFrame()

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
                # Note that you should never pass through here.
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
        self.ecart_median_gen = self.quantite_window["weighted_ecart_dec_val"].median()
        self.ecart_avg_gen = self.quantite_window["weighted_ecart_dec_val"].mean()

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
