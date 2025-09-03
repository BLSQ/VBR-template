from openhexa.sdk import (
    current_run,
    pipeline,
    workspace,
    parameter,
)
import pickle
import pandas as pd
import os
from vbr_custom import (
    categorize_quality,
    categorize_quantity_ecart,
    categorize_quantity_gain,
    define_risky_services,
    get_proportions,
    assign_taux_validation_per_zs,
    eligible_for_vbr,
)
import warnings
import random

from RBV_package import dates

import toolbox
from config_toolbox import list_cols_df_stats

warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=FutureWarning)


@pipeline("run_vbr_burundi")
@parameter(
    "nom_init",
    name="Nom du fichier d'initialisation pour la simulation",
    default="model",
    type=str,
    required=True,
)
@parameter(
    "frequence",
    name="Verification une fois par:",
    help="Une visite par mois ou une visite par trimestre",
    type=str,
    choices=["mois", "trimestre"],
    default="mois",
)
@parameter(
    "mois_start",
    name="Mois de debut de la simulation",
    type=int,
    choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    help="Si frequence = trimestre : mettre un mois faisant parti du trimestre",
    default=1,
)
@parameter(
    "year_start",
    name="Annee de debut de la simulation",
    type=int,
    choices=[2023, 2024, 2025],
    help="Annee de debut de la simulation",
    default=2024,
)
@parameter(
    "mois_fin",
    name="Mois de fin de la simulation",
    type=int,
    choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    help="Si frequence = trimestre : mettre un mois faisant parti du trimestre",
    default=12,
)
@parameter(
    "year_fin",
    name="Annee de fin de la simulation",
    type=int,
    choices=[2023, 2024, 2025],
    help="Annee de fin de la simulation",
    default=2024,
)
@parameter(
    "prix_verif",
    name="Cout de verification (euros)",
    type=int,
    help="Cout de verification par centre de sante",
    default=150000,
)
@parameter(
    "seuil_gain_verif_median",
    name="Gain de verification median a partir duquel un centre est a haut risque (euros) ",
    type=int,
    help="Gain de verification median a partir duquel un centre est a haut risque (euros) ",
    default=200000,
)
@parameter(
    "seuil_max_bas_risk",
    name="Seuil maximal pour categorie de risque faible",
    type=float,
    help="Seuil maximal pour categorie de risque faible",
    default=0.05,
)
@parameter(
    "seuil_max_moyen_risk",
    name="Seuil maximal pour categorie de risque modere",
    type=float,
    help="Seuil maximal pour categorie de risque modere",
    default=0.1,
)
@parameter(
    "window",
    name="fenetre d'observation minimum (# de mois)",
    help="Number of months that will be considered in the simulation.",
    type=int,
    choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
    default=6,
)
@parameter(
    "nb_period_verif",
    name="nombre minimum de visites effectuees dans le passe",
    help="nombre de periodes minimum ayant ete verifies sur la fenetre d'observation pour etre eligible a la VBR",
    type=int,
    choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    default=3,
)
@parameter(
    "proportion_selection_bas_risque",
    name="Pourcentage de centres selectionnes a risque faible",
    type=float,
    help="Pourcentage de centres sélectionnes parmi la catégorie a risque faible",
    default=0.1,
)
@parameter(
    "proportion_selection_haut_risque",
    name="Pourcentage de centres selectionnes a risque eleve",
    type=float,
    help="Pourcentage de centres sélectionnes parmi la catégorie a risque eleve",
    default=1.0,
)
@parameter(
    "paym_method_nf",
    name="Paiement des centres non-verifies",
    help="Quelle methode de paiement utilise t'on pour payer les centres non-verifies (complet = payer sur base des quantites declarees; taux de validation personnel/ZS : payer sur base du montant declare multiplie par le taux de validation median personnel/de la ZS sur les precedentes periodes)",
    type=str,
    choices=["complet", "tauxvalidation", "tauxvalidationZS"],
    default="tauxvalidation",
)
@parameter(
    "use_quality_for_risk",
    name="Utiliser donnees qualite pour le risque",
    help=" (finance/hygiene/general) utilises selon les regles du PMNS pour definir le risque",
    type=bool,
    default=False,
)
@parameter(
    "max_nb_services",
    name="Maximum number of services that can have a ecart bigger than seuil_max_moyen_risk",
    help="Threshold for the number of services that can have a weighted_ecart_dec_val bigger than seuil_max_moyen_risk without the center being high risk",
    type=int,
    default=0,
)
@parameter(
    "quantity_risk_calculation",
    name="The calculation method for the risk based on the val/dec/ver data. ",
    type=str,
    choices=["standard", "verificationgain", "ecartdecver"],
    default="ecartdecver",
)
@parameter(
    "verification_gain_low",
    name="Maximum verification gain for low risk centers",
    help="Per month",
    type=int,
    default=150000,
    required=False,
)
@parameter(
    "verification_gain_mod",
    name="Maximum verification gain for moderate risk centers",
    help="Per month",
    type=int,
    default=100000,
    required=False,
)
@parameter("folder", name="Folder", type=str, default="Extraction")
def run_vbr_burundi(
    nom_init,
    frequence,
    mois_start,
    year_start,
    mois_fin,
    year_fin,
    prix_verif,
    seuil_gain_verif_median,
    seuil_max_bas_risk,
    seuil_max_moyen_risk,
    window,
    nb_period_verif,
    proportion_selection_bas_risque,
    proportion_selection_haut_risque,
    paym_method_nf,
    use_quality_for_risk,
    folder,
    max_nb_services,
    quantity_risk_calculation,
    verification_gain_low,
    verification_gain_mod,
):
    regions = get_environment(nom_init)
    start = get_month(mois_start, year_start)
    end = get_month(mois_fin, year_fin)

    path_data = create_folders(folder)
    path_verif = create_subfolder(path_data, "verification_information")
    path_stats = create_subfolder(path_data, "simulation_statistics")
    path_service = create_subfolder(path_data, "service_information")

    proportions = get_proportions(
        proportion_selection_bas_risque,
        proportion_selection_haut_risque,
    )

    run_simulation(
        regions,
        frequence,
        start,
        end,
        prix_verif,
        seuil_gain_verif_median,
        seuil_max_bas_risk,
        seuil_max_moyen_risk,
        window,
        nb_period_verif,
        proportion_selection_bas_risque,
        proportion_selection_haut_risque,
        paym_method_nf,
        use_quality_for_risk,
        nom_init,
        path_service,
        path_stats,
        path_verif,
        proportions,
        max_nb_services,
        quantity_risk_calculation,
        verification_gain_low,
        verification_gain_mod,
    )


@run_vbr_burundi.task
def create_folders(folder):
    """
    Create the necessay folders for the simulation.

    Parameters
    ----------
    folder : str
        Name of the folder where we want to store the results (eg: PDF Burundi extraction).

    Returns
    -------
    path_data : str
        Path to the folder where we will store the results.
    """
    dict_paths = {
        folder: {
            "data": ["verification_information", "simulation_statistics", "service_information"]
        }
    }

    os.makedirs(os.path.join(workspace.files_path, "pipelines/run_vbr", folder), exist_ok=True)

    for subdir in dict_paths[folder]:
        os.makedirs(
            os.path.join(workspace.files_path, "pipelines/run_vbr", folder, subdir), exist_ok=True
        )
        for subdir2 in dict_paths[folder][subdir]:
            os.makedirs(
                os.path.join(workspace.files_path, "pipelines/run_vbr", folder, subdir, subdir2),
                exist_ok=True,
            )

    path_data = os.path.join(workspace.files_path, "pipelines/run_vbr", folder, subdir)

    return path_data


@run_vbr_burundi.task
def create_subfolder(folder, subfolder):
    """
    From a folder and a path, create a full path.

    Parameters
    ----------
    folder : str
        Name of the full path to the parent folder.
    subfolder : str
        Name of the subfolder to be created.

    Returns
    -------
    full_path : str
        Full path to the subfolder.
    """
    full_path = os.path.join(folder, subfolder)
    return full_path


@run_vbr_burundi.task
def get_month(mois, year):
    """
    From month and year, get the month in the format YYYYMM.

    Parameters
    ----------
    mois : int
        Month of the year.
    year : int
        Year.

    Returns
    -------
    int
        Month in the format YYYYMM.
    """
    return year * 100 + mois


@run_vbr_burundi.task
def run_simulation(
    regions,
    frequence,
    start,
    end,
    prix_verif,
    seuil_gain_verif_median,
    seuil_max_bas_risk,
    seuil_max_moyen_risk,
    window,
    nb_period_verif,
    proportion_selection_bas_risque,
    proportion_selection_haut_risque,
    paym_method_nf,
    use_quality_for_risk,
    model_name,
    path_service,
    path_stats,
    path_verif,
    proportions,
    max_nb_services,
    quantity_risk_calculation,
    verification_gain_low,
    verification_gain_mod,
):
    """
    Run the simulation.
    We will create three folders:
    (1) verification_information: We have one file per period and per region.
        Here, we store the information about verification of the centers -- if they are verified or not, and how
        much money they win / loose if they are verified/not/how.
    (2) simulation_statistics: Here we store the results of the simulation, per period.
        We have some statistics about the amount of money / won lost with simulations.
    (3) service_information: We have one file per period and per region.
        Here we have the information per service.

    Parameters
    ----------
    regions :  list of Group_Orgunits.
        The initialization data. It has a list of Group_Orgunits, composed by OrgUnits objects
        (each of the OrgUnits objects contains the data for a particular Organizational Unit).
    frequence : str
        Frequency of the simulation, either "mois" or "trimestre". It is inputed by the user.
    start: int
        The start date for the period to be considered. It is inputed by the user.
    end : int
        The end date for the period to be considered. It is inputed by the user.
    prix_verif : int
        How much it costs to verify a center (euros). It is inputed by the user.
    seuil_gain_verif_median : int
        Median verification gain from which the center is considered at high risk (euros). It is inputed by the user.
    seuil_max_bas_risk : float
        Threshold for low risk. It is inputed by the user.
        (We will compare it against a measure combining the dec/ver/val quantities)
    seuil_max_moyen_risk : float
        Threshold for medium risk. It is inputed by the user.
        (We will compare it against a measure combining the dec/ver/val quantities)
    window : int
        The minimum number of months we want to consider for the simulation. It is inputed by the user.
    nb_period_verif :int
        Minimum number of months with dec-val data (during the observation window) to be eligible for VBR.
        It is inputed by the user.
    proportion_selection_bas_risque : float
        The probability for a center with low risk to be verified.
        It is inputed by the user.
    proportion_selection_haut_risque : float
        The probability for a center with high risk to be verified.
        It is inputed by the user.
    paym_method_nf : str
        It tells us how we will pay the centers that are not verified.
        It is inputed by the user.
    use_quality_for_risk : bool
        If true, we use the quality data to evaluate the risk of the center.
        It is inputed by the user.
    model_name : str
        Name of the initialization file to load.
    path_service: str
        The path to store the csv with the information per service in.
    path_stats: str
        The path to store the csv with the statistics information in.
    path_verif: str
        The path to store the csv with the verification information in.
    proportions: dict
        Dictionary with the verification probabilities for each risk category.
    max_nb_services: int
        Threshold for the number of services that can have a weighted_ecart_dec_val
        bigger than seuil_max_moyen_risk without the center being high risk
    """
    for month in [int(str(m)) for m in dates.get_date_series(str(start), str(end), frequence)]:
        if frequence == "trimestre" and month % 100 % 3 != 0:
            continue

        current_run.log_info(f"Simulating the verification for {month}")

        path_verif_per_group, full_path_stats, path_service_per_group = create_file_names(
            path_stats,
            path_verif,
            path_service,
            frequence,
            seuil_gain_verif_median,
            window,
            nb_period_verif,
            proportion_selection_bas_risque,
            proportion_selection_haut_risque,
            prix_verif,
            seuil_max_bas_risk,
            seuil_max_moyen_risk,
            paym_method_nf,
            use_quality_for_risk,
            month,
            model_name,
            max_nb_services,
            quantity_risk_calculation,
            verification_gain_low,
            verification_gain_mod,
        )

        period = set_period(frequence, month)

        rows = []

        for group in regions:
            new_row = simulate_month_group(
                group,
                path_service_per_group,
                path_verif_per_group,
                frequence,
                period,
                prix_verif,
                seuil_gain_verif_median,
                seuil_max_bas_risk,
                seuil_max_moyen_risk,
                window,
                nb_period_verif,
                paym_method_nf,
                use_quality_for_risk,
                proportions,
                max_nb_services,
                quantity_risk_calculation,
                verification_gain_low,
                verification_gain_mod,
            )
            rows.append(new_row)

        df_stats = pd.DataFrame(rows, columns=list_cols_df_stats)

        df_stats.to_csv(full_path_stats, index=False)


@run_vbr_burundi.task
def get_environment(nom_init):
    """
    Load the simulation initialization data.

    Parameters
    ----------
    nom_init : str
        Name of the initialization file to load. The user choose it.

    Returns
    -------
    regions :  list of Group_Orgunits.
        The initialization data. It has a list of Group_Orgunits, composed by OrgUnits objects
        (each of the OrgUnits objects contains the data for a particular Organizational Unit).
    """
    current_run.log_info("Chargement des données d'initialisation de la simulation")
    data_path = f"{workspace.files_path}/pipelines/initialize_vbr/"
    with open(f"{data_path}initialization_simulation/{nom_init}.pickle", "rb") as file:
        # Deserialize and load the object from the file
        regions = pickle.load(file)
    return regions


def create_file_names(
    path_stats,
    path_verif,
    path_service,
    frequence,
    seuil_gain_verif_median,
    window,
    nb_period_verif,
    proportion_selection_bas_risque,
    proportion_selection_haut_risque,
    prix_verif,
    seuil_max_bas_risk,
    seuil_max_moyen_risk,
    paym_method_nf,
    use_quality_for_risk,
    month,
    model_name,
    max_nb_services,
    quantity_risk_calculation,
    verification_gain_low,
    verification_gain_mod,
):
    """
    Create the file names where the results will be stored

    Parameters
    ----------
    path_stats: str
        The path to store the csv with the statistics information in.
    path_verif: str
        The path to store the csv with the verification information in.
    path_service: str
        The path to store the csv with the information per service in.
    frequence : str
        Frequency of the simulation, either "mois" or "trimestre". It is inputed by the user.
    seuil_gain_verif_median : int
        Median verification gain from which the center is considered at high risk (euros). It is inputed by the user.
    window : int
        The minimum number of months we want to consider for the simulation. It is inputed by the user.
    nb_period_verif :int
        Minimum number of months with dec-val data (during the observation window) to be eligible for VBR.
        It is inputed by the user.
    proportion_selection_bas_risque : float
        The probability for a center with low risk to be verified.
        It is inputed by the user.
    proportion_selection_haut_risque : float
        The probability for a center with high risk to be verified.
        It is inputed by the user.
    prix_verif : int
        How much it costs to verify a center (euros). It is inputed by the user.
    seuil_max_bas_risk : float
        Threshold for low risk. It is inputed by the user.
        (We will compare it against a measure combining the dec/ver/val quantities)
    seuil_max_moyen_risk : float
        Threshold for medium risk. It is inputed by the user.
        (We will compare it against a measure combining the dec/ver/val quantities)
    paym_method_nf : str
        It tells us how we will pay the centers that are not verified.
        It is inputed by the user.
    use_quality_for_risk : bool
        If true, we use the quality data to evaluate the risk of the center.
        It is inputed by the user.
    month: int
        Month we are running the simulation for.
    model_name : str
        Name of the initialization file to load.
    max_nb_services: int
        Threshold for the number of services that can have a weighted_ecart_dec_val
        bigger than seuil_max_moyen_risk without the center being high risk


    Returns
    -------
    path_verif_per_group: str
        The path to the verification informations.
        We will create one csv per OU -- we will add a suffix to this name per each one
    full_path_stats:
        The full path to the .csv that will contain the statistics information.

    """
    file_name_verif = (
        f"mdl___{model_name}"
        f"-frq___{frequence}"
        f"-gvrf___{seuil_gain_verif_median}"
        f"-obswin___{window}"
        f"-minnb___{nb_period_verif}"
        f"-plow___{proportion_selection_bas_risque}"
        f"-phigh___{proportion_selection_haut_risque}"
        f"-cvrf___{prix_verif}"
        f"-seum___{seuil_max_moyen_risk}"
        f"-seub___{seuil_max_bas_risk}"
        f"-pai___{paym_method_nf}"
        f"-qlrisk___{use_quality_for_risk}"
        f"-mxs___{max_nb_services}"
        f"-qtrisk___{quantity_risk_calculation}"
        f"-vglow___{verification_gain_low}"
        f"-vgmod___{verification_gain_mod}"
    )

    path_verif_per_group = os.path.join(path_verif, file_name_verif)

    file_name_stats = (
        f"month___{month}"
        f"-frq___{frequence}"
        f"-gvrf___{seuil_gain_verif_median}"
        f"-obswin___{window}"
        f"-minnb___{nb_period_verif}"
        f"-plow___{proportion_selection_bas_risque}"
        f"-phigh___{proportion_selection_haut_risque}"
        f"-cvrf___{prix_verif}"
        f"-seum___{seuil_max_moyen_risk}"
        f"-seub___{seuil_max_bas_risk}"
        f"-pai___{paym_method_nf}"
        f"-qlrisk___{use_quality_for_risk}"
        f"-mxs___{max_nb_services}"
        f"-qtrisk___{quantity_risk_calculation}"
        f"-vglow___{verification_gain_low}"
        f"-vgmod___{verification_gain_mod}"
        ".csv"
    )

    full_path_stats = os.path.join(path_stats, f"model___{model_name}-{file_name_stats}")

    file_name_services = f"model___{model_name}"

    parth_services_per_group = os.path.join(path_service, file_name_services)

    return path_verif_per_group, full_path_stats, parth_services_per_group


def set_period(frequence, month):
    """
    Define the period we are running the simulation for.

    Parameters
    ----------
    frequence: str
        The frequence of the simulation
    month: int
        The month we are running the simulation for.

    Returns
    --------
    period: str or int (this is bad)
        The period we are running the simulation for, either a month or a quarter.

    """
    if frequence == "trimestre":
        quarter = str(dates.month_to_quarter(month))
        period = quarter
    else:
        period = month

    return period


def simulate_month_group(
    group,
    path_service_per_group,
    path_verif_per_group,
    frequence,
    period,
    prix_verif,
    seuil_gain_verif_median,
    seuil_max_bas_risk,
    seuil_max_moyen_risk,
    window,
    nb_period_verif,
    paym_method_nf,
    use_quality_for_risk,
    proportions,
    max_nb_services,
    quantity_risk_calculation,
    verification_gain_low,
    verification_gain_mod,
):
    """
    Run the simulation for a particular month.

    Parameters
    ----------
    group :  GroupOrgUnits.
        List of OrgUnits. Contains the information about the verifications for a particular area.
    path_service: str
        The path to store the csv with the information per service in.
    path_verif_per_group: str
        The path to store the csv with the verification information in. We will create a sub path per OU.
    frequence : str
        Frequency of the simulation, either "mois" or "trimestre". It is inputed by the user.
    period: str or int
        The period we are running the current simulation for. Its either a month or a quarter.
    prix_verif : int
        How much it costs to verify a center (euros). It is inputed by the user.
    seuil_gain_verif_median : int
        Median verification gain from which the center is considered at high risk (euros). It is inputed by the user.
    seuil_max_bas_risk : float
        Threshold for low risk. It is inputed by the user.
        (We will compare it against a measure combining the dec/ver/val quantities)
    seuil_max_moyen_risk : float
        Threshold for medium risk. It is inputed by the user.
        (We will compare it against a measure combining the dec/ver/val quantities)
    window : int
        The minimum number of months we want to consider for the simulation. It is inputed by the user.
    nb_period_verif :int
        Minimum number of months with dec-val data (during the observation window) to be eligible for VBR.
        It is inputed by the user.
    paym_method_nf : str
        It tells us how we will pay the centers that are not verified.
        It is inputed by the user.
    use_quality_for_risk : bool
        If true, we use the quality data to evaluate the risk of the center.
        It is inputed by the user.
    proportions: dict
        Dictionary with the verification probabilities for each risk category.
    max_nb_services: int
        Threshold for the number of services that can have a weighted_ecart_dec_val
        bigger than seuil_max_moyen_risk without the center being high risk

    Returns
    -------
    stats: tuple
        Contains the statistics for this group and period
    """
    initialize_group(group, proportions, prix_verif, paym_method_nf)

    for ou in group.members:
        process_ou(
            group,
            ou,
            frequence,
            period,
            nb_period_verif,
            window,
            paym_method_nf,
            seuil_gain_verif_median,
            seuil_max_bas_risk,
            seuil_max_moyen_risk,
            use_quality_for_risk,
            max_nb_services,
            quantity_risk_calculation,
            verification_gain_low,
            verification_gain_mod,
        )

    full_path_verif = os.path.join(
        f"{path_verif_per_group}-prov___{group.name}-prd___{period}.csv",
    )
    toolbox.get_verification_information(group)
    df_group_service = group.get_service_information()
    stats = toolbox.get_statistics(group, period)

    group.df_verification.to_csv(
        full_path_verif,
        index=False,
    )

    full_path_service = os.path.join(
        f"{path_service_per_group}-prov___{group.name}-prd___{period}-service.csv",
    )
    df_group_service.to_csv(
        full_path_service,
        index=False,
    )

    return stats


def set_ou_values(ou, frequence, period, nb_period_verif, window):
    """
    Define the attributes for a particular organizational until.

    Parameters:
    -----------
    ou: Orgunit
        Contains all of the information and methods for this particular Organizational Unit
    frequence : str
        Frequency of the simulation, either "mois" or "trimestre". It is inputed by the user.
    period: str or int
        The period we are running the current simulation for. Its either a month or a quarter.
    nb_period_verif :int
        Minimum number of months with dec-val data (during the observation window) to be eligible for VBR.
        It is inputed by the user.
    window : int
        The minimum number of months we want to consider for the simulation. It is inputed by the user.
    """
    ou.set_frequence(frequence)
    ou.set_month_verification(period)
    ou.set_nb_verif_min_per_window(nb_period_verif)
    if pd.api.types.is_numeric_dtype(ou.quantite["month"]):
        ou.quantite["month"] = ou.quantite["month"].astype("Int64").astype(str)
    if pd.api.types.is_numeric_dtype(ou.qualite["month"]):
        ou.qualite["month"] = ou.qualite["month"].astype("Int64").astype(str)
    ou.set_window(window)
    toolbox.get_ecart_median(ou)
    ou.get_diff_subsidies_decval_median()
    ou.get_taux_validation_median()
    quantite_month = ou.quantite_window[ou.quantite_window.month == ou.period]
    ou.dhis2_is_not_verified = quantite_month.dhis2_is_not_verified.any()


def process_ou(
    group,
    ou,
    frequence,
    period,
    nb_period_verif,
    window,
    paym_method_nf,
    seuil_gain_verif_median,
    seuil_max_bas_risk,
    seuil_max_moyen_risk,
    use_quality_for_risk,
    max_nb_services,
    quantity_risk_calculation,
    verification_gain_low,
    verification_gain_mod,
):
    """
    Process a particular Organizational Unit.
    We will set its values, get it's risk and the relevant gains/losses related to verification.

    Parameters
    -----------
    group :  GroupOrgUnits.
        List of OrgUnits. Contains the information about the verifications for a particular area.
    ou: Orgunit
        Contains all of the information and methods for this particular Organizational Unit
    frequence : str
        Frequency of the simulation, either "mois" or "trimestre". It is inputed by the user.
    period: str or int
        The period we are running the current simulation for. Its either a month or a quarter.
    nb_period_verif :int
        Minimum number of months with dec-val data (during the observation window) to be eligible for VBR.
        It is inputed by the user.
    window : int
        The minimum number of months we want to consider for the simulation. It is inputed by the user.
    paym_method_nf : str
        It tells us how we will pay the centers that are not verified.
        It is inputed by the user.
    seuil_gain_verif_median : int
        Median verification gain from which the center is considered at high risk (euros). It is inputed by the user.
    seuil_max_bas_risk : float
        Threshold for low risk. It is inputed by the user.
        (We will compare it against a measure combining the dec/ver/val quantities)
    seuil_max_moyen_risk : float
        Threshold for medium risk. It is inputed by the user.
        (We will compare it against a measure combining the dec/ver/val quantities)
    use_quality_for_risk : bool
        If true, we use the quality data to evaluate the risk of the center.
        It is inputed by the user.
    max_nb_services: int
        Threshold for the number of services that can have a weighted_ecart_dec_val
        bigger than seuil_max_moyen_risk without the center being high risk
    """
    set_ou_values(ou, frequence, period, nb_period_verif, window)

    if paym_method_nf == "complet":
        ou.get_gain_verif_for_period_verif(1)
    elif paym_method_nf == "tauxvalidation":
        ou.get_gain_verif_for_period_verif(ou.taux_validation)
    # If the payment method is "tauxvalidationZS" we will process the verification gains at group level.

    ou.define_gain_quantities(group.cout_verification_centre)

    if use_quality_for_risk:
        categorize_quality(ou)

    define_risky_services(ou, seuil_max_bas_risk, seuil_max_moyen_risk)

    if eligible_for_vbr(ou):
        ou.risk_weighted_ecart = categorize_quantity_ecart(
            max_nb_services,
            ou.nb_services_moyen_risk,
            ou.nb_services_risky,
        )
        ou.risk_ecart_dec_ver = categorize_quantity_ecart(
            max_nb_services,
            ou.nb_services_moyen_risk_dec_ver,
            ou.nb_services_risky_dec_ver,
        )
        ou.risk_gain_verif = categorize_quantity_gain(
            ou,
            verification_gain_low,
            verification_gain_mod,
        )
        if quantity_risk_calculation == "standard":
            ou.risk_quantite = ou.risk_weighted_ecart
        elif quantity_risk_calculation == "ecartdecver":
            ou.risk_quantite = ou.risk_ecart_dec_ver
        elif quantity_risk_calculation == "verificationgain":
            ou.risk_quantite = ou.risk_gain_verif

    else:
        ou.risk_quantite = "uneligible"
        ou.risk = "uneligible"
        ou.risk_weighted_ecart = "uneligible"
        ou.risk_ecart_dec_ver = "uneligible"
        ou.risk_gain_verif = "uneligible"

    ou.mix_risks(use_quality_for_risk)

    ou.set_verification(random.uniform(0, 1) <= group.proportions[ou.risk])


def initialize_group(group, proportions, prix_verif, paym_method_nf):
    """
    Initialize a particular group. We set its attributes and calculate the taux of validation.

    Parameters
    ----------
    group :  GroupOrgUnits.
        List of OrgUnits. Contains the information about the verifications for a particular area.
    proportions: dict
        Dictionary with the verification probabilities for each risk category.
    prix_verif : int
        How much it costs to verify a center (euros). It is inputed by the user.
    paym_method_nf : str
        It tells us how we will pay the centers that are not verified.
        It is inputed by the user.
    """
    group.set_proportions(proportions)
    group.set_cout_verification(prix_verif)

    if paym_method_nf == "tauxvalidationZS":
        assign_taux_validation_per_zs(group)


if __name__ == "__main__":
    run_vbr_burundi()
