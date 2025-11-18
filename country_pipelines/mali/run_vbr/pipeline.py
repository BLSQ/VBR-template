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
    categorize_quantity,
    get_proportions,
    eligible_for_vbr,
)
import warnings
import random

from RBV_package import dates

import orgunit
from orgunit import Orgunit, GroupOrgUnits, VBR
import config

warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=FutureWarning)


@pipeline("run_vbr")
@parameter(
    "model",
    name="Name of the initialization file",
    help="It comes from the first pipeline",
    default="model",
    type=str,
    required=True,
)
@parameter("folder", name="Output folder name", type=str, default="Extraction")
@parameter(
    "mois_start",
    name="Start month of the simulation",
    type=int,
    choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    help="If frequency = quarter: put a month that is part of the quarter",
    default=10,
)
@parameter(
    "year_start",
    name="Start year for the simulation",
    type=int,
    choices=[2025, 2026, 2027],
    default=2025,
)
@parameter(
    "mois_fin",
    name="End month For the simulation",
    type=int,
    choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    help="If frequency = quarter: put a month that is part of the quarter",
    default=1,
)
@parameter(
    "year_fin",
    name="End year for the simulation",
    type=int,
    choices=[2025, 2026, 2027],
    default=2026,
)
@parameter(
    "frequence",
    name="Frequency of verification",
    type=str,
    choices=["quarter", "month"],
    default="quarter",
)
@parameter(
    "window",
    name="Number of months that will be considered in the simulation.",
    help="Historical data we will take into account in our simulation.",
    type=int,
    choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
    default=6,
)
@parameter(
    "nb_period_verif",
    name="Minimum number of verification visits in the past (in months)",
    help="If the center was verified less than these amount of times, we will automatically verify it.",
    type=int,
    choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    default=1,
)
@parameter(
    "prix_verif",
    name="Cost of verification",
    type=int,
    help="How much it costs to verify a center, including transport, per diems, etc (XOF)",
    default=100000,
)
@parameter(
    "paym_method_nf",
    name="Payment method for non-verified centers",
    help="complet: full payment; tauxval: payment based on taux of validation of the service/center",
    type=str,
    choices=["complet", "tauxval"],
    default="tauxval",
)
@parameter(
    "proportion_selection_bas_risque",
    name="Percentage of low risk centers selected for verification",
    type=float,
    help="Among the low risk centers, we will randomly select this percentage of centers to be verified",
    default=0.1,
)
@parameter(
    "proportion_selection_moyen_risque",
    name="Percentage of moderate risk centers selected for verification",
    type=float,
    help="Among the moderate risk centers, we will randomly select this percentage of centers to be verified",
    default=0.5,
)
@parameter(
    "proportion_selection_haut_risque",
    name="Percentage of high risk centers selected for verification",
    type=float,
    help="Among the high risk centers, we will randomly select this percentage of centers to be verified",
    default=1.0,
)
@parameter(
    "quantity_risk_calculation",
    name="Risk calculation method",
    type=str,
    choices=["ecart", "verifgain"],
    default="verifgain",
)
@parameter(
    "seuil_max_bas_risk",
    name="Threshold for low risk centers",
    type=float,
    help="Maximum difference between the declared, validated and verified values for low risk centers.",
    default=0.05,
)
@parameter(
    "seuil_max_moyen_risk",
    name="Threshold for moderate risk centers",
    type=float,
    help="Maximum difference between the declared, validated and verified values for moderate risk centers.",
    default=0.1,
)
@parameter(
    "verification_gain_low",
    name="Minimum verification gain for low risk centers",
    help="Per month",
    type=int,
    default=100,
    required=False,
)
@parameter(
    "verification_gain_mod",
    name="Minimum verification gain for moderate risk centers",
    help="Per month",
    type=int,
    default=75,
    required=False,
)
def run_vbr(
    model,
    folder,
    mois_start,
    year_start,
    mois_fin,
    year_fin,
    frequence,
    window,
    nb_period_verif,
    prix_verif,
    paym_method_nf,
    proportion_selection_bas_risque,
    proportion_selection_moyen_risque,
    proportion_selection_haut_risque,
    quantity_risk_calculation,
    seuil_max_bas_risk,
    seuil_max_moyen_risk,
    verification_gain_low,
    verification_gain_mod,
):
    vbr_object = get_environment(model)
    proportions = get_proportions(
        proportion_selection_bas_risque,
        proportion_selection_moyen_risque,
        proportion_selection_haut_risque,
    )

    set_vbr_values(
        vbr_object,
        frequence,
        nb_period_verif,
        proportions,
        prix_verif,
        window,
        paym_method_nf,
        quantity_risk_calculation,
        seuil_max_bas_risk,
        seuil_max_moyen_risk,
        verification_gain_low,
        verification_gain_mod,
    )

    start = get_month_or_quarter(mois_start, year_start, frequence)
    end = get_month_or_quarter(mois_fin, year_fin, frequence)
    list_periods = orgunit.get_date_series(str(start), str(end), frequence)

    path_verif, path_stats, path_service = create_folders(folder)

    for period in list_periods:
        simulate_period(vbr_object, model, period, path_verif, path_stats, path_service)


def simulate_period(
    vbr_object: VBR, model: str, period: str, path_verif: str, path_stats: str, path_service: str
):
    """
    Run the simulation for 1 period.

    Parameters
    ----------
    vbr_object: VBR
        Object containing the VBR configuration.
    model: str
        Name of the initialization file.
    period: str
        Period to simulate (in the format YYYYMM or YYYYQ).
    path_verif: str
        Path to store the verification information.
    path_stats: str
        Path to store the simulation statistics.
    path_service: str
        Path to store the service information.
    """
    current_run.log_info(f"Simulating period {period}")
    path_verif_group, path_stats_full, path_service_group = create_file_names(
        vbr_object, model, period, path_verif, path_stats, path_service
    )
    rows = []
    for group in vbr_object.groups:
        new_row = simulate_period_group(
            vbr_object, group, period, path_verif_group, path_service_group
        )
        rows.append(new_row)

    df_stats = pd.DataFrame(rows, columns=config.list_cols_df_stats)
    df_stats.to_csv(path_stats_full, index=False)


def simulate_period_group(
    vbr_object: VBR,
    group: GroupOrgUnits,
    period: str,
    path_verif_group: str,
    path_service_group: str,
):
    """
    Run the simulation for 1 period and 1 group.

    Parameters
    ----------
    vbr_object: VBR
        Object containing the VBR configuration.
    group: Group_Orgunits
        Object containing the information from a group of Organizational Units.
    period: str
        Period to simulate (in the format YYYYMM or YYYYQ).
    path_verif_group: str
        Path to store the verification information for the group.
    path_service_group: str
        Path to store the service information for the group.

    Returns
    -------
    stats: list
        Statistics of the simulation for the group and period.
    """
    for ou in group.members:
        simulate_period_ou(vbr_object, ou, period)

    full_path_verif = os.path.join(
        f"{path_verif_group}-prov___{group.name}.csv",
    )
    full_path_service = os.path.join(
        f"{path_service_group}-prov___{group.name}.csv",
    )

    group.get_verification_information()
    df_group_service = group.get_service_information()
    stats = group.get_statistics(vbr_object, period)

    group.df_verification.to_csv(
        full_path_verif,
        index=False,
    )
    df_group_service.to_csv(
        full_path_service,
        index=False,
    )

    return stats


def simulate_period_ou(vbr_object: VBR, ou: Orgunit, period: str):
    """
    Run the simulation for 1 period and 1 organizational unit.

    Parameters
    ----------
    vbr_object: VBR
        Object containing the VBR configuration.
    ou: Orgunit
        Object containing the information from a particular Organizational Unit.
    period: str
        Period to simulate (in the format YYYYMM or YYYYQ).
    """
    set_ou_values(vbr_object, ou, period)
    if eligible_for_vbr(ou, vbr_object):
        categorize_quantity(ou, vbr_object)
        ou.mix_risks()
    else:
        ou.risk_quantite = "uneligible"
        ou.risk = "uneligible"
    ou.set_verification(random.uniform(0, 1) <= vbr_object.proportions[ou.risk])


def set_vbr_values(
    vbr_object: VBR,
    frequence: str,
    nb_period_verif: int,
    proportions: dict,
    prix_verif: int,
    window: int,
    paym_method_nf: str,
    quantity_risk_calculation: str,
    seuil_max_bas_risk: float,
    seuil_max_moyen_risk: float,
    verification_gain_low: int,
    verification_gain_mod: int,
):
    """
    Set the VBR configuration values.
    (Fill all of the attributes of the VBR object with the parameters given by the user).

    Parameters
    ----------
    vbr_object: VBR
        Object containing the VBR configuration.
    frequence: str
        Frequency of verification.
    nb_period_verif: int
        Minimum number of verification visits in the past (in months).
    proportions: dict
        Proportions of centers to verify per risk category.
    prix_verif: int
        Cost of verification.
    window: int
        Number of months that will be considered in the simulation.
    paym_method_nf: str
        Payment method for non-verified centers.
    quantity_risk_calculation: str
        Risk calculation method.
    seuil_max_bas_risk: float
        Threshold for low risk centers.
    seuil_max_moyen_risk: float
        Threshold for moderate risk centers.
    verification_gain_low: int
        Minimum verification gain for low risk centers.
    verification_gain_mod: int
        Minimum verification gain for moderate risk centers.
    """
    vbr_object.set_frequence(frequence)
    vbr_object.set_nb_verif_min_per_window(nb_period_verif)
    vbr_object.set_proportions(proportions)
    vbr_object.set_cout_verification(prix_verif)
    vbr_object.set_window(window)
    vbr_object.set_payment_method(paym_method_nf)
    vbr_object.set_quantity_risk_calculation(quantity_risk_calculation)
    vbr_object.set_seuil_max_bas_risk(seuil_max_bas_risk)
    vbr_object.set_seuil_max_moyen_risk(seuil_max_moyen_risk)
    vbr_object.set_verification_gain_low(verification_gain_low)
    vbr_object.set_verification_gain_mod(verification_gain_mod)


def create_folders(folder: str) -> tuple[str, str, str]:
    """
    Create the necessay folders for the simulation.

    Parameters
    ----------
    folder : str
        Name of the folder where we want to store the results (eg: Extraction).

    Returns
    -------
    verification : str
        Path to store the verification information.
    simulation : str
        Path to store the simulation statistics.
    service : str
        Path to store the service information.
    """
    base_path = os.path.join(workspace.files_path, "pipelines/run_vbr", folder, "data")
    subdirs = ["verification_information", "simulation_statistics", "service_information"]

    for subdir in subdirs:
        os.makedirs(os.path.join(base_path, subdir), exist_ok=True)

    verification = os.path.join(base_path, "verification_information")
    simulation = os.path.join(base_path, "simulation_statistics")
    service = os.path.join(base_path, "service_information")

    return verification, simulation, service


def get_month_or_quarter(mois: int, year: int, frequence: str) -> str:
    """
    From month and year, get the month in the format YYYYMM or quarter in the format YYYYQ.

    Parameters
    ----------
    mois : int
        Month of the year.
    year : int
        Year.
    frequence : str
        Frequency of verification (either 'month' or 'quarter').

    Returns
    -------
    str
        Month in the format YYYYMM or quarter in the format YYYYQ.
    """
    if frequence == "quarter":
        trimestre = dates.month_to_quarter(year * 100 + mois)
        return trimestre
    elif frequence == "month":
        return str(year * 100 + mois)
    else:
        raise ValueError("Frequence must be either 'month' or 'quarter'")


def get_environment(nom_init: str) -> VBR:
    """
    Load the simulation initialization data.

    Parameters
    ----------
    nom_init : str
        Name of the initialization file to load. The user choose it.

    Returns
    -------
    VBR
        Object containing the VBR configuration.
    """
    current_run.log_info("Chargement des données d'initialisation de la simulation")
    data_path = f"{workspace.files_path}/pipelines/initialize_vbr/"
    with open(f"{data_path}initialization_simulation/{nom_init}.pickle", "rb") as file:
        vbr_object = pickle.load(file)
    return vbr_object


def create_file_names(
    vbr_object: VBR, model: str, period: str, path_verif: str, path_stats: str, path_service: str
):
    """
    Create the file names for the simulation results.

    Parameters
    ----------
    vbr_object: VBR
        Object containing the VBR configuration.
    model: str
        Name of the initialization file.
    period: str
        Period to simulate (in the format YYYYMM or YYYYQ).
    path_verif: str
        Path to store the verification information.
    path_stats: str
        Path to store the simulation statistics.
    path_service: str
        Path to store the service information.

    Returns
    -------
    path_verif_per_group: str
        Path to store the verification information for the group.
    full_path_stats: str
        Path to store the simulation statistics.
    path_services_per_group: str
        Path to store the service information for the group.
    """
    file_name_per_group = (
        f"model___{model}"
        f"month___{period}"
        f"-frq___{vbr_object.period_type}"
        f"-obswin___{vbr_object.window}"
        f"-minnb___{vbr_object.nb_periods}"
        f"-cvrf___{vbr_object.cout_verification_centre}"
        f"-pai___{vbr_object.paym_method_nf}"
        f"-plow___{vbr_object.proportions['low']}"
        f"-pmid___{vbr_object.proportions['moderate']}"
        f"-phigh___{vbr_object.proportions['high']}"
        f"-qtrisk___{vbr_object.quantity_risk_calculation}"
        f"-seum___{vbr_object.seuil_max_moyen_risk}"
        f"-seub___{vbr_object.seuil_max_bas_risk}"
        f"-vglow___{vbr_object.verification_gain_low}"
        f"-vgmod___{vbr_object.verification_gain_mod}"
    )
    file_name_stats = (
        f"model___{model}"
        f"month___{period}"
        f"-frq___{vbr_object.period_type}"
        f"-obswin___{vbr_object.window}"
        f"-minnb___{vbr_object.nb_periods}"
        f"-cvrf___{vbr_object.cout_verification_centre}"
        f"-pai___{vbr_object.paym_method_nf}"
        f"-plow___{vbr_object.proportions['low']}"
        f"-pmid___{vbr_object.proportions['moderate']}"
        f"-phigh___{vbr_object.proportions['high']}"
        f"-qtrisk___{vbr_object.quantity_risk_calculation}"
        f"-seum___{vbr_object.seuil_max_moyen_risk}"
        f"-seub___{vbr_object.seuil_max_bas_risk}"
        f"-vglow___{vbr_object.verification_gain_low}"
        f"-vgmod___{vbr_object.verification_gain_mod}"
        ".csv"
    )

    full_path_stats = os.path.join(path_stats, file_name_stats)
    path_verif_per_group = os.path.join(path_verif, file_name_per_group)
    path_services_per_group = os.path.join(path_service, file_name_per_group)

    return path_verif_per_group, full_path_stats, path_services_per_group


def set_ou_values(vbr_object: VBR, ou: Orgunit, period: str):
    """
    Set all of the relevant indicators for a particular organizational unit and period.

    Parameters
    ----------
    vbr_object: VBR
        Object containing the VBR configuration.
    ou: Orgunit
        Object containing the information from a particular Organizational Unit.
    period: str
        Period to simulate (in the format YYYYMM or YYYYQ).
    """
    ou.set_period_verification(vbr_object, period)
    ou.set_window_df(vbr_object)
    ou.set_period_df(vbr_object)
    ou.calculate_ecart_median_window()
    ou.calculate_taux_median_window()
    ou.calculate_subsidies_period(vbr_object.paym_method_nf)
    ou.calculate_diff_subsidies_period(vbr_object)
    ou.calculate_gain_verification_window(
        vbr_object.cout_verification_centre, vbr_object.period_type, vbr_object.paym_method_nf
    )


if __name__ == "__main__":
    run_vbr()
