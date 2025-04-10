from openhexa.sdk import (
    current_run,
    pipeline,
    workspace,
    parameter,
    DHIS2Connection,
)
from openhexa.toolbox.dhis2 import DHIS2
import pickle
import json, requests
import pandas as pd
import numpy as np
import typing
from io import StringIO
import os, traceback, requests
from sqlalchemy import create_engine
import papermill
from datetime import datetime, timedelta
from vbr_custom import *
from RBV_package import rbv_environment


@pipeline("run_vbr")
@parameter(
    "nom_init",
    name="Nom du fichier d'initialisation pour la simulation",
    default="pilote",
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
    default=6,
)
@parameter(
    "year_start",
    name="Annee de debut de la simulation",
    type=int,
    choices=[2023, 2024, 2025],
    help="Annee de debut de la simulation",
    default=2023,
)
@parameter(
    "mois_fin",
    name="Mois de fin de la simulation",
    type=int,
    choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    help="Si frequence = trimestre : mettre un mois faisant parti du trimestre",
    default=6,
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
    help="nombre de mois minimum avec donnees dec-val observables, precedant la verification, pour etre eligible à la VBR",
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
    default=0.4,
)
@parameter(
    "proportion_selection_moyen_risque",
    name="Pourcentage de centres selectionnes a risque modere",
    type=float,
    help="Pourcentage de centres sélectionnés parmi la catégorie à risque modere",
    default=0.8,
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
    choices=["complet", "taux validation personnel", "taux validation moyen ZS"],
    default="taux validation personnel",
)
@parameter(
    "use_quality_for_risk",
    name="Utiliser donnees qualite pour le risque",
    help=" (finance/hygiene/general) utilises selon les regles du PMNS pour definir le risque",
    type=bool,
    default=False,
)
@parameter("folder", name="Folder", type=str, default="PBF burundi extraction")
def run_vbr(
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
    proportion_selection_moyen_risque,
    proportion_selection_haut_risque,
    paym_method_nf,
    use_quality_for_risk,
    folder,
):
    regions = get_environment(nom_init)
    start = get_month(mois_start, year_start)
    end = get_month(mois_fin, year_fin)
    filename = run_simulation(
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
        proportion_selection_moyen_risque,
        proportion_selection_haut_risque,
        paym_method_nf,
        use_quality_for_risk,
        folder,
        nom_init,
    )
    run_pm(filename, folder, "result_VBR", {})
    # run_pm(
    #    filename,
    #    "push to dhis2",
    #    {"quarter": quarter, "year": year, "province": "kl Kwilu DPS"},
    # )
    run_pm(filename, folder, "Suivi du risque", {})


@run_vbr.task
def add_file(filename):
    current_run.add_file_output(filename)


@run_vbr.task
def run_pm(res, folder, name, params={}):
    current_run.log_info(f"Executing {name} notebook")
    papermill.execute_notebook(
        f"{workspace.files_path}/{folder}/{name}.ipynb",
        f"{workspace.files_path}/{folder}/{name}_output.ipynb",
        parameters=params,
        progress_bar=True,
    )


@run_vbr.task
def get_month(mois, year):
    return year * 100 + mois


@run_vbr.task
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
    proportion_selection_moyen_risque,
    proportion_selection_haut_risque,
    paym_method_NF,
    use_quality_for_risk,
    folder,
    model_name,
):
    path = {folder: {"data": ["Selections_Verif", "result_simulation", "listes_detaillees"]}}
    if not os.path.exists(os.path.join(workspace.files_path, folder)):
        os.makedirs(os.path.join(workspace.files_path, folder))
    for dir in path:
        if not os.path.exists(os.path.join(workspace.files_path, dir)):
            os.makedirs(os.path.join(workspace.files_path, dir))
        for subdir in path[dir]:
            if not os.path.exists(os.path.join(workspace.files_path, dir, subdir)):
                os.makedirs(os.path.join(workspace.files_path, dir, subdir))
            for subdir2 in path[dir][subdir]:
                if not os.path.exists(os.path.join(workspace.files_path, dir, subdir, subdir2)):
                    os.makedirs(os.path.join(workspace.files_path, dir, subdir, subdir2))
    data_path = os.path.join(workspace.files_path, dir, subdir)
    for month in [
        int(str(m)) for m in rbv_environment.get_date_series(str(start), str(end), frequence)
    ]:
        if frequence == "trimestre" and month % 100 % 3 != 0:
            continue
        current_run.log_info(f"Simule la vérification pour {month}")
        file_verif_path = os.path.join(
            data_path,
            "Selections_Verif",
        )
        file_verif_name = f"FREQ:{frequence}-GAIN_VERIF_MEDIAN_MAX:{seuil_gain_verif_median}-MIN_NB_TRIM_OBS:{window}-MIN_NB_TRIM_WITH_VERIF:{nb_period_verif}-p_low:{proportion_selection_bas_risque}-p_mod:{proportion_selection_moyen_risque}-p_high:{proportion_selection_haut_risque}-cout_verif:{prix_verif}-seuil_max_moyen_risk:{seuil_max_moyen_risk}-seuil_max_bas_risk:{seuil_max_bas_risk}-Paiement:{paym_method_NF}-Quality_risk:{use_quality_for_risk}"
        file_verif_name = file_verif_name.replace(":", "___")
        file_results_path = os.path.join(data_path, "result_simulation")
        print("result path exists", os.path.exists(file_results_path))
        file_results_name = f"MONTH:{month}-FREQ:{frequence}-GAIN_VERIF_MEDIAN_MAX:{seuil_gain_verif_median}-MIN_NB_TRIM_OBS:{window}-MIN_NB_TRIM_WITH_VERIF:{nb_period_verif}-p_low:{proportion_selection_bas_risque}-p_mod:{proportion_selection_moyen_risque}-p_high:{proportion_selection_haut_risque}-cout_verif:{prix_verif}-seuil_max_moyen_risk:{seuil_max_moyen_risk}-seuil_max_bas_risk:{seuil_max_bas_risk}-Paiement:{paym_method_NF}-Quality_risk:{use_quality_for_risk}"
        file_results_name = file_results_name.replace(":", "___")
        simulate(
            regions,
            file_results_path,
            file_results_name,
            file_verif_path,
            file_verif_name,
            frequence,
            month,
            prix_verif,
            seuil_gain_verif_median,
            seuil_max_bas_risk,
            seuil_max_moyen_risk,
            window,
            nb_period_verif,
            proportion_selection_bas_risque,
            proportion_selection_moyen_risque,
            proportion_selection_haut_risque,
            paym_method_NF,
            use_quality_for_risk,
            folder,
            model_name,
        )
    return file_results_path


@run_vbr.task
def get_environment(nom_init):
    """Put some data processing code here."""
    current_run.log_info(f"Chargement des données d'initialisation de la simulation")
    data_path = f"{workspace.files_path}/"
    with open(f"{data_path}initialization_simulation/{nom_init}.pickle", "rb") as file:
        # Deserialize and load the object from the file
        regions = pickle.load(file)
    return regions


def simulate(
    regions,
    FILE_RESULTS_PATH,
    FILE_RESULTS_NAME,
    FILE_VERIF_PATH,
    FILE_VERIF_NAME,
    FREQ,
    MONTH,
    COUT_VERIF,
    GAIN_MEDIAN_SEUIL,
    seuil_max_bas_risk,
    seuil_max_moyen_risk,
    WINDOW,
    NB_PERIOD_VERIF,
    P_LOW,
    P_MOD,
    P_HIGH,
    paym_method_NF,
    use_quality_for_risk,
    folder,
    model_name,
):
    QUARTER = str(rbv_environment.month_to_quarter(MONTH))
    file_result_path = os.path.join(FILE_RESULTS_PATH, f"model___{model_name}-{FILE_RESULTS_NAME}")

    new = True
    f = pd.DataFrame(
        columns=[
            "province",
            "periode",
            "#centres",
            "#risque élevé",
            "#risque modéré",
            "#risque faible",
            "# vérifiés",
            "gain moyen",
            "taux validation moyen",
        ]
    )

    if FREQ == "trimestre":
        period = QUARTER
        print(period)
    else:
        period = MONTH
    for group in regions:
        for ou in group.members:
            ou.set_frequence(FREQ)
            ou.set_month_verification(period)
            ou.set_nb_verif_min_per_window(NB_PERIOD_VERIF)
            ou.set_window(WINDOW)
            # Define these functions in vbr_custom.py
            # ------
            categorize_quality(ou, use_quality_for_risk)
            categorize_quantity(ou, GAIN_MEDIAN_SEUIL, seuil_max_bas_risk, seuil_max_moyen_risk)
            # ------
            ou.mix_risks(use_quality_for_risk)
            ou.get_taux_validation_median()
            if paym_method_NF == "complet":
                ou.get_gain_verif_for_period_verif(1)
            elif paym_method_NF == "taux validation personnel":
                ou.get_gain_verif_for_period_verif(ou.taux_validation)
        if paym_method_NF == "taux validation moyen ZS":
            assign_taux_validation_per_ZS(group)

        # Define this function in vbr_custom.py
        # ------
        proportions = get_proportions(P_LOW, P_MOD, P_HIGH)
        # -------
        group.set_proportions(proportions)
        # group.set_gain_median_seuil(GAIN_MEDIAN_SEUIL)
        group.set_cout_verification(COUT_VERIF)
        verification_list = group.get_verification_list()
        verification_list_path = os.path.join(
            FILE_VERIF_PATH,
            f"{FILE_VERIF_NAME}-province___{group.name}-periode___{period}",
        )
        verification_list.to_csv(
            verification_list_path + ".csv",
            index=False,
        )
        detailled_list_dx = group.get_detailled_list_dx()
        detailled_list_dx.to_csv(
            f"/home/hexa/workspace/{folder}/data/listes_detaillees/-province___{group.name}-periode___{period}-detailled.csv",
            index=False,
        )
        current_run.log_info(f"Calcul des statistiques pour {group.name} en cours")
        group.save_orgunits_to_csv(
            f"{workspace.files_path}/{folder}/data/debug/group___{group.name}.csv"
        )

        stats = group.get_statistics(period)
        f = pd.concat([f, stats], ignore_index=True)
    # f.drop_duplicates(subset=["periode", "province"], inplace=True)
    f.to_csv(file_result_path + ".csv", index=False)


def assign_taux_validation_per_ZS(group):
    def mediane(g: list) -> float:
        L = len(g)
        if L > 1:
            pair = L % 2 == 0
            sorted_numbers = sorted(g)
            print("mediane", g)
            if pair:
                mediane = sum(sorted_numbers[L // 2 : (L // 2) + 2]) / 2
            else:
                mediane = sorted_numbers[L // 2]
        elif L == 1:
            mediane = g[0]
        else:
            mediane = pd.NA
        return mediane

    taux_val_ZS = {}
    for ou in group.members:
        taux_val_ZS.setdefault(f"{ou.identifier_verification[2]}-{ou.risk}", []).append(
            ou.taux_validation
        )
    for zs in taux_val_ZS:
        taux_val_ZS[zs] = mediane(taux_val_ZS[zs])
    for ou in group.members:
        if f"{ou.identifier_verification[2]}-{ou.risk}" in taux_val_ZS:
            ou.get_gain_verif_for_period_verif(
                taux_val_ZS[f"{ou.identifier_verification[2]}-{ou.risk}"]
            )
        else:
            ou.get_gain_verif_for_period_verif(ou.taux_validation)


if __name__ == "__main__":
    run_vbr()
