"""Template for newly generated pipelines."""

from openhexa.sdk import current_run, pipeline, workspace, parameter, DHIS2Connection
import pickle, requests, os, json
import pandas as pd

from RBV_package import rbv_environment
from openhexa.toolbox.dhis2 import DHIS2


@pipeline("initialize_vbr")
@parameter("dhis_con", type=DHIS2Connection, help="Connection to DHIS2", required=True)
@parameter("hesabu_con", type=str, help="Connection to hesabu", default="hesabu", required=True)
@parameter(
    "program_id",
    type=str,
    help="Program ID for contracts",
    default="okgPNNENgtD",
    required=True,
)
@parameter(
    "period",
    name="period",
    type=str,
    help="Period for the verification (either yyyymm eg 202406 or yyyyQt eg 2024Q2)",
    required=True,
    default="202406",
)
@parameter("model_name", name="Nom du modèle", type=str, default="model", required=True)
@parameter(
    "window",
    type=int,
    help="Number of months to consider",
    required=True,
    default=24,
)
@parameter("selection_provinces", type=bool, default=True)
@parameter("extract", name="Extraire les données", type=bool, default=False)
def initialize_vbr(
    dhis_con,
    hesabu_con,
    program_id,
    period,
    extract,
    model_name,
    window,
    selection_provinces,
):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """

    dhis = get_dhis2(dhis_con)
    hesabu = get_hesabu(hesabu_con)
    hesabu_params = get_hesabu_vbr_setup()
    packages_hesabu = get_hesabu_package_ids(hesabu_params)
    contract_group_id = get_contract_group_unit_id(hesabu_params)
    packages = fetch_hesabu_package(hesabu, packages_hesabu)
    periods = get_periods(period, window)
    contracts = fetch_contracts(dhis, program_id)
    done = get_package_values(dhis, periods, packages, contract_group_id, extract)
    quant = prepare_quantity_data(done, periods, packages, contracts, hesabu_params, extract)
    qual = prepare_quality_data(done, periods, packages, hesabu_params, extract)
    save_simulation_environment(
        quant, qual, contracts, hesabu_params, model_name, selection_provinces
    )


@initialize_vbr.task
def prepare_quantity_data(done, periods, packages, contracts, hesabu_params, extract):
    if extract:
        """Prepare the data for the verification."""
        data = pd.DataFrame()
        for id in packages:
            if hesabu_params["packages"][str(id)] == "quantite" and os.path.exists(
                f"{workspace.files_path}/packages/{id}"
            ):
                for p in periods:
                    if os.path.exists(f"{workspace.files_path}/packages/{id}/{p}.csv"):
                        df = pd.read_csv(f"{workspace.files_path}/packages/{id}/{p}.csv")
                        data = pd.concat([data, df], ignore_index=True)
        data = data.merge(contracts, on="org_unit_id")
        data = data[
            list(hesabu_params["quantite_attributes"].keys())
            + list(hesabu_params["contracts_attributes"].keys())
        ]
        data.rename(
            columns=hesabu_params["contracts_attributes"],
            inplace=True,
        )
        data.rename(
            columns=hesabu_params["quantite_attributes"],
            inplace=True,
        )
        print(data.contract_end_date.unique())
        data.contract_end_date = data.contract_end_date.astype(int)
        data = data[(data.contract_end_date >= data.month) & (~data.type_ou.isna())]

        data["gain_verif"] = (data["dec"] - data["val"]) * data["tarif"]
        data["subside_sans_verification"] = data["dec"] * data["tarif"]
        data["subside_avec_verification"] = data["val"] * data["tarif"]
        data["quarter"] = data["month"].map(rbv_environment.month_to_quarter)
        data = rbv_environment.calcul_ecarts(data)
        data.to_csv(f"{workspace.files_path}/quantity_data.csv", index=False)
    else:
        data = pd.read_csv(f"{workspace.files_path}/quantity_data.csv")
    return data


@initialize_vbr.task
def prepare_quality_data(done, periods, packages, hesabu_params, extract):
    if extract:
        """Prepare the data for the verification."""
        data = pd.DataFrame()
        for id in packages:
            if hesabu_params["packages"][str(id)] == "qualite" and os.path.exists(
                f"{workspace.files_path}/packages/{id}"
            ):
                for p in [rbv_environment.month_to_quarter(str(p)) for p in periods]:
                    if os.path.exists(f"{workspace.files_path}/packages/{id}/{p}.csv"):
                        df = pd.read_csv(f"{workspace.files_path}/packages/{id}/{p}.csv")
                        data = pd.concat([data, df], ignore_index=True)
        data = data[list(hesabu_params["qualite_attributes"].keys())]
        data.rename(
            columns=hesabu_params["qualite_attributes"],
            inplace=True,
        )
        data["score"] = data["num"] / data["denom"]
        data["month"] = data["quarter"].map(rbv_environment.quarter_to_months)
        data.to_csv(f"{workspace.files_path}/quality_data.csv", index=False)
    else:
        data = pd.read_csv(f"{workspace.files_path}/quality_data.csv")
    return data


@initialize_vbr.task
def save_simulation_environment(
    quant, qual, contracts, hesabu_params, group_name, selection_provinces
):
    regions = []
    orgunits = quant["ou"].unique()
    quality_indicators = sorted(qual["indicator"].unique())
    if (
        selection_provinces
        and "selection_provinces" in hesabu_params
        and len(hesabu_params["selection_provinces"]) > 0
    ):
        for province in hesabu_params["selection_provinces"]:
            print(f"province= {province}")
            group = rbv_environment.Group_Orgunits(
                province,
                quality_indicators,
            )
            nb_tot = 0
            for ou in orgunits:
                temp = quant[(quant.ou == ou) & (quant.level_2_name == province)]
                if len(temp) > 0:
                    group.add_ou(
                        rbv_environment.Orgunit(
                            ou,
                            temp,
                            qual[(qual.ou == ou)],
                            quality_indicators,
                            temp.type_ou.values[0]
                            in hesabu_params["type_ou_verification_systematique"],
                        )
                    )
                    nb_tot += 1
            print(f"number of orgunits= {nb_tot}")
            regions.append(group)
    else:
        group = rbv_environment.Group_Orgunits(
            "national",
            quality_indicators,
        )
        nb_tot = 0
        for ou in orgunits:
            temp = quant[(quant.ou == ou)]
            if len(temp) > 0:
                group.add_ou(
                    rbv_environment.Orgunit(
                        ou,
                        temp,
                        qual[(qual.ou == ou)],
                        quality_indicators,
                        temp.type_ou.values[0]
                        in hesabu_params["type_ou_verification_systematique"],
                    )
                )
                nb_tot += 1
        print(f"number of orgunits= {nb_tot}")
        regions.append(group)
    if not os.path.exists(f"{workspace.files_path}/initialization_simulation"):
        os.makedirs(f"{workspace.files_path}/initialization_simulation")
    with open(
        f"{workspace.files_path}/initialization_simulation/{group_name}.pickle", "wb"
    ) as file:
        # Serialize and save the object to the file
        pickle.dump(regions, file)
    print(
        f"Fichier d'initialisation sauvé: {workspace.files_path}/initialization_simulation/{group_name}.pickle"
    )
    return regions


@initialize_vbr.task
def get_dhis2(con_OH):
    return DHIS2(con_OH)


@initialize_vbr.task
def get_hesabu_vbr_setup():
    return json.load(open(f"{workspace.files_path}/hesabu/hesabu_vbr_setup.json", "r"))


@initialize_vbr.task
def get_hesabu_package_ids(hesabu_setup):
    return [int(id) for id in hesabu_setup["packages"]]


@initialize_vbr.task
def get_contract_group_unit_id(hesa_setup):
    return hesa_setup["main_contract_id"]


@initialize_vbr.task
def get_hesabu(con_hesabu):
    return workspace.get_connection(con_hesabu)


@initialize_vbr.task
def fetch_hesabu_package(con_hesabu, package_ids):
    headers = {
        "Accept": "application/vnd.api+json;version=2",
        "Accept-Language": "en-US",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "X-Token": con_hesabu.token,
        "X-Dhis2UserId": con_hesabu.dhis2_user_id,
    }
    hesabuPackages = {}
    for package_id in package_ids:
        url = f"{con_hesabu.url}/{package_id}"
        response = requests.get(url, headers=headers)
        hesabuPayload = response.json()
        hesabuPackage = rbv_environment.deserialize(hesabuPayload)

        activities = []
        freq = hesabuPackage["frequency"]
        for activity in hesabuPackage["projectActivities"]:
            some_ext = [
                mapping for mapping in activity["inputMappings"] if "externalReference" in mapping
            ]
            if len(activity["inputMappings"]) > 0 and len(some_ext) > 0:
                new_activity = {
                    "name": activity["name"],
                    "id": activity["id"],
                    "code": activity["code"],
                    "inputMappings": {},
                }

                for mapping in activity["inputMappings"]:
                    if "externalReference" in mapping:
                        new_activity["inputMappings"][mapping["input"]["code"]] = {
                            "externalReference": mapping["externalReference"],
                            "name": mapping["name"],
                        }

                activities.append(new_activity)

            activities = sorted(activities, key=lambda x: x["code"])

        hesabuPackage["activities"] = activities
        hesabuPackages[package_id] = {"content": hesabuPackage, "freq": freq}
    return hesabuPackages


@initialize_vbr.task
def get_package_values(dhis, periods, hesabu_packages, contract_group, extract):
    if extract:
        for package_id in hesabu_packages:
            package = hesabu_packages[package_id]["content"]
            freq = hesabu_packages[package_id]["freq"]
            if freq != "monthly":
                periods_adapted = list(
                    {rbv_environment.month_to_quarter(str(pe)) for pe in periods}
                )
                current_run.log_info(f"Adapted periods: {periods_adapted}")
            else:
                periods_adapted = periods
                current_run.log_info(f"Adapted periods: {periods_adapted}")
            package_name = package["name"]
            deg_external_reference = package["degExternalReference"]
            org_unit_ids = rbv_environment.get_org_unit_ids_from_hesabu(
                contract_group, package, dhis
            )
            # org_unit_ids = org_unit_ids.intersection(
            #    {"yBv0ReaU1yH", "rK4yuXZL7d9", "YVDncsgsS5r"}
            # )
            org_unit_ids = list(org_unit_ids)
            current_run.log_info(
                f"Fetching data for package {package_name} for {len(org_unit_ids)} org units"
            )
            if len(org_unit_ids) > 0:
                rbv_environment.fetch_data_values(
                    dhis,
                    deg_external_reference,
                    org_unit_ids,
                    periods_adapted,
                    package["activities"],
                    package_id,
                )

    return True


@initialize_vbr.task
def fetch_contracts(dhis, contract_program_id):
    program = dhis.api.get(
        f"programs/{contract_program_id}.json?fields=id,name,programStages[:all,programStageDataElements[dataElement[id,code,name,optionSet[options[code,name]]]"
    )
    data = dhis.api.get(
        f"sqlViews/QNKOsX4EGEk/data.json?var=programId:{contract_program_id}&paging=false"
    )

    data_elements = program["programStages"][0]["programStageDataElements"]
    data_value_headers = {}
    for de in data_elements:
        data_value_headers[de["dataElement"]["id"]] = de

    headers = data["listGrid"]["headers"]

    records = []
    rows = data["listGrid"]["rows"]
    for row in rows:
        record = {}
        index = 0
        for header in headers:
            value = row[index]
            record[header["name"]] = value
            index = index + 1

        data_values = json.loads(record["data_values"])
        for data_value in data_values:
            field_name = data_value_headers[data_value]["dataElement"]["code"]
            record[field_name] = data_values[data_value]["value"]
            record["last_modified_at"] = data_values[data_value]["lastUpdated"]

        del record["data_values"]

        record["start_period"] = record["contract_start_date"].replace("-", "")[0:6]
        record["end_period"] = record["contract_end_date"].replace("-", "")[0:6]

        records.append(record)
    records_df = pd.DataFrame(records)
    records_df.to_csv(f"{workspace.files_path}/contracts.csv", index=False)
    return records_df


@initialize_vbr.task
def get_periods(period, window):
    frequency = get_period_type(period)
    start, end = get_start_end(period, window, frequency)
    current_run.log_info(f"Periods considered: {start} to {end}")
    return rbv_environment.get_date_series(start, end, frequency)


def extract_status(x):
    def find_quant(l):
        for i in range(len(l)):
            if "QUANT" in l[i]:
                return i

    list = x.split("-")
    i = find_quant(list)
    dx = "-".join(list[i:])
    status = list[i - 1]
    return dx, status


def get_period_type(period):
    if "Q" in period:
        return "quarter"
    else:
        return "month"


def get_start_end(period, window, frequency):
    if frequency == "quarter":
        year = int(period[:4])
        quarter = int(period[-1])
        end = year * 100 + quarter * 3
        start = rbv_environment.months_before(end, window)
        start = rbv_environment.month_to_quarter(start)
        end = rbv_environment.month_to_quarter(end)
    else:
        end = int(period)
        start = rbv_environment.months_before(end, window)
    return str(start), str(end)


if __name__ == "__main__":
    initialize_vbr()
