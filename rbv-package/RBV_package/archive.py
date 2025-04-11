import pandas as pd


def add_higher_levels_and_names(dhis, data):
    lou = {ou["id"]: ou for ou in dhis.meta.organisation_units()}
    res = [
        {
            "dx": row.dx,
            "dx_name": row.dx_name,
            "period": int(row.pe),
            "level_5_uid": row["ou"],
            "level_5_name": lou[row["ou"]].get("name"),
            "level_4_uid": lou[row["ou"]]["path"].strip("/").split("/")[3],
            "level_4_name": lou[lou[row["ou"]]["path"].strip("/").split("/")[3]].get("name"),
            "level_3_uid": lou[row["ou"]]["path"].strip("/").split("/")[2],
            "level_3_name": lou[lou[row["ou"]]["path"].strip("/").split("/")[2]].get("name"),
            "level_2_uid": lou[row["ou"]]["path"].strip("/").split("/")[1],
            "level_2_name": lou[lou[row["ou"]]["path"].strip("/").split("/")[1]].get("name"),
            "value": int(float(row.value)),
        }
        for i, row in data.iterrows()
        if not row.isnull().any()
    ]
    return pd.DataFrame(res)


def period_to_quarter(p):
    p = int(p)
    year = p // 100
    quarter = ((p % 100) - 1) // 3 + 1
    return f"{year}Q{quarter}"


def last_quarter(year, quarter):
    if quarter == 1:
        return year - 1, 4
    else:
        return year, quarter - 1


def add_parents(df, parents):
    filtered_parents = {key: parents[key] for key in df["ou"] if key in parents}
    # Transform the `parents` dictionary into a DataFrame
    parents_df = pd.DataFrame.from_dict(filtered_parents, orient="index").reset_index()

    # Rename the index column to match the "ou" column
    parents_df.rename(
        columns={
            "index": "ou",
            "level_2_id": "level_2_uid",
            "level_3_id": "level_3_uid",
            "level_4_id": "level_4_uid",
            "level_5_id": "level_5_uid",
            "name": "level_5_name",
        },
        inplace=True,
    )

    # Join the DataFrame with the parents DataFrame on the "ou" column
    result_df = df.merge(parents_df, on="ou", how="left")
    return result_df
