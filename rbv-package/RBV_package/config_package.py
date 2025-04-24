list_cols_df_verfication = [
    "period",  # The period we ran the simulation for (eg 202501)
    "ou_id",  # The ID of the organizational Unit we ran the simulation for
    "level_2_uid",  # The ID of the second geographical level of the OU
    "level_2_name",  # The name of the second geographical level of the OU
    "level_3_uid",  # The ID of the third geographical level of the OU
    "level_3_name",  # The name of the third geographical level of the OU
    "level_4_uid",  # The ID of the forth geographical level of the OU
    "level_4_name",  # The name of the forth geographical level of the OU
    "level_5_uid",  # The ID of the fifth geographical level of the OU
    "level_5_name",  # The name of the fifth geographical level of the OU
    "level_6_uid",  # The ID of the sixth geographical level of the OU
    "level_6_name",  # The name of the sixth geographical level of the OU
    "bool_verified",  # The result of the simulation. True if the center would have been verified, False if not
    # We stablish this using the risk of the center. This is for the period.
    "diff_in_subsidies_decval_period",  # The median of:
    # the difference in subsidies that the center would recieve based on the declared or validated values.
    # It is calculated as: median((declared - validated) * tarif)
    # The bigger, the more extra subsidies the center would get if it wasn't verified.
    # Its calculated for the period of the simulation.
    # Note that these values can be empty -- we have no declared services for the relevant period.
    # The taux can still be filled -- it is calculated with the values for the window.
    "diff_in_subsidies_tauxval_period",  # The median of:
    # the difference in subsidies that the center would recieve without verification (calculated with taux)
    # and with verification.
    # The bigger, the more extra subsidies the center would get if it wasn't verified.
    # Its calculated for the period of the simulation.
    # Note that these values can be empty -- we have no declared services for the relevant period.
    # The taux can still be filled -- it is calculated with the values for the window.
    "benefice_complet_vbr",
    # Amount of money won per center with VBR. It is calculated as:
    # amount of money the center gets with VBR_taux
    # - amount of money the center gets with systematic verification
    # - how much it costs to verify the center
    # If it is bigger than zero, then we should verify the center.
    # Note that these values can be empty -- we have no declared services for the relevant period.
    # The taux can still be filled -- it is calculated with the values for the window.
    "taux_validation",  # The median of the taux_validation for the center.
    #  (The taux validation is 1 - (dec - val)/dec. The closer to one, the more the center tells the truth)
    # It takes into account all of the observation window.
    # It can be a NaN if we don't have data for the observation window. Note that the observation window
    # does not include the period of the simulation -- it stops just before.
    "subside_dec_period",  # The total subside the center would get based only on the declared values.
    #   It is calculated for the period of the simulation.
    # Note that these values can be empty -- we have no declared services for the relevant period.
    # The taux can still be filled -- it is calculated with the values for the window.
    "subside_val_period",  # The total subside the center would get based on the validated values.
    # (The subside the center would get with verification)
    # It only takes into account the period we are running the simulation for.
    # Note that these values can be empty -- we have no declared services for the relevant period.
    # The taux can still be filled -- it is calculated with the values for the window.
    "subside_taux_period",  # The total subside the center would get based on the declared values,
    # but taking into account the taux of the center.
    # (The subside the center would get without verification)
    # It only takes into account the period we are running the simulation for.
    # Note that these values can be empty -- we have no declared services for the relevant period.
    # The taux can still be filled -- it is calculated with the values for the window.
    "ecart_median",  # The median of the ecart for the center.
    # The ecart measures the difference between the declared, verified and validated values.
    # 0.4*(ecart_dec_ver) + 0.6*(ecart_ver_val) with
    # ecart_dec_ver = (dec - ver) / ver & ecart_ver_val = (ver - val) / ver
    # The closer to 1, the more the center is lying.
    # For all of the observstion window.
    # It is empty when the there is no data for the observation window or the center is not eligible.
    # (So, you can have this empty with other variables not empty because the center is not eligible,
    # but there is data)
    "categorie_risque",  # The overall risk of the center. It is caracterized with the data from the
    # observation window.
]
list_cols_df_stats = [
    "province",  # The name of the province
    "periode",  # The period we ran the simulation for (eg 202501)
    "total number of centres",  # The total number of centers for the province. It includes all of the centers
    # with data for the simulation. (So, if you extracted the data from 2024-01 to 2025-12, you will include
    # all of the centers that had data in that period).
    "number of centers high risk",  # Number of centers that are at high risk. It takes into account the data
    # for the observation window.
    "number of centers middle risk",  # Number of centers that are at middle risk. It takes into account the data
    # for the observation window.
    "number of centers low risk",  # Number of centers that are at low risk. It takes into account the data
    # for the observation window.
    "number of verified centers",  # Number of verified center in the period of the simulation.
    "cost of verification (VBR)",  # Cost of verifying the centers if we use VBR. It is
    # number of verified centers x cost of verifying a center
    "cost of verification (syst)",  # Cost of verifying the centers if we use systematic verification. It is
    # total number of centres x cost of verifying a center
    "subsidies (VBR)",  # The total subsidies centers would get for the period of the simulation if we use VBR.
    "subsidies (syst)",  # The total subsidies centers would get for the period of the simulation if we use systematic verification.
    "total cost (VBR)",  # The total cost of doing VBR
    # it is cost of verification (VBR) + subsidies (VBR)
    "total cost (syst)",  # The total cost of doing systematic verification
    # it is ost of verification (syst) + subsidies (syst)
    "ratio cost_verification cost_total (VBR)",  # The part of the total cost that is dedicated for verification
    # when doing VB
    "ratio cost_verification cost_total (syst)",  # The part of the total cost that is dedicated for verification
    # when doing systematic verification
    "Number of centers where vbr was beneficial",  # The number of unverified centers where VBR made money.
    "Number of centers over-subsidized",  # The number of unverified centers where VBR meant over-subsidies.
    "Money saved by VBR (taking into account verif costs)",  # The amount of money won by VBR in the centers
    # where VBR won money
    "Amount of over-subsidies",  # The amount of over subsidied given to non-verified centers where
    # VBR lost money.
    "Average of over-subsidies (non-verified centers)",  # The average of of over-subsidies is non-verified centers.
    "Average of over-subsidies (verified centers)",  # The average of over-subsidies that would have been given
    # to verified centers.
    "total gain vbr",  # How much money in total we win by doing VBR. If it is bigger than zero, we should
    # not do VBR.
]
list_cols_df_services = [  # This we do not use further I think.
    "period",
    "ou_id",
    "Category of the center",
    "bool verified",
    "service",
    "Taux of validation",
]
