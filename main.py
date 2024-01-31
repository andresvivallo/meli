import os
import warnings
from datetime import timedelta

from config import JSON_CONFIG
from utils import (
    unzipping_all_files,
    trf_json_file_to_dataframe,
    join_prints_with_taps,
    read_pays_csv,
)

ZIP_FILE = "CodeEx.zip"
DEST_PATH = "tmp"
PAY_FILE = "pays.csv"



if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    wd = os.getcwd()

    unzipping_all_files(
        file=f"{wd}/data/{ZIP_FILE}",
        dest_path=f"{wd}/{DEST_PATH}/"
    )
    
    df_dict = trf_json_file_to_dataframe(
        path_files=f"{wd}/{DEST_PATH}/{ZIP_FILE.replace('.zip', '')}",
        json_config=JSON_CONFIG
    )
    
    prints_with_taps = join_prints_with_taps(
        df_dict=df_dict
    )


    import pandas as pd
    # Getting value_props
    lst_value_prop = list(prints_with_taps.value_prop.unique())
    
    # List of useful new columns
    # pre_three_lw_prints = "three_lw_prints_"
    # three_lw_prints_colums = [
    #     pre_three_lw_prints + sub for sub in lst_value_prop
    # ] 
    
    # Considering the last week as the last 7 days recorded in the data
    start_date_of_last_week = (
        prints_with_taps.day.max() - timedelta(days = 6)
    )
    
    # Filtering only the last week prints
    prints_last_week = prints_with_taps.loc[
        prints_with_taps.day >= start_date_of_last_week
    ].reset_index()

    # Adding new columns
    # prints_last_week[three_lw_prints_colums] = 0
    
    # Creating empty dataframe for save counts
    three_lw_result = pd.DataFrame()

    # Working by users
    users_ids = list(prints_last_week.user_id.unique())
    for i in range(len(users_ids)):
        user_id = users_ids[i]
        print()
        print(f"Working on user {i} of {len(users_ids)-1}")
        # Getting unique days of specific user
        days = list(
            prints_last_week[
                prints_last_week.user_id == user_id
            ].day.unique()
        )
        
        # Loop into unique days of the user
        for k in range(len(days)):
            day = days[k]
            
            # Defining window of analyze, considering the 
            # last 3 weeks (21 days) before the objective row
            end_date = day - timedelta(days=1)
            start_date = (
                end_date - timedelta(days=20)
            )
            # Filtering with the window of analyze and user_id
            mask = (
                prints_with_taps.day >= start_date
            ) & (
                prints_with_taps.day <= end_date
            ) & (
                prints_with_taps.user_id == user_id
            )

            # Applying filter and counting prints by value_prop
            counts = prints_with_taps.loc[mask].groupby(
                "value_prop"
            ).count().reset_index()[
                ["value_prop", "day"]
            ].rename(columns={"day": "count"})
            # adding useful info
            counts["user_id"] = user_id
            counts["day"] = day
            
            three_lw_result = pd.concat([three_lw_result, counts])


        # Defining last 3 weeks prints counts by row and value_prop
        # for three_lw_prints in three_lw_prints_colums:
        #     try:
        #         value = counts[
        #             counts["value_prop"] == (
        #                 f"{three_lw_prints.replace(pre_three_lw_prints, '')}"
        #             )
        #         ]["count"]
        #         prints_last_week[f"{three_lw_prints}"][i] = value
        #     except:
        #         prints_last_week[f"{three_lw_prints}"][i] = 0

    # long to wide and rename columns
    prefix = "l3w_prints"
    three_lw_result_wide = three_lw_result.pivot(
        index=["day", "user_id"],
        columns=["value_prop"],
        values="count"
    ).reset_index().rename(
        columns={
            "cellphone_recharge": f"{prefix}_cellphone_recharge",
            "credits_consumer" : f"{prefix}_credits_consumer",
            "link_cobro" : f"{prefix}_link_cobro",
            "point" : f"{prefix}_point",
            "prepaid" : f"{prefix}_prepaid",
            "send_money" : f"{prefix}_send_money",
            "transport" : f"{prefix}_transport",
        }
    )
    three_lw_result_wide.fillna(0, inplace=True)

    prints_last_week_and_l3w = prints_last_week.merge(
        three_lw_result_wide,
        on=["day", "user_id"],
        how="left"
    )






    
    df_pays = read_pays_csv(
            path_file=(
                f"{wd}/{DEST_PATH}/"
                f"{ZIP_FILE.replace('.zip', '')}/{PAY_FILE}"
            )
        )

    
    
